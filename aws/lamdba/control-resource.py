"""
AWS リソースの操作を行う

実施できる操作は以下の通り
- EC2, AutoScalingGroupの起動
- EC2, AutoScalingGroupの停止
- EC2, AutoScalingGroupのステータス取得
"""

import os
import json
import boto3

EXEC_ENV = "EXEC_ENV"

class ControlService:
    def __init__(self, aws_service: list=None, target_service: dict=None, action: str=None) -> None:
        """
        aws_service: 起動、停止を制御するAWSサービスのリスト
        target_service: 起動、停止を制御するサービスのタグ情報
        action: 起動、停止、ステータスを取得する
        """
        self.aws_service = aws_service
        self.target_service = target_service
        self.action = action
        self.exec_env = os.environ[EXEC_ENV] if EXEC_ENV in os.environ else ""

class AWSClient:
    """
    boto3のクライアントを管理する
    """
    def __init__(self, aws_service_name) -> None:
        self.aws_service_name = aws_service_name
        self.service = boto3.client(aws_service_name, region_name=os.environ['AWS_REGION'])

def validate_params(event) -> tuple:
    """
    引数のバリデーションを行う
    """
    aws_service = event["aws_service"]
    target_service = event["target_service"]
    action = event["action"]

    if not aws_service:
        return ControlService(), {
            'statusCode': 400,
            'body': json.dumps('aws_service is required!')
        }
    else:
        # 型チェック
        if not isinstance(aws_service, list):
            return ControlService(), {
                'statusCode': 400,
                'body': json.dumps('aws_service is invalid!')
            }

    if not target_service:
        return ControlService(), {
            'statusCode': 400,
            'body': json.dumps('target_service is required!')
        }
    else:
        # 型チェック
        if not isinstance(target_service, dict):
            return ControlService(), {
                'statusCode': 400,
                'body': json.dumps('target_service is invalid!')
            }
    if not action:
        return ControlService(), {
            'statusCode': 400,
            'body': json.dumps('action is required!')
        }
    else:
        # 型チェック
        if not isinstance(action, str):
            return ControlService(), {
                'statusCode': 400,
                'body': json.dumps('action is invalid!')
            }
    return ControlService(aws_service, target_service, action), {}


def get_resource(aws_service_name) -> AWSClient or None:
    try:
        return AWSClient(aws_service_name)
    except boto3.exceptions.ResourceNotExistsError:
        return None

def update_ec2(ec2_client, control_service):
    response = ec2_client.describe_instances()
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            for tag in instance["Tags"]:
                if tag["Key"] == "Service" and tag["Value"] == control_service.target_service["service"]:
                    if control_service.action == "start":
                        if instance["State"]["Name"] == "stopped":
                            ec2_client.start_instances(InstanceIds=[instance["InstanceId"]])
                    elif control_service.action == "stop":
                        if instance["State"]["Name"] == "running":
                            # スポットインスタンスは停止できない
                            if "InstanceLifecycle" in instance and instance["InstanceLifecycle"] == "spot":
                                continue
                            ec2_client.stop_instances(InstanceIds=[instance["InstanceId"]])

def get_ec2_status(ec2_client, control_service):
    result = []
    response = ec2_client.describe_instances()
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            info = {}
            for tag in instance["Tags"]:
                if tag["Key"] == "Name":
                    info["Name"] = tag["Value"]
                elif tag["Key"] == "Service" \
                and tag["Value"] == control_service.target_service["service"] \
                and instance["State"]["Name"] == "running":
                    info["InstanceId"] = instance["InstanceId"]
                    info["InstanceType"] = instance["InstanceType"]

            if info and "InstanceId" in info:
                if "Name" not in info:
                    info["Name"] = ""
                result.append(info)
    return result


def update_auto_scaling_group(autoscaling_clinet, control_service):
    response = autoscaling_clinet.describe_auto_scaling_groups()
    for group in response["AutoScalingGroups"]:
        for tag in group["Tags"]:
            if tag["Key"] == "Service" and tag["Value"] == control_service.target_service["service"]:
                if control_service.action == "start":
                    autoscaling_clinet.update_auto_scaling_group(AutoScalingGroupName=group["AutoScalingGroupName"],
                                                         MinSize=1,
                                                         MaxSize=1,
                                                         DesiredCapacity=1)
                elif control_service.action == "stop":
                    autoscaling_clinet.update_auto_scaling_group(AutoScalingGroupName=group["AutoScalingGroupName"],
                                                        MinSize=0,
                                                        MaxSize=0,
                                                        DesiredCapacity=0)

def get_aws_service_status(aws_clients, control_service):
    res = []
    for _aws_client in aws_clients:
        if _aws_client.aws_service_name == "autoscaling":
            res.append(get_auto_scaling_group_status(_aws_client.service, control_service))
        elif _aws_client.aws_service_name == "ec2":
            res.append(get_ec2_status(_aws_client.service, control_service))
    return res


def update_aws_service(aws_clients, control_service):

    for _aws_client in aws_clients:
        if _aws_client.aws_service_name == "autoscaling":
            update_auto_scaling_group(_aws_client.service, control_service)
        elif _aws_client.aws_service_name == "ec2":
            update_ec2(_aws_client.service, control_service)


def get_auto_scaling_group_status(autoscaling_clinet, control_service):
    result = []
    response = autoscaling_clinet.describe_auto_scaling_groups()
    for group in response["AutoScalingGroups"]:
        for tag in group["Tags"]:
            if tag["Key"] == "Service" and tag["Value"] == control_service.target_service["service"]:
                res = autoscaling_clinet.describe_auto_scaling_groups(AutoScalingGroupNames=[group["AutoScalingGroupName"]])
                _group = res["AutoScalingGroups"][0]
                result.append({"AutoScalingGroupName": _group["AutoScalingGroupName"],
                               "Size": _group["DesiredCapacity"]})
    return result


def lambda_handler(event, context):
    """
    引数で受け取ったAWSサービス上のユーザーのサービスを起動、停止を制御する

    ### サンプル
    event = {
            "aws_service": ["aws_service_name"],
            "target_service": {"tag_name": "your_service_name"}
            "action": "start" or "stop" or "status"
            }
    """

    # 引数のバリデーション
    control_service, validate_result = validate_params(event)
    if validate_result:
        return validate_result

    aws_clients = []
    for service_name in control_service.aws_service:
        _s = get_resource(service_name)
        if not _s:
            return {
                'statusCode': 400,
                'body': json.dumps('aws_service is invalid!')
            }
        aws_clients.append(_s)

    msg = ""
    if control_service.exec_env:
        msg = f"実行環境: {control_service.exec_env}\n"

    _action = control_service.action
    if _action == "status":
        res = get_aws_service_status(aws_clients, control_service)
        body = json.dumps(res)
        if body:
            msg += f"次のサービスが稼働しています\n{body}"
        else:
            msg += "停止しています"

        return {
            'statusCode': 200,
            'body': msg
        }

    if _action == "start" or _action == "stop":
        update_aws_service(aws_clients, control_service)

        if _action == "start":
            msg += "起動リクエストの受付を開始しました"
        elif _action == "stop":
            msg += "停止リクエストの受付を開始しました"

        return {
            'statusCode': 200,
            'body': msg
        }

    return {
        'statusCode': 404,
        'body': 'Not Found!'
    }


if __name__ == "__main__":
    import sys
    _event = {
             "aws_service": ["ec2"],
             "target_service": {"service": sys.argv[1]},
             "action": "status",
             }

    print(lambda_handler(_event, None))
