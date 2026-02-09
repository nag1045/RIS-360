#!/usr/bin/env python3

import aws_cdk as cdk
from stacks.s3_stacks import S3Stack
from stacks.network_stack import NetworkStack
from stacks.security_group_stack import SecurityGroupStack
import requests
from stacks.airflow_ec2_stack import AirflowEC2Stack

my_ip = requests.get("https://checkip.amazonaws.com").text.strip() + "/32"


app = cdk.App()

env_name = app.node.try_get_context("env") or "dev"

network_stack = NetworkStack(
    app,
    f"RIS360-NetworkStack-{env_name}",
    env_name=env_name
)

sg_stack = SecurityGroupStack(
    app,
    f"RIS360-SGStack-{env_name}",
    vpc=network_stack.vpc,
    env_name=env_name,
    my_ip=my_ip
)


airflow_stack = AirflowEC2Stack(
    app,
    f"RIS360-AirflowEC2Stack-{env_name}",
    vpc=network_stack.vpc,
    airflow_sg=sg_stack.airflow_sg,
    env_name=env_name
)


S3Stack(
    app,
    f"RIS360-S3Stack-{env_name}",
    env_name=env_name,
)

app.synth()
