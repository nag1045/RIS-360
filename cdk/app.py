#!/usr/bin/env python3

import aws_cdk as cdk
import os
from stacks.s3_stacks import S3Stack
from stacks.network_stack import NetworkStack
from stacks.security_group_stack import SecurityGroupStack
import requests
from stacks.airflow_ec2_stack import AirflowEC2Stack
from stacks.redshift_serverless_stack import RedshiftServerlessStack
from stacks.iam_stack import IAMStack



my_ip = requests.get("https://checkip.amazonaws.com").text.strip() + "/32"


app = cdk.App()

env_name = app.node.try_get_context("env") or "dev"
aws_env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION")
)

# 1️⃣ IAM stack (FIRST)
iam_stack = IAMStack(
    app,
    f"RIS360-IAMStack-{env_name}",
    env_name=env_name,
    env=aws_env
)

network_stack = NetworkStack(
    app,
    f"RIS360-NetworkStack-{env_name}",
    env_name=env_name,
    env=aws_env
)

sg_stack = SecurityGroupStack(
    app,
    f"RIS360-SGStack-{env_name}",
    vpc=network_stack.vpc,
    env_name=env_name,
    my_ip=my_ip,
    env=aws_env

)


airflow_stack = AirflowEC2Stack(
    app,
    f"RIS360-AirflowEC2Stack-{env_name}",
    vpc=network_stack.vpc,
    airflow_sg=sg_stack.airflow_sg,
    env_name=env_name,
    env=aws_env
)


S3Stack(
    app,
    f"RIS360-S3Stack-{env_name}",
    env_name=env_name,
    env=aws_env,
)


redshift_stack = RedshiftServerlessStack(
    app,
    f"RIS360-RedshiftServerlessStack-{env_name}",
    vpc=network_stack.vpc,
    redshift_sg=sg_stack.redshift_sg,
    env_name=env_name,
    env=aws_env
)


app.synth()
