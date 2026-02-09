#!/usr/bin/env python3

import aws_cdk as cdk
from stacks.s3_stacks import S3Stack
from stacks.network_stack import NetworkStack
from stacks.security_group_stack import SecurityGroupStack
import requests

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


S3Stack(
    app,
    f"RIS360-S3Stack-{env_name}",
    env_name=env_name,
)

app.synth()
