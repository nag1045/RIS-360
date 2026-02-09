from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3
)
from constructs import Construct


class S3Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, env_name: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        bucket_configs = ["landing", "bronze", "silver", "gold"]

        for layer in bucket_configs:
            s3.Bucket(
                self,
                f"{layer.capitalize()}Bucket",
                bucket_name=f"ris-360-{layer}-{env_name}",
                versioned=True,
                encryption=s3.BucketEncryption.S3_MANAGED,
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                removal_policy=RemovalPolicy.DESTROY if env_name != "prod" else RemovalPolicy.RETAIN,
                auto_delete_objects=True if env_name != "prod" else False
            )
