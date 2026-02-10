from aws_cdk import (
    Stack,
    aws_iam as iam
)
from constructs import Construct


class IAMStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, env_name: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # 🔹 Glue Service Role
        self.glue_role = iam.Role(
            self,
            "GlueServiceRole",
            role_name=f"ris-360-glue-role-{env_name}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3FullAccess"  # tighten later
                )
            ]
        )
