from aws_cdk import (
    Stack,
    aws_iam as iam
)
from constructs import Construct


class IAMStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, env_name: str, github_org: str,  github_repo: str,**kwargs):
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

        # -------------------------------
        # GitHub Actions OIDC Provider
        # -------------------------------
        github_oidc = iam.OpenIdConnectProvider(
            self,
            "GitHubOIDCProvider",
            url="https://token.actions.githubusercontent.com",
            client_ids=["sts.amazonaws.com"]
        )

        # -------------------------------
        # GitHub Actions Deployment Role
        # -------------------------------
        self.github_actions_role = iam.Role(
            self,
            "GitHubActionsDeployRole",
            role_name=f"ris-360-github-actions-role-{env_name}",
            assumed_by=iam.FederatedPrincipal(
                github_oidc.open_id_connect_provider_arn,
                {
                    "StringEquals": {
                        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
                    },
                    "StringLike": {
                        "token.actions.githubusercontent.com:sub":
                            f"repo:{github_org}/{github_repo}:ref:refs/heads/main"
                    }
                },
                "sts:AssumeRoleWithWebIdentity"
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AdministratorAccess"  # tighten later
                )
            ]
        )
