from aws_cdk import (
    Stack,
    aws_redshiftserverless as redshift,
    aws_ec2 as ec2
)
from constructs import Construct


class RedshiftServerlessStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.Vpc,
        redshift_sg: ec2.SecurityGroup,
        env_name: str,
        redshift_role,
        **kwargs
    ):
        super().__init__(scope, construct_id, **kwargs)

        # 🔹 Namespace (database + users)
        self.namespace = redshift.CfnNamespace(
            self,
            "RIS360RedshiftNamespace",
            namespace_name=f"ris360-ns-{env_name}",
            db_name="ris_360_analytics",
            admin_username="admin",
            admin_user_password="ChangeMe123!" , # 🔐 use secret manager
             iam_roles=[redshift_role.role_arn]
        )

        # 🔹 Workgroup (compute + networking)
        self.workgroup = redshift.CfnWorkgroup(
            self,
            "RIS360RedshiftWorkgroup",
            workgroup_name=f"ris360-wg-{env_name}",
            namespace_name=self.namespace.namespace_name,
            base_capacity=32,  # RPUs (can start small)
            subnet_ids=[subnet.subnet_id for subnet in vpc.private_subnets],
            security_group_ids=[redshift_sg.security_group_id]
        )

        self.workgroup.add_dependency(self.namespace)
