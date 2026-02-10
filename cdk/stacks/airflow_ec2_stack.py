from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam
)
from constructs import Construct


class AirflowEC2Stack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.Vpc,
        airflow_sg: ec2.SecurityGroup,
        env_name: str,
        **kwargs
    ):
        super().__init__(scope, construct_id, **kwargs)

        # 🔹 IAM Role for EC2 (minimal for now)
        self.ec2_role = iam.Role(
            self,
            "AirflowEC2Role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            role_name=f"ris-360-airflow-ec2-role-{env_name}"
        )

        self.ec2_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonS3FullAccess"
            )
        )
        self.ec2_role.add_to_policy(
    iam.PolicyStatement(
        actions=[
            "glue:GetJob",          
            "glue:StartJobRun",
            "glue:GetJobRun",
            "glue:GetJobRuns"
        ],
        resources=["*"]
    )
)

        self.ec2_role.add_to_policy(
            iam.PolicyStatement(
            actions=["iam:PassRole"],
            resources=[
                f"arn:aws:iam::{self.account}:role/ris-360-glue-role-{env_name}"
        ]
    )
)


        # 🔹 Custom AMI (replace with your AMI ID)
        airflow_ami = ec2.MachineImage.lookup(
                name="ris-360-airflow-ami",
                owners=["self"]
        )


        # 🔹 EC2 Instance
        self.airflow_instance = ec2.Instance(
            self,
            "AirflowEC2Instance",
            instance_name=f"ris-360-airflow-ec2-{env_name}",
            instance_type=ec2.InstanceType("t3.medium"),
            machine_image=airflow_ami,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),
            security_group=airflow_sg,
            role=self.ec2_role
        )
        # Session manager access for EC2
        self.ec2_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
            "AmazonSSMManagedInstanceCore"
    )
)

