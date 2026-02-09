from aws_cdk import (
    Stack,
    aws_ec2 as ec2
)
from constructs import Construct


class SecurityGroupStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, vpc: ec2.Vpc, env_name: str, my_ip: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # 🔐 Airflow EC2 SG
        self.airflow_sg = ec2.SecurityGroup(
            self,
            "AirflowSecurityGroup",
            vpc=vpc,
            security_group_name=f"ris-360-airflow-sg-{env_name}",
            description=f"Airflow SG for {env_name}",
            allow_all_outbound=True
        )

        if env_name != "prod":
            self.airflow_sg.add_ingress_rule(
                peer=ec2.Peer.ipv4(my_ip),
                connection=ec2.Port.tcp(22),
                description="SSH from local IP"
            )

            self.airflow_sg.add_ingress_rule(
                peer=ec2.Peer.ipv4(my_ip),
                connection=ec2.Port.tcp(8080),
                description="Airflow UI from local IP"
            )

        # 🔐 Redshift SG
        self.redshift_sg = ec2.SecurityGroup(
            self,
            "RedshiftSecurityGroup",
            vpc=vpc,
            security_group_name=f"ris-360-redshift-sg-{env_name}",
            description=f"Redshift SG for {env_name}",
            allow_all_outbound=True
        )

        self.redshift_sg.add_ingress_rule(
            peer=self.airflow_sg,
            connection=ec2.Port.tcp(5439),
            description="Redshift access from Airflow"
        )
