from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_s3_deployment as s3deploy,
    aws_iam as iam,
)
from constructs import Construct
import os

configs_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../configs/")
)

glue_jobs_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../glue/jobs")
)

scripts_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../scripts/")
)

class GlueStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 artifact_bucket,
                 env_name: str,
                 **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Glue role
        glue_role = iam.Role(
            self,
            "GlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        )

        glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        )

        # Allow Glue to read artifact bucket
        artifact_bucket.grant_read(glue_role)


        s3deploy.BucketDeployment(
            self,
            "DeployGlueScripts",
            sources=[s3deploy.Source.asset(configs_path)],
            destination_bucket=artifact_bucket,
            destination_key_prefix="configs/"
        )

        # Upload glue_jobs folder to artifact bucket
        s3deploy.BucketDeployment(
            self,
            "DeployGlueScripts",
            sources=[s3deploy.Source.asset(glue_jobs_path)],
            destination_bucket=artifact_bucket,
            destination_key_prefix="glue/jobs/"
        )

        s3deploy.BucketDeployment(
            self,
            "DeployGlueScripts",
            sources=[s3deploy.Source.asset(scripts_path)],
            destination_bucket=artifact_bucket,
            destination_key_prefix="scripts/"
        )


        # Example Glue Job
        glue.CfnJob(
            self,
            "SilverJob",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{artifact_bucket.bucket_name}/glue/current/silver.py"
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2
        )