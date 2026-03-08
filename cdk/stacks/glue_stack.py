from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_s3_deployment as s3deploy,
    aws_iam as iam,
)
from constructs import Construct
import yaml
import os

glue_jobs_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../glue/jobs")
)

scripts_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../scripts/")
)

glue_job_config_path = os.path.join(
    os.path.dirname(__file__),
    "../../scripts/config/glue_jobs.yaml"
)


class GlueStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 artifact_bucket,glue_role,
                 env_name: str,
                 **kwargs):
        super().__init__(scope, construct_id, **kwargs)


        # Allow Glue to read artifact bucket
        artifact_bucket.grant_read(glue_role)

        # Upload glue_jobs folder to artifact bucket
        s3deploy.BucketDeployment(
            self,
            "DeployGlueScripts",
            sources=[s3deploy.Source.asset(glue_jobs_path)],
            destination_bucket=artifact_bucket,
            destination_key_prefix="glue/jobs/"
        )


# this will make sure to move all the scripts file to s3
        s3deploy.BucketDeployment(
            self,
            "DeployScripts",
            sources=[s3deploy.Source.asset(scripts_path)],
            destination_bucket=artifact_bucket,
            destination_key_prefix="scripts/"
        )

        with open(glue_job_config_path) as f: #opening the glue job details yaml file
            job_config = yaml.safe_load(f)

        for job in job_config["jobs"]: #this will make all the required glue job at once 

            glue.CfnJob(
                self,
                job["id"],

                name=job["name"],

                role=glue_role.role_arn,

                command=glue.CfnJob.JobCommandProperty(
                    name="glueetl",
                    script_location=f"s3://{artifact_bucket.bucket_name}/glue/jobs/{job['script']}",
                    python_version="3"
                ),

                glue_version="4.0",
                worker_type="G.1X",
                number_of_workers=2,

                default_arguments={
                    "--job-language": "python",
                    "--TempDir": f"s3://{artifact_bucket.bucket_name}/temp/",
                    "--enable-continuous-cloudwatch-log": "true",
                    "--enable-metrics": "true",
                    "--enable-job-insights": "true",
                    "--job-bookmark-option": "job-bookmark-enable",
                    "--datalake-formats": "iceberg"
        }
    )