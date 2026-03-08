from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_s3_deployment as s3deploy,
    aws_iam as iam,
)
from constructs import Construct
import os

glue_jobs_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../glue/jobs")
)

scripts_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../scripts/")
)

class GlueStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 artifact_bucket,glue_role,
                 env_name: str,
                 **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # # Glue role
        # glue_role = iam.Role(
        #     self,
        #     "GlueRole",
        #     assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        # )

        # glue_role.add_managed_policy(
        #     iam.ManagedPolicy.from_aws_managed_policy_name(
        #         "service-role/AWSGlueServiceRole"
        #     )
        # )

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


        # Example Glue Job
        glue.CfnJob(
            self,
            "GoldJobBenefitsGeneral",
            name="ris360-benefits-general-gold-job",   # <-- explicit Glue job name
            role=glue_role.role_arn,

            command=glue.CfnJob.JobCommandProperty(
            name="glueetl",
            script_location=f"s3://{artifact_bucket.bucket_name}/glue/current/benefit_general_gold.py",
            python_version="3"
         ),

            glue_version="4.0",

            worker_type="G.1X",
            number_of_workers=2,

        execution_property=glue.CfnJob.ExecutionPropertyProperty(
        max_concurrent_runs=1
    ),

        default_arguments={
            "--job-language": "python",
            "--TempDir": f"s3://{artifact_bucket.bucket_name}/temp/",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-metrics": "true",
            "--enable-job-insights": "true",
            "--job-bookmark-option": "job-bookmark-enable"

            # Iceberg support
            "--datalake-formats": "iceberg",

            # Spark Iceberg configs
            "--conf": "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
            "--conf": "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
            "--conf": "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
            "--conf": "spark.sql.catalog.glue_catalog.warehouse=s3://ris-360-gold-dev/warehouse/",
            "--conf": "spark.sql.iceberg.write.spark.fanout.enabled=true",
            "--conf": "spark.sql.iceberg.write.distribution-mode=hash"
    },

            max_retries=1,
            timeout=60
)