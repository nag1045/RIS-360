from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import sys
import os
sys.path.insert(0, "/home/ubuntu/RIS-360")
from scripts.utils.metadata_logger import log_pipeline_start, log_pipeline_end
from airflow.operators.python import PythonOperator

def start_logging(**context):

    run_id = context['dag_run'].run_id
    log_pipeline_start(run_id)

def end_logging(**context):

    run_id = context['dag_run'].run_id
    log_pipeline_end(run_id, "SUCCESS")

default_args = {
    "owner": "ris360",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="ris360_pipeline_dag",
    default_args=default_args,
    description="It will process RIS data from RAW to GOLD",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:
    

    log_pipeline_start_task = PythonOperator(
    task_id="log_pipeline_start",
    python_callable=start_logging,
    provide_context=True
    )

    convert_task = BashOperator(
        task_id="convert_xlsx_to_csv",
        bash_command="python3 /home/ubuntu/RIS-360/scripts/ingestion/xlsx_to_csv.py"
    )

    validate_task = BashOperator(
        task_id="validate_csv",
        bash_command="python3 /home/ubuntu/RIS-360/scripts/ingestion/validate_csv.py"
    )

    silver_task = BashOperator(
        task_id="silver_ingestion",
        bash_command="python3 /home/ubuntu/RIS-360/scripts/ingestion/run_silver_ingestion.py"
    )

    gold_benefits_general = GlueJobOperator(
    task_id="gold_benefits_general",
    job_name="ris360-benefits-general-gold-job",
    aws_conn_id="aws_default",
    region_name="us-east-1",
    wait_for_completion=True
    )

    gold_cola_general = GlueJobOperator(
    task_id="gold_cola_general",
    job_name="ris360-benefits-cola-gold-job",
    aws_conn_id="aws_default",
    region_name="us-east-1",
    wait_for_completion=True
    )


    gold_finance_investments = GlueJobOperator(
    task_id="gold_finance_investments",
    job_name="ris360-finance-investments-gold-job",
    aws_conn_id="aws_default",
    region_name="us-east-1",
    wait_for_completion=True
    )


    gold_finance_contributions = GlueJobOperator(
    task_id="gold_finance_contributions",
    job_name="ris360-finance-contributions-gold-job",
    aws_conn_id="aws_default",
    region_name="us-east-1",
    wait_for_completion=True
    )


    gold_unfunded_liabilities = GlueJobOperator(
    task_id="gold_unfunded_liabilities",
    job_name="ris360-unfunded-liabilities-gold-job",
    aws_conn_id="aws_default",
    region_name="us-east-1",
    wait_for_completion=True
    )    


    gold_finance_full = GlueJobOperator(
    task_id="gold_finance_full",
    job_name="ris360-finance-full-gold-job",
    aws_conn_id="aws_default",
    region_name="us-east-1",
    wait_for_completion=True
    )    

    log_pipeline_end_task = PythonOperator(
    task_id="log_pipeline_end",
    python_callable=end_logging,
    provide_context=True
    )

    log_pipeline_start_task>>convert_task >> validate_task >> silver_task >> [gold_benefits_general,gold_cola_general,
                                                     gold_finance_investments,gold_finance_contributions,
                                                     gold_unfunded_liabilities,gold_finance_full] >>log_pipeline_end_task