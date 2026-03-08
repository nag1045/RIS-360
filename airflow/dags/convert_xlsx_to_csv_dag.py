from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    "owner": "ris360",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="xlsx_to_csv_pipeline",
    default_args=default_args,
    description="Convert XLSX from ingestion bucket to CSV bronze bucket",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

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
    wait_for_completion=True
    )

    gold_cola_general = GlueJobOperator(
    task_id="gold_cola_general",
    job_name="ris360-benefits-cola-gold-job",
    aws_conn_id="aws_default",
    wait_for_completion=True
    )



    convert_task >> validate_task >> silver_task >> [gold_benefits_general,gold_cola_general]