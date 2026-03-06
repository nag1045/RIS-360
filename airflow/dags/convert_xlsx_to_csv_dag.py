from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
    convert_task >> validate_task >> silver_task