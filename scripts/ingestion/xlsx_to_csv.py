import pandas as pd
import os
import yaml
import logging
import sys
sys.path.insert(0, "/home/ubuntu/RIS-360")
from move_processed_file import move_processed_file
from scripts.utils.metadata_logger import is_file_processed, log_file_status
from airflow.utils.log.logging_mixin import LoggingMixin
import boto3
logger = LoggingMixin().log

#################################################################################################
run_id = sys.argv[1]

s3 = boto3.client("s3")

bucket = "ris-360-landing-dev"
prefix = "incoming/"

response = s3.list_objects_v2(
    Bucket=bucket,
    Prefix=prefix
)

files = response.get("Contents", [])

for obj in files:

    key = obj["Key"]

    if not key.endswith(".xlsx"):
        continue

    file_name = key.split("/")[-1]
    dataset_name = file_name.split("_")[0]

    print("Processing file:", file_name)
    print("Dataset:", dataset_name)

    # ---------------------------
    # Check if file already processed
    # ---------------------------
    if is_file_processed(file_name):

        print(f"{file_name} already processed, skipping")

        log_file_status(
            file_name,
            dataset_name,
            "SKIPPED",
            0,
            run_id
        )

        continue

    # ---------------------------
    # Process file
    # ---------------------------

    dataset_config_folder="/home/ubuntu/RIS-360/scripts/config"
    config_file="dataset_config.yaml"
    yaml_file_path=os.path.join(dataset_config_folder, config_file)

    with open(yaml_file_path, "r") as f:
        config = yaml.safe_load(f)

    input_folder = "s3://ris-360-landing-dev/incoming"
    output_folder = "s3://ris-360-bronze-dev"

    for dataset_name, dataset_info in config["datasets"].items():

        file_name_config = dataset_info["file"]

        if file_name_config != file_name:
            continue

        allowed_sheets = dataset_info["sheets"]

        file_path = os.path.join(input_folder, file_name)

        xls = pd.ExcelFile(file_path)

        for sheet_name in allowed_sheets:

            if sheet_name not in xls.sheet_names:
                print(f"⚠ Sheet not found: {sheet_name}")
                continue

            df = pd.read_excel(file_path, sheet_name=sheet_name)

            df.columns = (
                df.columns
                .str.strip()
                .str.lower()
                .str.replace(" ", "_")
                .str.replace("%", "pct")
                .str.replace("/", "_")
            )

            output_file = f"{dataset_name}__{sheet_name}.csv"
            output_path = os.path.join(output_folder, output_file)

            df.to_csv(output_path, index=False)

            print(f"✅ Converted: {output_file}")

    move_processed_file(f"incoming/{file_name}")

    log_file_status(
        file_name,
        dataset_name,
        "PROCESSED",
        len(df),
        run_id
    )

print("\nAll configured sheets processed.")