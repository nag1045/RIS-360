import boto3
import logging

logger = logging.getLogger(__name__)

s3 = boto3.client("s3")

LANDING_BUCKET = "ris-360-landing-dev"

def move_processed_file(file_key):

    source = {
        "Bucket": LANDING_BUCKET,
        "Key": file_key
    }

    destination_key = file_key.replace("incoming/", "processed/")

    logger.info(f"Moving {file_key} to processed folder")

    # Copy file
    s3.copy_object(
        Bucket=LANDING_BUCKET,
        CopySource=source,
        Key=destination_key
    )

    # Delete original
    s3.delete_object(
        Bucket=LANDING_BUCKET,
        Key=file_key
    )

    logger.info(f"File moved to {destination_key}")