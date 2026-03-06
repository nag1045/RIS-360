import boto3
import pandas as pd

bronze_bucket = "ris-360-bronze-dev"

s3 = boto3.client("s3")

print("\n🔍 Starting CSV Validation...\n")

response = s3.list_objects_v2(Bucket=bronze_bucket)

for obj in response.get("Contents", []):

    file = obj["Key"]

    if not file.endswith(".csv"):
        continue

    file_path = f"s3://{bronze_bucket}/{file}"

    print(f"\n📂 File: {file}")
    print("-" * 60)

    df = pd.read_csv(file_path)

    # Row & Column Count
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")

    # Data Types
    print("\nColumn Data Types:")
    print(df.dtypes)

    # Null Percentage
    null_pct = (df.isnull().sum() / len(df)) * 100
    print("\nNull % by Column:")
    print(null_pct[null_pct > 0].sort_values(ascending=False))

    # Duplicate Rows
    dup_count = df.duplicated().sum()
    print(f"\nDuplicate Rows: {dup_count}")

    # Completely Empty Columns
    empty_cols = df.columns[df.isnull().all()].tolist()
    if empty_cols:
        print(f"\nCompletely Empty Columns: {empty_cols}")

    print("=" * 60)

print("\n✅ Validation Completed.")