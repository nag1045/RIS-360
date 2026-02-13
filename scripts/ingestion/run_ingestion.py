import pandas as pd
import argparse
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from scripts.utils.schema_utils import enforce_schema
from scripts.config.datasets import DATASETS


def process_dataset(name):

    if name not in DATASETS:
        print(f"❌ Dataset '{name}' not found in DATASETS config.")
        print("Available datasets:")
        for key in DATASETS.keys():
            print(f"  - {key}")
        sys.exit(1)

    config = DATASETS[name]

    print(f"\n🚀 Processing dataset: {name}")
    print(f"📥 Input:  {config['input']}")
    print(f"📤 Output: {config['output']}")

    # Read CSV
    df = pd.read_csv(config["input"])

    print(f"📊 Rows: {len(df)} | Columns: {len(df.columns)}")

    # Enforce schema
    df = enforce_schema(df, config["schema"])

    # Save to Parquet
    df.to_parquet(config["output"], engine="pyarrow", index=False)

    print(f"✅ {name} processed successfully")


def run_all():
    print("\n🔁 Running ingestion for ALL datasets...\n")
    for dataset_name in DATASETS.keys():
        process_dataset(dataset_name)
    print("\n🎉 All datasets processed successfully")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        type=str,
        help="Dataset name to process (if not provided, all datasets will run)"
    )

    args = parser.parse_args()

    if args.dataset:
        process_dataset(args.dataset)
    else:
        run_all()
