import os
import pandas as pd

csv_folder = "D:\\RIS-360-DATA\\raw_csv"
data_type_folder="D:\\RIS-360-DATA\\data_types"

print("\n🔍 Starting CSV Validation...\n")

for file in os.listdir(csv_folder):
    if not file.endswith(".csv"):
        continue

    file_path = os.path.join(csv_folder, file)
    print(f"\n📂 File: {file}")
    print("-" * 60)

    df = pd.read_csv(file_path)

    # Row & Column Count
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")

    # Data Types
    print("\nColumn Data Types:")
    print(df.dtypes)
    data_type_path=os.path.join(data_type_folder,file.split('.')[0]+"_pandas_dtypes.csv")
    df.dtypes.to_csv(data_type_path)


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
