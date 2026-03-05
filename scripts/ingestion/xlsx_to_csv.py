import pandas as pd
import os
import yaml
from move_processed_file import move_processed_file

# Load YAML config
dataset_config_folder="/home/ubuntu/RIS-360/configs"
config_file="dataset_config.yaml"
yaml_file_path=os.path.join(dataset_config_folder, config_file)
with open(yaml_file_path, "r") as f:
    config = yaml.safe_load(f)

# Input and output folders
input_folder = "s3://ris-360-landing-dev/incoming"
output_folder = "s3://ris-360-bronze-dev"

os.makedirs(output_folder, exist_ok=True)

# Loop through datasets defined in YAML
for dataset_name, dataset_info in config["datasets"].items():
    
    file_name = dataset_info["file"]
    allowed_sheets = dataset_info["sheets"]
    
    file_path = os.path.join(input_folder, file_name)
    
    print(f"\nProcessing dataset: {dataset_name}")
    
    xls = pd.ExcelFile(file_path)
    
    print("Available sheets:", xls.sheet_names)
    
    for sheet_name in allowed_sheets:
        
        if sheet_name not in xls.sheet_names:
            print(f"⚠ Sheet not found: {sheet_name}")
            continue
        
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        # Clean column names
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

        

print("\nAll configured sheets processed.")
move_processed_file(f"incoming/{file_name}")