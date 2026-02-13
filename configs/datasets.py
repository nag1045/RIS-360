from scripts.config import schemas

RAW_PATH = r"D:\RIS-360-DATA\raw_csv"
STAGING_PATH = r"D:\RIS-360-DATA\staging"

DATASETS = {

    "benefits_benefit_general": {
        "input": f"{RAW_PATH}\\Equable-Benefits-Database__Benefit General.csv",
        "output": f"{STAGING_PATH}\\benefits_benefit_general_clean.parquet",
        "schema": schemas.BENEFITS_BENEFIT_GENERAL_SCHEMA
    },

    "benefits_cola_2022": {
        "input": f"{RAW_PATH}\\Equable-Benefits-Database__COLA Actuals 2022.csv",
        "output": f"{STAGING_PATH}\\benefits_cola_2022_clean.parquet",
        "schema": schemas.COLA_ACTUALS_SCHEMA
    },

    "benefits_cola_2023": {
        "input": f"{RAW_PATH}\\Equable-Benefits-Database__COLA Actuals 2023.csv",
        "output": f"{STAGING_PATH}\\benefits_cola_2023_clean.parquet",
        "schema": schemas.COLA_ACTUALS_SCHEMA
    },

    "finance_full": {
        "input": f"{RAW_PATH}\\Equable-Finance-Database-Full__Combined.csv",
        "output": f"{STAGING_PATH}\\finance_full_clean.parquet",
        "schema": schemas.FINANCE_FULL_SCHEMA
    },

    "finance_investments": {
        "input": f"{RAW_PATH}\\Equable-Finance-Database-Investments__Combined.csv",
        "output": f"{STAGING_PATH}\\finance_investments_clean.parquet",
        "schema": schemas.INVESTMENTS_SCHEMA
    },

    "finance_contributions": {
        "input": f"{RAW_PATH}\\Equable-Finance-Database-Contributions__Combined.csv",
        "output": f"{STAGING_PATH}\\finance_contributions_clean.parquet",
        "schema": schemas.CONTRIBUTIONS_SCHEMA
    },

    "unfunded_liabilities": {
        "input": f"{RAW_PATH}\\Sources-of-Unfunded-Liabilities-Database__Gain Loss Data.csv",
        "output": f"{STAGING_PATH}\\unfunded_liabilities_clean.parquet",
        "schema": schemas.UNFUNDED_LIABILITIES_SCHEMA
    }
}
