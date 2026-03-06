from scripts.config import schemas

bronze_bucket = "s3://ris-360-bronze-dev"
silver_bucket = "s3://ris-360-silver-dev"


DATASETS = {

    "benefits_benefit_general": {
        "input": f"{bronze_bucket}/Equable-Benefits-Database__Benefit General.csv",
        "output": f"{silver_bucket}/benefits_benefit_general_clean.parquet",
        "schema": schemas.BENEFITS_BENEFIT_GENERAL_SCHEMA
    },

    "benefits_cola_2022": {
        "input": f"{bronze_bucket}/Equable-Benefits-Database__COLA Actuals 2022.csv",
        "output": f"{silver_bucket}/benefits_cola_2022_clean.parquet",
        "schema": schemas.COLA_ACTUALS_SCHEMA
    },

    "benefits_cola_2023": {
        "input": f"{bronze_bucket}/Equable-Benefits-Database__COLA Actuals 2023.csv",
        "output": f"{silver_bucket}/benefits_cola_2023_clean.parquet",
        "schema": schemas.COLA_ACTUALS_SCHEMA
    },

    "finance_full": {
        "input": f"{bronze_bucket}/Equable-Finance-Database-Full__Combined.csv",
        "output": f"{silver_bucket}/finance_full_clean.parquet",
        "schema": schemas.FINANCE_FULL_SCHEMA
    },

    "finance_investments": {
        "input": f"{bronze_bucket}/Equable-Finance-Database-Investments__Combined.csv",
        "output": f"{silver_bucket}/finance_investments_clean.parquet",
        "schema": schemas.INVESTMENTS_SCHEMA
    },

    "finance_contributions": {
        "input": f"{bronze_bucket}/Equable-Finance-Database-Contributions__Combined.csv",
        "output": f"{silver_bucket}/finance_contributions_clean.parquet",
        "schema": schemas.CONTRIBUTIONS_SCHEMA
    },

    "unfunded_liabilities": {
        "input": f"{bronze_bucket}/Sources-of-Unfunded-Liabilities-Database__Gain Loss Data.csv",
        "output": f"{silver_bucket}/unfunded_liabilities_clean.parquet",
        "schema": schemas.UNFUNDED_LIABILITIES_SCHEMA
    }
}
