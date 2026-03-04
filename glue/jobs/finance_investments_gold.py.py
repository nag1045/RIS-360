# Databricks notebook source
df_investment=spark.read.parquet('/Volumes/workspace/ris_schema/ris360_data/finance_investments_clean.parquet')

# COMMAND ----------

df_investment.columns

# COMMAND ----------

fact_investment_performance = (
    df_investment
    .select(
        "equableid",
        "investment_fund",
        "year",
        "month",
        "datefye",
        "arr",
        "discountrate",
        "returnclassification",
        "returns_1year",
        "returns_3year",
        "returns_5year",
        "returns_10year"
    )
)

# COMMAND ----------

fact_investment_performance.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet("/Volumes/workspace/ris_schema/ris360_data/gold/fact_investment_performance/")

# COMMAND ----------

