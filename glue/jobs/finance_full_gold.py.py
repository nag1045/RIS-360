# Databricks notebook source
df_full=spark.read.parquet('/Volumes/workspace/ris_schema/ris360_data/finance_full_clean.parquet')

# COMMAND ----------

df_full.columns

# COMMAND ----------

df_full.groupBy("equableid", "year", "month") \
    .count() \
    .filter("count > 1") \
    .show()

# COMMAND ----------

dim_actuarial_firm = (
    df_full
    .select("actuarialfirm")
    .dropDuplicates()
)

dim_actuarial_firm.write \
    .mode("overwrite") \
    .parquet("/Volumes/workspace/ris_schema/ris360_data/gold/dim_actuarial_firm/")

# COMMAND ----------

fact_funding_status = (
    df_full
    .select(
        "equableid",
        "year",
        "datefye",
        "ava",
        "aal",
        "uaal",
        "fundedratio_actuarial",
        "fundedratio_gasb",
        "fundedratio_mva",
        "discountrate",
        "amortperiodr"
    )
)

fact_funding_status.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet("/Volumes/workspace/ris_schema/ris360_data/gold/fact_funding_status/")

# COMMAND ----------

fact_investment_returns = (
    df_full
    .select(
        "equableid",
        "year",
        "arr",
        "returns_1year",
        "returns_3year",
        "returns_5year",
        "returns_10year",
        "returnclassification"
    )
)

fact_investment_returns.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet("/Volumes/workspace/ris_schema/ris360_data/gold/fact_investment_returns/")

# COMMAND ----------

fact_membership = (
    df_full
    .select(
        "equableid",
        "year",
        "activemembertotal",
        "inactivevestedmembers",
        "inactivenonvested",
        "beneficiariestotal",
        "mem_nc",
        "mem_uaal",
        "mem_tot",
        "emp_nc",
        "emp_uaal",
        "emp_tot"
    )
)

fact_membership.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet("/Volumes/workspace/ris_schema/ris360_data/gold/fact_membership/")

# COMMAND ----------

fact_macro = (
    df_full
    .select(
        "equableid",
        "year",
        "gdp",
        "stategenfundexpenditures",
        "stateownsourceexpenditures",
        "statetotalexpenditures"
    )
)

fact_macro.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet("/Volumes/workspace/ris_schema/ris360_data/gold/fact_macro_indicators/")

# COMMAND ----------

