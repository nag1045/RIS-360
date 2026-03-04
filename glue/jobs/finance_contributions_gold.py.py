# Databricks notebook source
df_contributions=spark.read.parquet('/Volumes/workspace/ris_schema/ris360_data/finance_contributions_clean.parquet')

# COMMAND ----------

df_contributions.display()

# COMMAND ----------

df_contributions.columns

# COMMAND ----------

from pyspark.sql.functions import col

dim_contri = (
    df_contributions
    .select(
        "equableid",
        "system_name",
        "plan_fullname",
        "plan_internalname",
        "plan_shorthand",
        "stateabbrev",
        "statename"
    )
    .dropDuplicates(["equableid"])
)
dim_contri.write.mode('overwrite').parquet('/Volumes/workspace/ris_schema/ris360_data/gold/dim_contributions/')


# COMMAND ----------

fact_contri= (
    df_contributions
    .select(
        "equableid",
        "datefye",
        "month",
        "year",
        "adec",
        "adecaspercentofcoveredpayroll",
        "paidcontributionaspercentofcover",
        "requiredemployercontribution",
        "paidemployercontribution",
        "percentreqcontpaidcontpaidasper",
        "mem_nc",
        "mem_uaal",
        "mem_tot",
        "emp_nc",
        "emp_uaal",
        "emp_tot",
        "nc_tot",
        "uaal_tot"
    )
)

# COMMAND ----------

fact_contri.write.mode('overwrite').partitionBy('year').parquet('/Volumes/workspace/ris_schema/ris360_data/gold/fact_contributions/')

# COMMAND ----------

