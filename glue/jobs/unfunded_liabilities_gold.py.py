# Databricks notebook source
df_ul=spark.read.parquet('/Volumes/workspace/ris_schema/ris360_data/unfunded_liabilities_clean.parquet')

# COMMAND ----------

df_ul.columns 

# COMMAND ----------

fact_unfunded_liabilities = (
    df_ul.select(
        "equableid",
        "year",
        "uaal",
        "arr",
        "agginvest",
        "aggdemo",
        "aggassume",
        "aggbenefit",
        "aggcont",
        "agginterest",
        "aggother",
        "agglegacy",
        "aggstart",
        "aggundoc",
        "aggtotal",
        "investexpnonaggregated",
        "otherinvestmentrelated",
        "generalunspecifiedassetgain",
        "demographicexperiencepayroll",
        "demographicexperiencemortalit",
        "demographicexperienceturnover",
        "demographicexperiencedisabili",
        "assumedreturnchange",
        "mortalityassumptionchange",
        "payrollassumptionchange",
        "otherspecifiedassumptioncha",
        "otheractuarialmethodchange",
        "generalunspecifiedassumption",
        "benefitformulachange",
        "colachange",
        "colaexperience",
        "servicepurchases",
        "benefitexperience",
        "generalunspecifiedbenefitde",
        "contributiondeficiencysurplus",
        "expectedchangeintheuaalint",
        "changestofundingpolicy",
        "generalunspecifiedactuarial",
        "generalunspecifiedexperience",
        "unspecifiedamendments",
        "datacorrections",
        "changeactuarialfirm",
        "undeclaredother"
    )
)

# COMMAND ----------

fact_unfunded_liabilities.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet("/Volumes/workspace/ris_schema/ris360_data/gold/fact_unfunded_liabilities")

# COMMAND ----------

