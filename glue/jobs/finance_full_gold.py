import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from awsglue.context import GlueContext

# ----------------------------------
# Initialize Glue
# ----------------------------------

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.sparkContext.setLogLevel("INFO")

# ----------------------------------
# Iceberg Configuration
# ----------------------------------

spark.conf.set(
    "spark.sql.catalog.glue_catalog",
    "org.apache.iceberg.spark.SparkCatalog"
)

spark.conf.set(
    "spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog"
)

spark.conf.set(
    "spark.sql.catalog.glue_catalog.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO"
)

spark.conf.set(
    "spark.sql.catalog.glue_catalog.warehouse",
    "s3://ris-360-gold-dev/warehouse/"
)

spark.conf.set(
    "spark.sql.iceberg.write.spark.fanout.enabled",
    "true"
)

spark.conf.set(
    "spark.sql.iceberg.write.distribution-mode",
    "hash"
)

# ----------------------------------
# Read Silver Dataset
# ----------------------------------

silver_path = "s3://ris-360-silver-dev/finance_full/"

df_full = spark.read.parquet(silver_path)

# Ensure correct partition clustering
df_full = df_full.withColumn("year", col("year").cast("int"))
df_full = df_full.filter(col("year").isNotNull())
df_full = df_full.repartition("year")

print("Row Count:", df_full.count())

# ----------------------------------
# Dimension: actuarial firm
# ----------------------------------

dim_actuarial_firm = (
    df_full
    .select("actuarialfirm")
    .dropDuplicates()
)

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.ris360_gold.dim_actuarial_firm (
    actuarialfirm STRING
)
USING iceberg
""")

dim_actuarial_firm.writeTo(
    "glue_catalog.ris360_gold.dim_actuarial_firm"
).append()

# ----------------------------------
# Fact: funding status
# ----------------------------------

fact_funding_status = df_full.select(
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

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.ris360_gold.fact_funding_status (
    equableid STRING,
    year INT,
    datefye STRING,
    ava DOUBLE,
    aal DOUBLE,
    uaal DOUBLE,
    fundedratio_actuarial DOUBLE,
    fundedratio_gasb DOUBLE,
    fundedratio_mva DOUBLE,
    discountrate DOUBLE,
    amortperiodr DOUBLE
)
USING iceberg
PARTITIONED BY (year)
""")

fact_funding_status.writeTo(
    "glue_catalog.ris360_gold.fact_funding_status"
).append()

# ----------------------------------
# Fact: investment returns
# ----------------------------------

fact_investment_returns = df_full.select(
    "equableid",
    "year",
    "arr",
    "returns_1year",
    "returns_3year",
    "returns_5year",
    "returns_10year",
    "returnclassification"
)

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.ris360_gold.fact_investment_returns (
    equableid STRING,
    year INT,
    arr DOUBLE,
    returns_1year DOUBLE,
    returns_3year DOUBLE,
    returns_5year DOUBLE,
    returns_10year DOUBLE,
    returnclassification STRING
)
USING iceberg
PARTITIONED BY (year)
""")

fact_investment_returns.writeTo(
    "glue_catalog.ris360_gold.fact_investment_returns"
).append()

# ----------------------------------
# Fact: membership
# ----------------------------------

fact_membership = df_full.select(
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

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.ris360_gold.fact_membership (
    equableid STRING,
    year INT,
    activemembertotal DOUBLE,
    inactivevestedmembers DOUBLE,
    inactivenonvested DOUBLE,
    beneficiariestotal DOUBLE,
    mem_nc DOUBLE,
    mem_uaal DOUBLE,
    mem_tot DOUBLE,
    emp_nc DOUBLE,
    emp_uaal DOUBLE,
    emp_tot DOUBLE
)
USING iceberg
PARTITIONED BY (year)
""")

fact_membership.writeTo(
    "glue_catalog.ris360_gold.fact_membership"
).append()

# ----------------------------------
# Fact: macro indicators
# ----------------------------------

fact_macro = df_full.select(
    "equableid",
    "year",
    "gdp",
    "stategenfundexpenditures",
    "stateownsourceexpenditures",
    "statetotalexpenditures"
)

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.ris360_gold.fact_macro_indicators (
    equableid STRING,
    year INT,
    gdp DOUBLE,
    stategenfundexpenditures DOUBLE,
    stateownsourceexpenditures DOUBLE,
    statetotalexpenditures DOUBLE
)
USING iceberg
PARTITIONED BY (year)
""")

fact_macro.writeTo(
    "glue_catalog.ris360_gold.fact_macro_indicators"
).append()

print("All Iceberg tables written successfully")