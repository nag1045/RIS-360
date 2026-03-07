import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from awsglue.context import GlueContext

# ----------------------------------
# Initialize Glue + Spark
# ----------------------------------

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ----------------------------------
# Iceberg Catalog Configuration
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
    "s3://ris-360-gold-dev/"
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
# Paths
# ----------------------------------

silver_path = "s3://ris-360-silver-dev/finance_contributions/finance_contributions_clean.parquet"

# ----------------------------------
# Read Silver Data
# ----------------------------------

df_contributions = spark.read.parquet(silver_path)
df_contributions = df_contributions.repartition("year")
print("Columns:", df_contributions.columns)
print("Row Count:", df_contributions.count())

df_contributions.createOrReplaceTempView("contributions")

# ----------------------------------
# Dimension Table (Iceberg)
# ----------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.ris360_gold.dim_contributions
USING iceberg
AS
SELECT DISTINCT
    equableid,
    system_name,
    plan_fullname,
    plan_internalname,
    plan_shorthand,
    stateabbrev,
    statename
FROM contributions
""")

# ----------------------------------
# Fact Table (Iceberg)
# ----------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.ris360_gold.fact_contributions (
    equableid STRING,
    datefye STRING,
    month INT,
    year INT,
    adec DOUBLE,
    adecaspercentofcoveredpayroll DOUBLE,
    paidcontributionaspercentofcover DOUBLE,
    requiredemployercontribution DOUBLE,
    paidemployercontribution DOUBLE,
    percentreqcontpaidcontpaidasper DOUBLE,
    mem_nc DOUBLE,
    mem_uaal DOUBLE,
    mem_tot DOUBLE,
    emp_nc DOUBLE,
    emp_uaal DOUBLE,
    emp_tot DOUBLE,
    nc_tot DOUBLE,
    uaal_tot DOUBLE
)
USING iceberg
PARTITIONED BY (year)
""")


fact_contri = (
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

fact_contri.writeTo(
    "glue_catalog.ris360_gold.fact_contributions"
).append()


print("Gold Iceberg tables generated successfully")