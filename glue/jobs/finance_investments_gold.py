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
# Ensure Gold DB exists
# ----------------------------------

spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.ris360_gold")

# ----------------------------------
# Read Silver Dataset
# ----------------------------------

silver_path = "s3://ris-360-silver-dev/finance_investments/"

df_investment = spark.read.parquet(silver_path)

# ----------------------------------
# Fact Table Transformation
# ----------------------------------

fact_investment_performance = (
    df_investment.select(
        col("equableid").cast("string"),
        col("investment_fund").cast("string"),
        col("year").cast("int"),
        col("month").cast("int"),
        col("datefye").cast("string"),
        col("arr").cast("double"),
        col("discountrate").cast("double"),
        col("returnclassification").cast("string"),
        col("returns_1year").cast("double"),
        col("returns_3year").cast("double"),
        col("returns_5year").cast("double"),
        col("returns_10year").cast("double")
    )
)

# ----------------------------------
# Create Iceberg Table
# ----------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.ris360_gold.fact_investment_performance (
    equableid STRING,
    investment_fund STRING,
    year INT,
    month INT,
    datefye STRING,
    arr DOUBLE,
    discountrate DOUBLE,
    returnclassification STRING,
    returns_1year DOUBLE,
    returns_3year DOUBLE,
    returns_5year DOUBLE,
    returns_10year DOUBLE
)
USING iceberg
PARTITIONED BY (year)
""")

# ----------------------------------
# Write Data
# ----------------------------------

fact_investment_performance.writeTo(
    "glue_catalog.ris360_gold.fact_investment_performance"
).append()

print("fact_investment_performance Iceberg table written successfully")