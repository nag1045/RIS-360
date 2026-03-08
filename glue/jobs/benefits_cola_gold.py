import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col, when
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
# Ensure Gold database exists
# ----------------------------------

spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.ris360_gold")

# ----------------------------------
# Read Silver Dataset
# ----------------------------------

silver_path = "s3://ris-360-silver-dev/benefits_cola/"

df_cola = spark.read.parquet(silver_path)

df_cola = df_cola.withColumn("actualyear", col("actualyear").cast("int"))
df_cola = df_cola.filter(col("actualyear").isNotNull())
df_cola = df_cola.repartition("actualyear")

print("Row Count:", df_cola.count())

# ----------------------------------
# Dimension: Plan
# ----------------------------------

dim_plan = (
    df_cola.select(
        col("equableclassid").cast("string"),
        col("class_name").cast("string"),
        col("occupation").cast("string"),

        # FIX: convert 0/1 → boolean
        when(col("legacy_plan") == 1, True).otherwise(False).alias("legacy_plan"),
        when(col("rsr_exclude") == 1, True).otherwise(False).alias("rsr_exclude"),

        col("state").cast("string"),
        col("plan_name").cast("string"),
        col("tier_hiredates").cast("string"),
        col("plan_type").cast("string")
    )
    .dropDuplicates(["equableclassid"])
)

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.ris360_gold.dim_plan (
    equableclassid STRING,
    class_name STRING,
    occupation STRING,
    legacy_plan BOOLEAN,
    rsr_exclude BOOLEAN,
    state STRING,
    plan_name STRING,
    tier_hiredates STRING,
    plan_type STRING
)
USING iceberg
""")

dim_plan.writeTo(
    "glue_catalog.ris360_gold.dim_plan"
).append()

# ----------------------------------
# Fact: COLA metrics
# ----------------------------------

fact_cola = df_cola.select(
    col("equableclassid").cast("string"),
    col("actualyear").cast("int"),
    col("cola_actual").cast("double"),
    col("cola_amount_provisions").cast("double"),
    col("cola_amount_rsr").cast("double"),
    col("compound_cola").cast("double"),
    col("cola_datepaid").cast("string"),
    col("cola_distribution_provision").cast("string")
)

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.ris360_gold.fact_cola (
    equableclassid STRING,
    actualyear INT,
    cola_actual DOUBLE,
    cola_amount_provisions DOUBLE,
    cola_amount_rsr DOUBLE,
    compound_cola DOUBLE,
    cola_datepaid STRING,
    cola_distribution_provision STRING
)
USING iceberg
PARTITIONED BY (actualyear)
""")

fact_cola.writeTo(
    "glue_catalog.ris360_gold.fact_cola"
).append()

print("Gold COLA Iceberg tables written successfully")