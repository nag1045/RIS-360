import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lower, greatest, when
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
# Ensure Gold DB exists
# ----------------------------------

spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.ris360_gold")

# ----------------------------------
# Read Silver Dataset
# ----------------------------------

silver_path = "s3://ris-360-silver-dev/benefit_general/"
df_general = spark.read.parquet(silver_path)

# ----------------------------------
# Transformations
# ----------------------------------

# Normalize Plan Type
df_general = df_general.withColumn(
    "plan_type_normalized",
    lower(col("plan_type"))
)

# Convert 0/1 → boolean
df_general = df_general.withColumn(
    "legacy_plan",
    when(col("legacy_plan") == 1, True).otherwise(False)
)

# Max employee contribution
df_general = df_general.withColumn(
    "max_employee_contribution_pct",
    greatest(
        "fas_eecont1",
        "fas_eecont2",
        "fas_eecont3",
        "fas_eecont4",
        "fas_eecont5",
        "fas_eecont6",
        "fas_eecont7",
        "fas_eecont8"
    )
)

# Vesting category
df_general = df_general.withColumn(
    "vesting_category",
    when(col("fas_vest") <= 5, "Fast Vesting")
    .when(col("fas_vest") <= 10, "Moderate Vesting")
    .otherwise("Slow Vesting")
)

# Risk score
df_general = df_general.withColumn(
    "risk_score",
    col("fas_additive_multipliers").cast("int") +
    col("risk_sharing_tools").cast("int") +
    col("fas_compoundcola").cast("int")
)

df_general = df_general.dropDuplicates(["equable_class_id"])

# ----------------------------------
# Flatten multiplier tiers
# ----------------------------------

df_general_mul_flat = df_general.selectExpr(
    "equable_class_id",
    """
    stack(7,
        1, fas_multiplier1, fas_multiplier1_yos,
        2, fas_multiplier2, fas_multiplier2_yos,
        3, fas_multiplier3, fas_multiplier3_yos,
        4, fas_multiplier4, fas_multiplier4_yos,
        5, fas_multiplier5, fas_multiplier5_yos,
        6, fas_multiplier6, fas_multiplier6_yos,
        7, fas_multiplier7, fas_multiplier7_yos
    ) as (tier_number, multiplier, yos)
    """
)

# ----------------------------------
# Write Gold Tables (Iceberg)
# ----------------------------------

df_general.writeTo(
    "glue_catalog.ris360_gold.dim_plan_features"
).createOrReplace()

df_general_mul_flat.writeTo(
    "glue_catalog.ris360_gold.dim_plan_multipliers"
).createOrReplace()

print("Gold tables written successfully")