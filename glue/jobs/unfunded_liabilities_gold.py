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

silver_path = "s3://ris-360-silver-dev/unfunded_liabilities/"

df_ul = spark.read.parquet(silver_path)

# ----------------------------------
# Fact Table Transformation
# ----------------------------------

fact_unfunded_liabilities = (
    df_ul.select(
        col("equableid").cast("string"),
        col("year").cast("int"),
        col("uaal").cast("double"),
        col("arr").cast("double"),
        col("agginvest").cast("double"),
        col("aggdemo").cast("double"),
        col("aggassume").cast("double"),
        col("aggbenefit").cast("double"),
        col("aggcont").cast("double"),
        col("agginterest").cast("double"),
        col("aggother").cast("double"),
        col("agglegacy").cast("double"),
        col("aggstart").cast("double"),
        col("aggundoc").cast("double"),
        col("aggtotal").cast("double"),
        col("investexpnonaggregated").cast("double"),
        col("otherinvestmentrelated").cast("double"),
        col("generalunspecifiedassetgain").cast("double"),
        col("demographicexperiencepayroll").cast("double"),
        col("demographicexperiencemortalit").cast("double"),
        col("demographicexperienceturnover").cast("double"),
        col("demographicexperiencedisabili").cast("double"),
        col("assumedreturnchange").cast("double"),
        col("mortalityassumptionchange").cast("double"),
        col("payrollassumptionchange").cast("double"),
        col("otherspecifiedassumptioncha").cast("double"),
        col("otheractuarialmethodchange").cast("double"),
        col("generalunspecifiedassumption").cast("double"),
        col("benefitformulachange").cast("double"),
        col("colachange").cast("double"),
        col("colaexperience").cast("double"),
        col("servicepurchases").cast("double"),
        col("benefitexperience").cast("double"),
        col("generalunspecifiedbenefitde").cast("double"),
        col("contributiondeficiencysurplus").cast("double"),
        col("expectedchangeintheuaalint").cast("double"),
        col("changestofundingpolicy").cast("double"),
        col("generalunspecifiedactuarial").cast("double"),
        col("generalunspecifiedexperience").cast("double"),
        col("unspecifiedamendments").cast("double"),
        col("datacorrections").cast("double"),
        col("changeactuarialfirm").cast("double"),
        col("undeclaredother").cast("double")
    )
)

# ----------------------------------
# Create Iceberg Table
# ----------------------------------

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.ris360_gold.fact_unfunded_liabilities (
    equableid STRING,
    year INT,
    uaal DOUBLE,
    arr DOUBLE,
    agginvest DOUBLE,
    aggdemo DOUBLE,
    aggassume DOUBLE,
    aggbenefit DOUBLE,
    aggcont DOUBLE,
    agginterest DOUBLE,
    aggother DOUBLE,
    agglegacy DOUBLE,
    aggstart DOUBLE,
    aggundoc DOUBLE,
    aggtotal DOUBLE,
    investexpnonaggregated DOUBLE,
    otherinvestmentrelated DOUBLE,
    generalunspecifiedassetgain DOUBLE,
    demographicexperiencepayroll DOUBLE,
    demographicexperiencemortalit DOUBLE,
    demographicexperienceturnover DOUBLE,
    demographicexperiencedisabili DOUBLE,
    assumedreturnchange DOUBLE,
    mortalityassumptionchange DOUBLE,
    payrollassumptionchange DOUBLE,
    otherspecifiedassumptioncha DOUBLE,
    otheractuarialmethodchange DOUBLE,
    generalunspecifiedassumption DOUBLE,
    benefitformulachange DOUBLE,
    colachange DOUBLE,
    colaexperience DOUBLE,
    servicepurchases DOUBLE,
    benefitexperience DOUBLE,
    generalunspecifiedbenefitde DOUBLE,
    contributiondeficiencysurplus DOUBLE,
    expectedchangeintheuaalint DOUBLE,
    changestofundingpolicy DOUBLE,
    generalunspecifiedactuarial DOUBLE,
    generalunspecifiedexperience DOUBLE,
    unspecifiedamendments DOUBLE,
    datacorrections DOUBLE,
    changeactuarialfirm DOUBLE,
    undeclaredother DOUBLE
)
USING iceberg
PARTITIONED BY (year)
""")

# ----------------------------------
# Write Data
# ----------------------------------

fact_unfunded_liabilities.writeTo(
    "glue_catalog.ris360_gold.fact_unfunded_liabilities"
).append()

print("fact_unfunded_liabilities Iceberg table written successfully")