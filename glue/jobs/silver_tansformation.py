import os
# print(os.environ["JAVA_HOME"])

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RIS-360-Local-Test")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet("D:\\RIS-360-DATA\\staging\\finance_full_clean.parquet")

df.printSchema()
df.show(5)
