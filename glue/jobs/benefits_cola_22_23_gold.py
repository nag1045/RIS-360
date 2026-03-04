import os
# print(os.environ["JAVA_HOME"])

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RIS-360-Gold") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()



input_folder = "/mnt/d/RIS-360-DATA/staging/"
output_folder = "/mnt/d/RIS-360-DATA/gold/"

df_cola = spark.read.parquet(input_folder+"benefits_cola_2022_clean.parquet")
df_cola.show()