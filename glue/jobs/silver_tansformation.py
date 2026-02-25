import os
# print(os.environ["JAVA_HOME"])

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RIS-360-Gold") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()


# input_folder = "D:\\RIS-360-DATA\\staging\\"
# output_folder = "D:\\RIS-360-DATA\\gold\\"

input_folder = "/mnt/d/RIS-360-DATA/staging/"
output_folder = "/mnt/d/RIS-360-DATA/gold/"

df_general = spark.read.parquet(input_folder+"benefits_benefit_general_clean.parquet")

from pyspark.sql.functions import lower
df_general=df_general.withColumn('plan_type_normalized',lower('plan_type')) #Normalize Plan Type

# COMMAND ----------

# df_general.display()

# COMMAND ----------

# df_general.select('legacy_plan').show(5)
# df_general.select('legacy_plan').printSchema

# COMMAND ----------

from pyspark.sql.functions import col
df_general=df_general.withColumn('legacy_plan',col('legacy_plan').cast('boolean'))   #Standardize Binary Columns

# COMMAND ----------

# df_general.select('legacy_plan').show(5)

# COMMAND ----------

# df_general.select('legacy_plan').distinct().show(5)

# COMMAND ----------

from pyspark.sql.functions import greatest 
df_general=df_general.withColumn('max_employee_contribution_pct',greatest(
    "fas_eecont1",
        "fas_eecont2",
        "fas_eecont3",
        "fas_eecont4",
        "fas_eecont5",
        "fas_eecont6",
        "fas_eecont7",
        "fas_eecont8"
))   # Max Employee Contribution %


# COMMAND ----------

# df_general.select('max_employee_contribution_pct').show(5)

# COMMAND ----------

from pyspark.sql.functions import when,col
df_general=df_general.withColumn('vesting_category',when(col('fas_vest')<=5,'Fast Vesting')\
                                         .when(col('fas_vest')<=10,'Moderate Vesting')\
                                         .otherwise('Slow Vesting'))      # Vesting Years Category

# COMMAND ----------

df_general.select('vesting_category').distinct().show()

# COMMAND ----------

df_general=df_general.withColumn('risk_score',(
                                    col('fas_additive_multipliers').cast('int')+ 
                                    col('risk_sharing_tools').cast('int')+
                                    col('fas_compoundcola').cast('int')
)
                      )    # Risk Score for Plan

# COMMAND ----------

# df_general.select('risk_score').distinct().show()

# COMMAND ----------

# df_general.select('equable_class_id').count()

# COMMAND ----------

df_general=df_general.dropDuplicates(['equable_class_id'])

# COMMAND ----------

# df_general.select('equable_class_id').count()

# COMMAND ----------


from pyspark.sql.functions import expr

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


# COMMAND ----------

# df_general_mul_flat.display()

# COMMAND ----------

df_general.write.mode("overwrite") \
    .parquet(output_folder+"dim_plan_features")

df_general_mul_flat.write.mode("overwrite") \
    .parquet(output_folder+"dim_plan_multipliers")
