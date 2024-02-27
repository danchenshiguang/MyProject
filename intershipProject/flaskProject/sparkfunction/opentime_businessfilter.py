from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, BooleanType, FloatType
from pyspark.sql.functions import col, expr, concat, countDistinct, to_timestamp, year, count, month, weekofyear, avg, \
    monotonically_increasing_id, \
    row_number, explode, split, date_format
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('checkin-analysis') \
    .getOrCreate()
df = spark.read.json('yelp_academic_dataset_business.json')


'''
根据开门时间筛选店铺
'''

def open_day_df(business_df,day,target_hour,target_minutes):
    open_df = business_df.where(col('is_open') == 1)
    open_day_df = open_df.filter(col('hours').getItem(day).isNotNull())
    open_time_df=open_day_df.withColumn("start_hour", split(col('hours').getItem(day), ":")[0].cast("int")) \
       .withColumn("start_minute", split(col('hours').getItem(day), ":")[1].substr(0, 2).cast("int")) \
       .withColumn("end_hour", split(col('hours').getItem(day), "-")[1].substr(0, 2).cast("int")) \
       .withColumn("end_minute", split(col('hours').getItem(day), "-")[1].substr(3, 2).cast("int"))
    result = open_time_df.filter((col("start_hour") < col("end_hour")) |
                       ((col("start_hour") == col("end_hour")) & (col("start_minute") <= col("end_minute"))))
    result = result.filter((col("start_hour") < target_hour) |
                           ((col("start_hour") == target_hour) & (col("start_minute") <= target_minutes)))
    result = result.filter((col("end_hour") > target_hour) |
                           ((col("end_hour") == target_hour) & (col("end_minute") >= target_minutes)))
    return result

open_time_df = open_day_df(df,"Monday",8,30)

open_time_df.show()

