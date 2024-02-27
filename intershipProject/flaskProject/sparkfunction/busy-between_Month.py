from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, BooleanType, FloatType
from pyspark.sql.functions import col, expr, concat, countDistinct, to_timestamp, year, count, hour, weekofyear, avg, \
    monotonically_increasing_id, \
    row_number, explode, split, date_format, month, when, lit, lag
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.types import IntegerType
'''
计算每月打卡同比和环比，输入checkin_clean_df,int类型的年，business_id,和y_or_m=0,1。0表示同比，1表示环比
有打卡的月才会显示。
'''
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('checkin-analysis') \
    .getOrCreate()

df = spark.read.json('checkin_clean.json/checkin_clean.json')


# 构建包含所有月份的dataframe

# 添加year列，这里以2022年为例

# all_months.show()
'''
注意由于缺少打卡数的月份太多，而且以月来计打卡数实在太少，注释里有分上半年和下半年来计算同环比
的备用方案，需要的时候可以更换
'''

# 计算每月的环比比
# 注意这里输入的year参数要是int类型
def comparative_growth_y_or_m(checkin_clean_df, a_year, business_id, y_or_m):
    # 创建一个两年时间的dataframe
    all_months = spark.range(1, 13).withColumnRenamed("id", "month")
    all_months = all_months.withColumn("month", all_months.month.cast(IntegerType()))
    all_months = all_months.withColumn("year", year(lit(str(a_year))))
    all_months_2 = spark.range(1, 13).withColumnRenamed("id", "month")
    all_months_2 = all_months_2.withColumn("month", all_months_2.month.cast(IntegerType()))
    all_months_2 = all_months_2.withColumn("year", year(lit(str(a_year - 1))))
    new_months = all_months.union(all_months_2)

    checkin_df = checkin_clean_df.withColumnRenamed('col', 'checkin_date')
    # 将字符串时间转换成时间戳
    checkin_df = checkin_df.withColumn("date_timestamp", to_timestamp(col("checkin_date"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("checkin_date")
    # 讲年划分为上半年和下半年
    # checkin_df = checkin_df.withColumn('year', year(col('date_timestamp'))) \
    #     .withColumn('month', month(col('date_timestamp')))
    # checkin_df = checkin_df.withColumn('half_year', when(checkin_df.month < 7, 'half_year_1').otherwise('half_year_2'))
    # checkin_df = checkin_df.select('business_id', 'year', 'half_year', 'date_timestamp') \
    #     .where(col('business_id') == business_id) \
    #     .groupby('year', 'half_year') \
    #     .agg(count(col('date_timestamp')).alias('checkin_count'))
    # if business_id == None:
    checkin_df = checkin_df.select('business_id', year('date_timestamp').alias('year'),
                                   month(col('date_timestamp')).alias('month'), 'date_timestamp') \
        .where((col('year').isin(str(a_year), str(a_year - 1))) & (col('business_id') == business_id)) \
        .groupby('business_id', 'year', 'month') \
        .agg(count('date_timestamp').alias('checkin_number'))
    checkin_df = new_months.join(checkin_df, on=['year', 'month'], how='left')
    checkin_df = checkin_df.fillna(1000, subset=['checkin_number'])
    if y_or_m == 0:
        checkin_df = checkin_df.withColumn('prev_year_value', lag('checkin_number', 12)
                                           .over(Window.partitionBy("month") \
                                                 .orderBy("year")))
        checkin_df = checkin_df.withColumn("YoY_checkin_ratio",
                                           (col("checkin_number") - col("prev_year_value")) / col("prev_year_value"))
    else:
        checkin_df = checkin_df.withColumn('prev_month_value', lag('checkin_number', 1)
                                           .over(Window.partitionBy("year") \
                                                 .orderBy("month")))
        checkin_df = checkin_df.withColumn("MoM_checkin_ratio",
                                           (col("checkin_number") - col("prev_month_value")) / col("prev_month_value"))
        checkin_df = checkin_df.where(col('prev_month_value') != 1000)

    checkin_df = checkin_df.where(col('business_id').isNotNull())
    # checkin_df.show()
    # else:
    #     checkin_df = checkin_df.select('business_id', year('date_timestamp').alias('year'),
    #                                    month(col('date_timestamp')).alias('month'), 'date_timestamp') \
    #         .where((col('business_id') == business_id) & (col('year') == a_year)) \
    #         .groupby('business_id', 'year', 'month') \
    #         .agg(count('date_timestamp').alias('checkin_number'))

    checkin_df.show()


comparative_growth_y_or_m(df, 2014, "--7jw19RH9JKXgFohspgQw", 1)
