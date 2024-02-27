from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, count, date_format
from pyspark.sql.types import StructType, StringType, StructField, TimestampType

'''
商家查看每天的打卡数，输入checkin_clean_df,商家id和'yyyy-mm-dd'形式的时间字符串
'''
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('checkin_day') \
    .getOrCreate()
checkin_schema = StructType([StructField("business_id", StringType(), True),
                             StructField("col", TimestampType(), True)])
df = spark.read.schema(checkin_schema).json('checkin_clean.json/checkin_clean.json')


# 对应商家检索
# def select_business(business):
#     business_df = df_timestamp.where(col('business_id') == business)
#     return business_df


# 查找某天是否有打卡的人
def checkin_day_look(checkin_clean_df, business_id, day):
    df_timestamp = checkin_clean_df.withColumn('date_timestamp', to_timestamp(col('col'), "yyyy-MM-dd HH:mm:ss")).drop(
        'col')
    business_df = df_timestamp.where(col('business_id') == business_id)
    checkin_look_df = business_df.select(date_format('date_timestamp', 'yyyy-MM-dd').alias('year-month-day'),
                                         'date_timestamp') \
        .where(col('year-month-day') == day) \
        .groupby('year-month-day') \
        .agg(count('date_timestamp').alias('checkin_count'))
    return checkin_look_df


checkin_look_df = checkin_day_look(df, "--MbOh2O1pATkXa7xbU6LA", '2013-11-16')
# checkin_look_df.show()
# 输出对应天的打卡数量
#     checkin_count=checkin_look_df.select('checkin_count').where(col('year-month-day')=='2013-11-16').collect()[0][0]
#     print(checkin_count)
