from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, count, hour

'''
繁忙时间段分析(打卡）
输入explode后的checkin_clean_df,business_id
获得一个商家各小时的打卡数排名
'''
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('checkin-analysis') \
    .getOrCreate()
df = spark.read.json('checkin_clean.json/checkin_clean.json')


def busy_time_get(checkin_clean_df, business_id):
    checkin_df = checkin_clean_df.withColumnRenamed('col', 'checkin_date')
    # 将字符串时间转换成时间戳
    checkin_df = checkin_df.withColumn("date_timestamp", to_timestamp(col("checkin_date"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("checkin_date")
    checkin_df = checkin_df.select('business_id', hour(col('date_timestamp')).alias('hour')) \
        .where(col('business_id') == business_id) \
        .groupby('hour') \
        .agg(count(col('hour')).alias('checkin_hour_count')) \
        .orderBy(col('checkin_hour_count'), ascending=False)\
        .show()


busy_time_get(df, "---kPU91CF4Lq2-WlRu9Lw")
