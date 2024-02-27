from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, BooleanType, FloatType
from pyspark.sql.functions import col, expr, concat, countDistinct, to_timestamp, year, count, month, weekofyear, avg, \
    monotonically_increasing_id, \
    row_number, explode, split, date_format
import matplotlib.pyplot as plt
import pandas as pd

'''
对商家进行打卡分析，绘制每月柱状图和累积柱状图
'''
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('checkin-analysis') \
    .getOrCreate()
checkin_schema = StructType([StructField("business_id", StringType(), True),
                             StructField("date", StringType(), True)])
df = spark.read.schema(checkin_schema).json('yelp_academic_dataset_checkin.json')
# df.show(truncate=False)
# dfWithIndex=df.withColumn('id',monotonically_increasing_id())

# pear_df=dfWithIndex.head(3)
# pear_df.show()
# dfWithIndex=df.withColumn('id')




# slice_df_clean.repartition(1).write.json('checkin_clean.json')

#


# slice_df_clean.printSchema()
def checkin_inmonth(checkin_df):
    # 将多date转换成一个date对一个商家
    slice_df_clean = checkin_df.select('business_id', explode(split(col("date"), ',').alias('date_clean')))
    slice_df_clean = slice_df_clean.withColumnRenamed('col', 'date_clean')
    # 将date转换成年月日的时间戳
    slice_df_clean = slice_df_clean.withColumn('date_timestamp', to_timestamp(col('date_clean'), "yyyy-MM-dd HH:mm:ss"))
    checkin_count = slice_df_clean.select('business_id', date_format('date_timestamp', 'yyyy-MM').alias('year-month'),
                                          'date_timestamp') \
        .groupby('business_id', 'year-month') \
        .agg(count('date_timestamp').alias('checkin_number'))
    return checkin_count

checkin = checkin_inmonth(df)
checkin.show()


# 画折线图
# plt.plot(checkin_month['year-month'],checkin_month['checkin_number'])
# plt.xticks(rotation=60)
# plt.show()
# fig = plt.figure(figsize = (15,8))
# plt.bar(checkin_month['year-month'],checkin_month['checkin_number'],width = 1.0,color='m')
# plt.xticks(rotation=90)
# fig.show()
# checkin_month.insert(checkin_month.shape[1],'accumulated_checkin',0)
#
# checkin_month['accumulated_checkin']=checkin_month['checkin_number'].cumsum()
# print(checkin_month)
# 将打卡数据的时间戳按升序排序



# 画每年累积图
def checkin_time_plot(checkin_count_df,start_time, end_time,business_id):
    # 将dataframe转换成panda的dataframe
    checkin_month = checkin_count_df.sort_values('year-month')
    pdf_checkin = checkin_month.toPandas()
    pdf_checkin['year-month'] = pd.to_datetime(pdf_checkin['year-month'])
    checkin_month = pdf_checkin[pdf_checkin['business_id'].isin([business_id])]
    fig = plt.figure(figsize=(15, 8))
    start_time = pd.Timestamp(start_time)
    end_time = pd.Timestamp(end_time)
    checkin_month_slice = checkin_month.loc[
        (checkin_month['year-month'] >= start_time) & (checkin_month['year-month'] <= end_time)]
    # 将指定时间段进行打卡数累加统计
    # print(checkin_month_slice)
    checkin_month_slice.insert(checkin_month_slice.shape[1], 'accumulated_checkin', 0)
    checkin_month_slice['accumulated_checkin'] = checkin_month_slice['checkin_number'].cumsum()
    plt.bar(checkin_month_slice['year-month'], checkin_month_slice['accumulated_checkin'], width=10)
    plt.xticks(rotation=60)
    fig.show()


# checkin_time_plot(checkin,"2021-01-01", "2021-11-01")
