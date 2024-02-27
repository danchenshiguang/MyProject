from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, BooleanType, FloatType
from pyspark.sql.functions import col, expr, concat, countDistinct, to_timestamp, year, count,month,weekofyear,avg,monotonically_increasing_id,\
    row_number,explode,split,date_format
'''
商家查看每天的tips
'''
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('tips_look') \
    .getOrCreate()
df=spark.read.json('yelp_academic_dataset_tip.json')
# df.printSchema()

# 将date转换成年月日的时间戳
df_timestamp=df.withColumn('date_timestamp',to_timestamp(col('date'),"yyyy-MM-dd HH:mm:ss"))
df_timestamp.drop('date')
# df_timestamp.show()
def select_business(business):
    business_df=df_timestamp.where(col('business_id')==business)
    return business_df

def day_tip_look(tip_df,day):
    tip_df_show=tip_df.select('user_id',date_format('date_timestamp','yyyy-MM-dd').alias('year-month-day'),'text')\
        .where(col('year-month-day')==day)
    return tip_df_show

business_df=select_business(business="3uLgwr0qeCNMjKenHJwPGQ")
tip_df_show=day_tip_look(business_df,'2012-05-18')
tip_df_show=tip_df_show.toPandas()
tip_df_show=tip_df_show.to_dict()
print(tip_df_show['user_id'][0])
