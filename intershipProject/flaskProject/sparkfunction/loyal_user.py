from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, row_number
from pyspark.sql import Window

'''
查找忠实用户
输入评论表和business_id
得到user_id和对应的忠实star
'''
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('loyal-user') \
    .getOrCreate()
df = spark.read.json('yelp_academic_dataset_review.json')


# df=spark.read.format('jdbc')\
#     .option('url','jdbc:mysql://192.168.102.221:3306/yelp?useSSL=false')\
#     .option('dbtable','review')\
#     .option('user','hive')\
#     .option('password','admin')\
#     .load()
# df.show()
def loyal_user_find(review_df, business):
    loyal_user_df = review_df.select('user_id', 'stars') \
        .where(col('business_id') == business) \
        .groupby('user_id') \
        .agg(count('stars').alias('stars_count'), avg('stars').alias('stars_avg'))
    # 对count和avg两列进行加权平均，得到忠实用户的综合star
    loyal_user_df = loyal_user_df.withColumn('loyal_star',
                                             col('stars_count') * 0.2 + col('stars_avg') * 0.8) \
        .orderBy('loyal_star', ascending=False)
    # loyal_user_df.show()
    # loyal_user_df_avg=review_df.select('user_id', 'stars')\
    # .where(col('business_id') == business)\
    # .groupby('user_id')\
    # .agg(avg('stars').alias('stars_avg'))\
    # .orderBy(col('stars_avg').desc())\
    # .limit(30)
    # loyal_user_df_avg=loyal_user_df_avg.select('user_id')
    # loyal_user_df_count=loyal_user_df_count.select(('user_id'))
    # loyal_user_df =loyal_user_df_count.join(loyal_user_df_avg, 'user_id', 'inner')
    return loyal_user_df


# loyal_user = loyal_user_find(df, "XQfwVwDr-v0ZS3_CbbE5Xw")
# loyal_users.show()
def loyal_user_find_re(review_df):
    loyal_user_df = review_df.select('business_id', 'user_id', 'stars') \
        .groupby('business_id', 'user_id') \
        .agg(count('stars').alias('stars_count'), avg('stars').alias('stars_avg'))
    # 对count和avg两列进行加权平均，得到忠实用户的综合star
    loyal_user_df = loyal_user_df.withColumn('loyal_star',
                                             col('stars_count') * 0.2 + col('stars_avg') * 0.8)
    window = Window.partitionBy('business_id').orderBy(desc('loyal_star'))
    # loyal_user_df = loyal_user_df.select('business_id','user_id','loyal_star')\
    # .over(window)
    loyal_user_df = loyal_user_df.withColumn('row_num', row_number().over(window)) \
        .drop('stars_count')
    loyal_user_df = loyal_user_df.drop('stars_avg')
    loyal_user_df = loyal_user_df.where(loyal_user_df.row_num < 6)
    loyal_user_df = loyal_user_df.toPandas()
    loyal_user_df.to_csv('loyal_user_all_business.csv', index=False)
    # loyal_user_df.show()
    # loyal_user_df.show()
    # loyal_user_df_avg=review_df.select('user_id', 'stars')\
    # .where(col('business_id') == business)\
    # .groupby('user_id')\
    # .agg(avg('stars').alias('stars_avg'))\
    # .orderBy(col('stars_avg').desc())\
    # .limit(30)
    # loyal_user_df_avg=loyal_user_df_avg.select('user_id')
    # loyal_user_df_count=loyal_user_df_count.select(('user_id'))
    # loyal_user_df =loyal_user_df_count.join(loyal_user_df_avg, 'user_id', 'inner')
    # return loyal_user_df


loyal_user_find_re(df)
