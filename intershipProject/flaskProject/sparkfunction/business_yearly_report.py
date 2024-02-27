from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, BooleanType, FloatType
from pyspark.sql.functions import col, expr, concat, countDistinct, to_timestamp, year, count, hour, weekofyear, avg, \
    monotonically_increasing_id, \
    row_number, explode, split, date_format, month, when, lit, lag, sum
from pyspark.sql import Window
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.types import IntegerType
import nltk
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat, countDistinct, to_timestamp, year, count, month, date_format
from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, BooleanType, FloatType

from nltk import FreqDist
from nltk.corpus import webtext
from wordcloud import WordCloud

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('business_yearly_report') \
    .getOrCreate()
checkin_schema = StructType([StructField("business_id", StringType(), True),
                             StructField("date", StringType(), True)])
review_DF = spark.read.json('yelp_academic_dataset_review.json')
checkin_DF = spark.read.json('yelp_academic_dataset_checkin.json')
'''
统计某商家一年内的总打卡数
'''


def checkin_inyear(checkin_df, business_id, a_year):
    # 将多date转换成一个date对一个商家
    slice_df_clean = checkin_df.select('business_id', explode(split(col("date"), ',')).alias('date_clean'))
    # 将date转换成年月日的时间戳
    slice_df_clean = slice_df_clean.withColumn('date_timestamp', to_timestamp(col('date_clean'), "yyyy-MM-dd HH:mm:ss"))
    checkin_in_a_year = slice_df_clean.select('business_id', year('date_timestamp').alias('year'), 'date_timestamp') \
        .where((col('business_id') == business_id) & (col('year') == a_year)) \
        .groupby('business_id', 'year') \
        .agg(count(col('date_timestamp')).alias('checkin_count'))
    return checkin_in_a_year


'''
统计一年中某商家收到的总评论数
'''


def review_inyear(review_df, business_id, a_year):
    review_df = review_df.withColumn('date_timestamp', to_timestamp(col('date'), "yyyy-MM-dd HH:mm:ss"))
    review_in_a_year = review_df.select('business_id', 'review_id', year('date_timestamp').alias('year')) \
        .where((col('business_id') == business_id) & (col('year') == a_year)) \
        .groupby('business_id', 'year') \
        .agg(count(col('review_id')).alias('review_number'))
    return review_in_a_year


'''
统计一年中某商家评论最多的用户和评论星数最多的用户top10
'''


def review_user_inyear(review_df, business_id, a_year):
    review_df = review_df.withColumn('date_timestamp', to_timestamp(col('date'), "yyyy-MM-dd HH:mm:ss"))
    review_most_user = review_df.select('user_id', "review_id", year('date_timestamp').alias('year')) \
        .where((col('business_id') == business_id) & (col('year') == a_year)) \
        .groupby('user_id', 'year') \
        .agg(count(col('review_Id')).alias('give_review_count')) \
        .orderBy('give_review_count', ascending=False)\
        .limit(10)
    review_stars_most_user = review_df.select('user_id', "review_id", year('date_timestamp').alias('year'), 'stars') \
        .where((col('business_id') == business_id) & (col('year') == a_year)) \
        .groupby('user_id','year') \
        .agg(sum('stars').alias('give_stars_sum')) \
        .orderBy('give_stars_sum', ascending=False)\
        .limit(10)
    return review_most_user, review_stars_most_user

'''
统计该商家今年的年度热词top10

'''

def review_keywords_inyear(review_df, business_id, a_year):
    review_df = review_df.withColumn('date_timestamp', to_timestamp(col('date'), "yyyy-MM-dd HH:mm:ss"))
    reviews = review_df.select('business_id', 'text') \
        .where((col('business_id') == business_id) & (year(col('date_timestamp')) == a_year))
    reviews = reviews.toPandas()
    all_reviews_text = reviews.groupby('business_id').agg({'text': ''.join})
    result_text = all_reviews_text.iloc[0][0]
    # print(results.iloc[0][0])
    result_text = result_text.replace(",", " ", 10000000)
    result_text = result_text.replace("&", " ", 10000000)
    result_text = result_text.replace(".", " ", 10000000)
    result_text = result_text.replace("!", " ", 10000000)
    result_text = result_text.replace("'s", " ", 10000000)
    result_text = result_text.replace("n't", " ", 10000000)
    result_text = result_text.replace("(", " ", 10000000)
    result_text = result_text.replace(")", " ", 10000000)
    # 分词
    tokens = nltk.word_tokenize(result_text)

    # 去除停用词
    stopwords = nltk.corpus.stopwords.words('english')
    filtered_tokens = [word for word in tokens if word.lower() not in stopwords]

    # 计算词频
    fdist = FreqDist(filtered_tokens)
    most_common = fdist.most_common(10)
    # print(most_common)
    key_word_yearly = []
    for i in most_common:
        key_word_yearly.append(i[0])
    return key_word_yearly


'''
统计当年与前一年的评论数、打卡量、评论平均评分变化情况，给出变化百分比
'''


def see_differ_from_last_year(review_df, checkin_df, business_id, a_year):
    last_year = int(a_year) - 1
    last_year = str(last_year)
    # 得到今年和去年的评论数量变化百分比

    review_df = review_df.withColumn('date_timestamp', to_timestamp(col('date'), "yyyy-MM-dd HH:mm:ss"))
    review_count_yearly = review_df.select('business_id', 'review_id', year('date_timestamp').alias('year')) \
        .where((col('business_id') == business_id) & (col('year').isin(a_year, last_year))) \
        .groupby('year') \
        .agg(count('review_id').alias('review_count')) \
        .orderBy('year', ascending=True)
    if review_count_yearly.count() ==2:
        review_count_this_year = review_count_yearly.where(col("year") == a_year).select("review_count").collect()[0][0]
        review_count_last_year = review_count_yearly.where(col("year") == last_year).select("review_count").collect()[0][0]
        review_count_differ = (review_count_this_year - review_count_last_year) / review_count_last_year
    elif review_count_yearly.count() == 1:
        if review_count_yearly.where(col('year')==a_year).count() == 1:
            review_count_this_year = \
            review_count_yearly.where(col("year") == a_year).select("review_count").collect()[0][0]
            review_count_differ = "the checkin_number has raised to {review_count}".format(
                review_count=review_count_this_year)
        else:
            review_count_differ ="sorry to tell you no one reviews this year"
    else:
        review_count_differ = "sorry to tell you no one reviews this year"

    # 得到今年和去年的打卡数变化百分比
    checkin_df = checkin_df.select('business_id', explode(split(col("date"), ',')).alias('date'))
    checkin_clean_df = checkin_df.withColumn('date_timestamp', to_timestamp(col('date'), "yyyy-MM-dd HH:mm:ss"))
    checkin_count_yearly = checkin_clean_df.select('business_id', year('date_timestamp').alias('year'),
                                                   'date_timestamp') \
        .where((col('business_id') == business_id) & (col('year').isin(a_year, last_year))) \
        .groupby('year') \
        .agg(count('date_timestamp').alias('checkin_count')) \
        .orderBy('year', ascending=True)
    # checkin_count_yearly.show()
    if checkin_count_yearly.count()==2:
        checkin_count_this_year = checkin_count_yearly.where(col("year") == a_year).select("checkin_count").collect()[0][0]
        checkin_count_last_year = checkin_count_yearly.where(col("year") == last_year).select("checkin_count").collect()[0][0]
        checkin_count_differ = (checkin_count_this_year - checkin_count_last_year) / checkin_count_last_year
    elif checkin_count_yearly.count()==1:
        if checkin_count_yearly.where(col('year')==a_year).count() == 1:
            checkin_count_this_year = \
            checkin_count_yearly.where(col("year") == a_year).select("checkin_count").collect()[0][0]
            checkin_count_differ = "the checkin_number has raised to {checkin_count}".format(checkin_count=checkin_count_this_year)
        else:
            checkin_count_differ= "sorry to tell you the checkin_number this year is 0"
    else:
        checkin_count_differ = "sorry to tell you the checkin_number this year is 0"
    # 得到去年和今年评分平均星级的变化
    review_stars_yearly = review_df.select('business_id', 'review_id', 'stars', year('date_timestamp').alias('year')) \
        .where((col('business_id') == business_id) & (col('year').isin(a_year, last_year))) \
        .groupby('year') \
        .agg(count('stars').alias('stars_avg')) \
        .orderBy('year', ascending=True)
    if review_stars_yearly.count()==2:
      review_stars_this_year = review_stars_yearly.where(col("year") == a_year).select("stars_avg").collect()[0][0]
      review_stars_last_year = review_stars_yearly.where(col("year") == last_year).select("stars_avg").collect()[0][0]
      review_stars_differ = (review_stars_this_year - review_stars_last_year) / review_stars_last_year
    elif review_stars_yearly.count()==1:
        if review_stars_yearly.where(col('year')==a_year).count() == 1:
            review_stars_this_year = review_stars_yearly.where(col("year") == a_year).select("stars_avg").collect()[0][
                0]
            review_stars_differ = "the checkin_number has raised to {review_stars_count}".format(
                review_stars_count=review_stars_this_year)
        else:
            review_stars_differ = "sorry to tell you no one reviews this year"
    else:
        review_stars_differ = "sorry to tell you no one reviews this year"



    return review_count_differ, checkin_count_differ, review_stars_differ

# results = review_merge_inyear(review_DF, 'gebiRewfieSdtt17PTW6Zg', '2015')

# print(most_common[0][0])

# a=see_defer_from_last_year(review_DF, checkin_DF,"--7PUidqRWpRSpXebiyxTg",'2011')
# print(a)
def business_yearly_report(checkin_df,review_df,business_id,a_year):
    checkin_this_year = checkin_inyear(checkin_df,business_id,a_year)
    review_count_this_year =review_inyear(review_df,business_id,a_year)
    review_users = review_user_inyear(review_df,business_id,a_year)
    review_keywords =review_keywords_inyear(review_df,business_id,a_year)
    differ = see_differ_from_last_year(review_df,checkin_df,business_id,a_year)
    checkin = checkin_this_year.select('checkin_count').collect()[0][0]
    review_count = review_count_this_year.select('review_number').collect()[0][0]
    review_users_most=[row['user_id'] for row in review_users[0].select('user_id').collect()]
    review_stars_most=[row['user_id'] for row in review_users[1].select('user_id').collect()]
    report_list=[{'business_id': business_id,'year': a_year,'checkin this year':checkin,'all reviews':review_count,
                  'review-number-top users':review_users_most,'review-stars-top users':review_stars_most,
                  'yearly keywords': review_keywords,'differ from last year(checkin number) ':differ[0],
                  'differ from last year(review number)':differ[1],
                  'differ from last year(average stars)':differ[2]
                  }]
    report = spark.createDataFrame(report_list)

    return report

a_report = business_yearly_report(checkin_DF,review_DF,"--7PUidqRWpRSpXebiyxTg",'2011')
a_report.show()
