import matplotlib.pyplot as plt
from pyecharts import options as opts
from pyecharts.charts import Pie
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year

'''
筛选年度精英用户的数量
'''
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('elite-user-yearly') \
    .getOrCreate()
# df = spark.read.json('yelp_academic_dataset_user.json')
df=spark.read.format('jdbc')\
    .option('url','jdbc:mysql://192.168.102.221:3306/yelp?useSSL=false')\
    .option('dbtable','business')\
    .option('user','hive')\
    .option('password','admin')\
    .load()
df.show()

def select_elite_yearly_users(user_df, year):
    elite_user_df = user_df.filter(col('elite').like(f'%{year}%'))
    year_elite_users_count = elite_user_df.select('user_id').count()
    return year_elite_users_count


# elite_user_count = select_elite_yearly_users(df, '2007')
# print(elite_user_count)

'''
计算每年精英用户占比,并画饼图
'''
# 此函数用来计算到某年为止的总用户数

def until_year_all_user(user_df, until_year):
    user_df = user_df.withColumn('date_timestamp', to_timestamp(col('yelping_since'), "yyyy-MM-dd HH:mm:ss")).drop(
        'yelping_since')
    users = user_df.select('user_id', year('date_timestamp').alias('year')) \
        .where(col('year') <= until_year) \
        .count()
    return users


# all_users_theYear = until_year_all_user(df, '2014')


# def elite_user_prop_aYear(user_df, aYear):
#     elite_users = select_elite_yearly_users(user_df, aYear)
#     all_users = until_year_all_user(user_df, aYear)
#     user_prop = elite_users / all_users
#     return user_prop
# 画普通图
def pie_plot_eliteUsers(user_df,ayear):
    all_users = until_year_all_user(user_df,ayear)
    eliter_users_theyear =select_elite_yearly_users(user_df,ayear)
    sizes = [eliter_users_theyear, all_users-eliter_users_theyear]
    labels = ['eliter users', 'common users']
    explodes=[0.1, 0]
    plt.pie(sizes, labels=labels, explode=explodes, autopct='%1.1f%%')
    plt.show()

# pie_plot_eliteUsers(df,'2014')
# 画htmlpyechart图
def pie_plot_echarts(user_df,ayear):
    all_users = until_year_all_user(user_df,ayear)
    eliter_users_theyear =select_elite_yearly_users(user_df,ayear)
    sizes = [eliter_users_theyear, all_users-eliter_users_theyear]
    labels = ['eliter users', 'common users']
    colors = [' #FF9999','#87CEFA']#'#E6E6FA', '#FFB6C1'
    pie = Pie()
    pie.add("",[list(z) for z in zip(labels,sizes)])
    pie.set_global_opts(title_opts=opts.TitleOpts(title="优质用户饼状图"))
    pie.set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {d}%"))
    pie.set_colors(colors)
    pie.render('pie.html')
# pie_plot_echarts(df,'2014')

