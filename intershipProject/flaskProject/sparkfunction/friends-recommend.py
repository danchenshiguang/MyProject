from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split
from pyspark.sql.types import StructType, StringType, StructField

'''
统计一个用户和其他非好友用户的共同好友数，以进行好友推荐
输入user_df和该用户的id
得到和其他非好友用户的共同好友列表
'''
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('new-user') \
    .getOrCreate()
df = spark.read.json('user_part.json/part-00000-2f166d9b-00fb-4284-885b-3c2d55bbdaf8-c000.json')
# 用于测试共同好友能否统计出来
data = [("Alice", "Bob"), ("Alice", "c"), ("Alice", "e"), ("Bob", "Charlie"), ("c", "Charlie")]
schema = StructType([StructField("user_id", StringType(), True), StructField("friends", StringType(), True)])
test_df = spark.createDataFrame(data, schema)



def friends_recommdation(user_df, user_id):
    user_df = user_df.select('user_id', explode(split(col('friends'), ',')).alias('friend'))
    # a=user_df.where((col('user_id')=='MC1ddgzCu9styPdvxHapnw')& (col('friends')=='Gkale2UeU8R15cj3MW31kQ')).count()
    # print(a)
    user_df_2 = user_df.withColumnRenamed('user_id', 'user_id_2') \
        .withColumnRenamed('friend', 'friend_2')
    shared_df = user_df.join(user_df_2, user_df['friend'] == user_df_2['user_id_2']).select('user_id', 'friend_2')
    # shared_df.show(truncate=False)
    shared_df = shared_df.groupby('user_id', 'friend_2') \
        .count() \
        .orderBy('count', ascending=False)
    shared_df = shared_df.where(col('user_id') == user_id)
    shared_df = shared_df.withColumnRenamed('friend_2', 'friend')
    already_in_friendlist = user_df.where(col('user_id') == user_id)
    shared_df = shared_df.join(already_in_friendlist, "friend", "leftanti")
    return shared_df


# friends_recommdation(df)



share_df = friends_recommdation(test_df, 'Alice')
share_df.show()



# user_df_2.show()
# shared_friends_count=user_df.select('friends').where(col('user_id') == '9pfR0kaXb08H7K8zxNAGYQ') \
#     .join(user_df.select('friends')
#           .where(col('user_id') == '3dFm9mbiGzuQquF9uPVDLw'),'friends','inner').count()
# all_users_df=all_users.toPandas()
# all_users_df['shared_friends']=0

# for index,row in enumerate(all_users.select('user_id').collect()):
#     shared_friends_count=user_df.select('friends').where(col('user_id') == '9pfR0kaXb08H7K8zxNAGYQ') \
#         .join(user_df.select('friends')
#               .where(col('user_id') == row['user_id']),'friends','inner').count()
#     # all_users_df_index=all_users_df.index[all_users_df['user_id']==row['user_id']]
#     all_users_df.loc[]
# def friends_recommendation(user_df, user):
#     user_df = user_df.select('user_id', explode(split(col('friends').alias('friends'),',')))
#     user_df.show()
