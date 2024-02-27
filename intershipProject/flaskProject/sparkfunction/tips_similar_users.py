from pyspark.sql import SparkSession
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
from scipy.linalg import norm

'''
寻找两用户之间的评论相似性，并以此来为用户推荐知音，输入tips_df,user_id
'''

'''
余弦相似度的计算
'''
def tf_similarity(s1, s2):
    cv = CountVectorizer(tokenizer=lambda s: s.split())  # 转化为TF矩阵
    corpus = [s1, s2]
    vectors = cv.fit_transform(corpus).toarray()  # 计算TF系数
    return np.dot(vectors[0], vectors[1]) / (norm(vectors[0]) * norm(vectors[1]))


# 开启一个会话
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('tips_analysis') \
    .getOrCreate()
df = spark.read.json('yelp_academic_dataset_tip.json')


def similar_tips(tips_df, user_id):
    similar_list = []
    tips_df = tips_df.select('user_id', 'text')
    tips_pd_df = tips_df.toPandas()
    user_all_tips = tips_pd_df.groupby('user_id').agg({'text': ''.join}).reset_index()
    user_all_tips.to_csv('user_all_tips.csv',index=False)

    # print(user_all_tips.loc[user_all_tips['user_id'] == user_id, 'text'])


    # ind = user_all_tips.loc[user_all_tips['user_id'] == user_id].index[0]
    # user_all_tips['text'] = user_all_tips['text'].apply(lambda x: x.replace(",", " ", 10000000))
    # user_all_tips['text'] = user_all_tips['text'].apply(lambda x: x.replace("&", " ", 10000000))
    # user_all_tips['text'] = user_all_tips['text'].apply(lambda x: x.replace(".", " ", 10000000))
    # user_all_tips['text'] = user_all_tips['text'].apply(lambda x: x.replace(")", " ", 10000000))
    # user_all_tips['text'] = user_all_tips['text'].apply(lambda x: x.replace("(", " ", 10000000))
    # # user_all_tips['text'] = user_all_tips['text'].apply(lambda x: x.replace("!", " ", 10000000))

    # result_text = result_text.replace(",", " ", 10000000)
    # result_text = result_text.replace("&", " ", 10000000)
    # result_text = result_text.replace(".", " ", 10000000)
    # result_text = result_text.replace("!", " ", 10000000)
    # result_text = result_text.replace("'s", " ", 10000000)
    # result_text = result_text.replace("n't", " ", 10000000)
    # result_text = result_text.replace("(", " ", 10000000)
    # result_text = result_text.replace(")", " ", 10000000)
    # for index, row in user_all_tips.iterrows():
    #     similar_list.append(tf_similarity(user_all_tips.loc[ind, 'text'], row['text']))
    # user_all_tips['tf_similarity'] = similar_list
    # user_all_tips=user_all_tips.sort_values(['tf_similarity'], ascending=False)
    # user_all_tips.drop('text', axis=1, inplace=True)
    # user_top_similar = user_all_tips[1:21]
    # values = user_top_similar.values.tolist()
    # columns = user_top_similar.columns.tolist()
    #
    # # print(user_all_tips)
    # user_similar_recommend=spark.createDataFrame(values,columns)
    # user_similar_recommend.withColumnRenamed('tf_similarity','friend may sharing interest')
    # return user_similar_recommend

# df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]})

# print(df.loc[df['A'] == 1, 'B'])
similar_tips(df, 'VL12EhEdT4OWqGq0nIqkzw')
# friends_recommend = similar_tips(df, 'VL12EhEdT4OWqGq0nIqkzw')
# friends_recommend.show()



# print(user_all_tips.head(20))
# print(len(user_all_tips))

# print(similar_list)
# print(tf_similarity(user_all_tips.loc[1,'text'],user_all_tips.loc[1,'text']))
# df = pd.DataFrame({'A': [1, 2, 3, 4], 'B': ['a', 'b', 'c', 'd']})
# index = df.loc[df['A'] == 3].index[0]
# result = df.loc[index, 'B']
# print(result)
