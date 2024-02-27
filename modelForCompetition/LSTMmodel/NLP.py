import numpy as np
import pandas as pd
import math
import jieba
import os
os.chdir("D:\学习材料\数学\数学建模与实验\美赛\美赛进行时\\2023\\2023_MCM-ICM_Problems")
#%%
file = 'Problem_C_Data_Wordle.xlsx'
df_sheet=pd.read_excel(file,sheet_name="Sheet1")
docs=df_sheet["Word"]
# 将文件每行分词，分词后的词语放入words中
words = []
for i in range(len(docs)):
    docs[i] = jieba.lcut(docs[i].strip("\n"))
    words += docs[i]

# 找出分词后不重复的词语，作为词袋
vocab = sorted(set(words), key=words.index)

# 建立一个M行V列的全0矩阵，M问文档样本数，这里是行数，V为不重复词语数，即编码维度
V = len(vocab)
M = len(docs)
onehot = np.zeros((M, V))  # 二维矩阵要使用双括号
tf = np.zeros((M, V))

for i, doc in enumerate(docs):
    for word in doc:
        if word in vocab:
            pos = vocab.index(word)
            onehot[i][pos] = 1
            tf[i][pos] += 1  # tf,统计某词语在一条样本中出现的次数

row_sum = tf.sum(axis=1)  # 行相加，得到每个样本出现的词语数
# 计算TF(t,d)
tf = tf / row_sum[:, np.newaxis]  # 分母表示各样本出现的词语数，tf为单词在样本中出现的次数，[:,np.newaxis]作用类似于行列转置
# 计算DF(t,D)，IDF
df = onehot.sum(axis=0)  # 列相加，表示有多少样本包含词袋某词
idf = list(map(lambda x: math.log10((M + 1) / (x + 1)), df))

# 计算TFIDF
tfidf = tf * np.array(idf)
tfidf = pd.DataFrame(tfidf, columns=vocab)
