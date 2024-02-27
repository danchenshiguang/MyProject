import numpy as np
import pandas as pd
import os

os.chdir("D:\学习材料\数学\数学建模与实验\美赛\美赛进行时\\2023\\2023_MCM-ICM_Problems")
df = pd.read_excel('c_test.xlsx')
try_feature_list = {'ave': [], 'standard': []}
for i in range(8, 12):
    try_feature_list['ave'].append(np.mean(df.iloc[:, i]))
    try_feature_list['standard'].append(np.std(df.iloc[:, i]))
random_nums=[]
def generate_normal(mean, std, size):
    # 生成标准正态分布随机数
    x=[]
    while len(x)< 150:
        y= np.random.normal(mean,std,1)
        if y>0:
            x.append(y)
    return x
for i in range(0,4):
        random_nums.append(generate_normal(try_feature_list['ave'][i], std=try_feature_list['standard'][i],size=150))
random_list=pd.DataFrame(random_nums).T
random_list.to_excel('kk.xlsx')