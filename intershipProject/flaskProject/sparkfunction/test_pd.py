import pandas as pd
import itertools
def combine_values(x):
    return ','.join(sorted(x))
df = pd.DataFrame({'A': [1, 2, 3, 4], 'B': ['a!', 'b!', 'c!', 'd!']})

# 对列 B 中的值进行两两成对遍历，并将结果添加为新列 C
# df['C'] = df['B'].apply(lambda x: list(itertools.combinations(x.split(','), 2)))
# for row in df['B']:
df['B']=df['B'].apply(lambda x: x.replace("!"," ",10000000))

print(df)
# 对新列 C 中的每个元素应用 combine_values 函数
