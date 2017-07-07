import pandas as pd



file1 = pd.read_csv("./0702v2/raw_data_v2170706.csv")
file2 = pd.read_csv("./0702v2/raw_data_170706.csv")


file = pd.concat([file1, file2], ignore_index=True)

file.to_csv("./0702v2/all.csv")