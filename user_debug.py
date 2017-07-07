import pandas as pd



file1 = "0626/raw_data_0626v2.csv"
file2 = "0702/raw_data_0702v2.csv"


df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)


week1 = df1[df1["week_iso"] == 1]["user_id"]
week2 = df1[df1["week_iso"] == 1]["user_id"]

non_overlap = list(set(week1) & set(week2))


print(len(non_overlap))
print(non_overlap)



