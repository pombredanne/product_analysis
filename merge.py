import pandas as pd

file1 = pd.read_csv("./week_data/all_0717_week.csv", index_col=0)
file2 = pd.read_csv("./week_data/0724/raw_data_170727.csv", index_col=0)

print(len(file1))
print(len(file2))

file = pd.concat([file1, file2],)
print(len(file))
# file = file.loc[:, ~file.columns.str.contains('^Unnamed')]

file.to_csv("./week_data/all_0724_week.csv")