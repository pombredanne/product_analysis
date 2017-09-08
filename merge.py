import pandas as pd

file1 = pd.read_csv("/Users/apple/Desktop/Sophia/week_data2/all_0828_week.csv", index_col=0)
file2 = pd.read_csv("/Users/apple/Desktop/Sophia/week_data2/raw_data_170907.csv", index_col=0)

print(len(file1))
print(len(file2))

file = pd.concat([file1, file2]).fillna(0)
print(len(file))
# file = file.loc[:, ~file.columns.str.contains('^Unnamed')]

file.to_csv("./week_data2/all_0904_week.csv")