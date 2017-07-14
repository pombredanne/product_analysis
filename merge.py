import pandas as pd

file1 = pd.read_csv("./DB/all.csv", index_col=0)
file2 = pd.read_csv("./0710/raw_data_170710.csv", index_col=0)

print(len(file1))
print(len(file2))

file = pd.concat([file1, file2], ignore_index=True)
print(len(file))
file = file.loc[:, ~file.columns.str.contains('^Unnamed')]

file.to_csv("./DB/all_0710.csv")