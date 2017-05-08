import pandas as pd


if __name__ == "__main__":

    cohorts = pd.read_csv("./metrics_segementation.csv", low_memory=False)

    print(cohorts)