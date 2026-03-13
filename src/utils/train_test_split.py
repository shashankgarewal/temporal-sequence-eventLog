import pandas as pd

def time_aware_split(df, time_col, train_ratio=0.7, val_ratio=0.15):

    df = df.sort_values(time_col)

    n = len(df)

    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    train = df.iloc[:train_end]
    val   = df.iloc[train_end:val_end]
    test  = df.iloc[val_end:]

    return train, val, test