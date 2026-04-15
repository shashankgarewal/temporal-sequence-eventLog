import pandas as pd

def time_aware_split(df: pd.DataFrame, train_ratio=0.75, val_ratio=0.15, case_col='case_id'):
    """
    Split event log dataframe chronologically by case.
    """

    ordered_case_ids = df[case_col].unique()
    n = len(ordered_case_ids)

    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    train_ids = set(ordered_case_ids[:train_end])
    val_ids = set(ordered_case_ids[train_end:val_end])
    test_ids = set(ordered_case_ids[val_end:])

    train = df[df[case_col].isin(train_ids)].copy()
    val = df[df[case_col].isin(val_ids)].copy()
    test = df[df[case_col].isin(test_ids)].copy()

    return train, val, test