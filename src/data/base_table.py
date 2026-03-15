import pandas as pd
import numpy as np
from src.utils.load import load

def build_flag(series: pd.Series, missing_flags: bool = True) -> pd.Series:
    """Create data missing/presence flag of pandas series

    Args:
        series (pd.Series): input pandas Series to evaluate.
        missing_flags (bool):   True (Defaults) - returns 1 for NaN values.
                                False           - reutrns 1 for non-NaN values.

    Returns:
        pd.Series: pandas series with 0/1 int flag
    """
    if missing_flags:
        return series.isna().astype(int)
    return series.notna().astype(int)

# features which have changes due to data migraton/sourcing
def make_constant(df: pd.DataFrame, grp_case, col: str) -> pd.Series:
    """
    grp_case: df.groupby("case_id")
    col: column with case_level changing values 
    returns: events series with case_level constant value
    """
    return grp_case[col].transform('first')

def drop_few_missing(df, threshold:int = 10):
    """function to drop cases where field missing"""
    missing_table = df.isna().groupby(df['case_id']).all()
    missing_count = missing_table.sum(axis=0)
    feature_names = missing_count[(missing_count > 0) & (missing_count < threshold)].index.tolist()
    
    cases_to_drop = missing_table[feature_names].any(axis=1)
    drop_ids = cases_to_drop[cases_to_drop].index
    
    return df[~df['case_id'].isin(drop_ids)]
    

def fill_some_unknown(df):
    isna_record = df.isna().sum()
    feature_names = isna_record[isna_record > 0].index

    low_cardinality_map = (df[feature_names].nunique() < 0.01 * df.case_id.nunique()).to_dict()
    
    for feature, low_flag in low_cardinality_map.items():
        if low_flag:
            df[feature] = df[feature].fillna("Unknown")
        else:
            print(f'{feature},', end=" ")
            # freq = df[feature].value_counts()
            # df[f'{feature}_freq'] = df[feature].map(freq)
    return df

def build_base_table(events_path: str = r'data\canonical\events.parquet', 
                     mbt_path: str = r'data\base_table\mbt.parquet'):
    """function to create modeling ready base table

    Args:
        path (str): Relative path from project root of canonical events data 

    Returns:
        pd.DataFrame: data table ready to perform feature encoding for modeling
    """
    df = load(events_path)
    grp_case = df.groupby('case_id')
    profile = load('configs/feature_profile.yaml')
    
    for feature in profile.get('sparse'):
        df[f'{feature[:-2]}missing_flag']= build_flag(df[feature])
        
    df['contact_channel'] = make_constant(grp_case,'contact_channel')
    df['reported_symptom_missing_flag'] = build_flag(df['reported_symptom'], missing_flags=False) # capture original missingness
    df['reported_symptom'] = grp_case['reported_symptom'].transform('ffill')
    df['reported_symptom'] = df['reported_symptom'].fillna('Unknown')
    
    
    df = drop_few_missing(df)
    df = fill_some_unknown(df)
    
    print(f"saved modeling base table: {mbt_path}")
    return df