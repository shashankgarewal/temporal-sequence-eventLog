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
        
    # ------------------------------ contact_channel ----------------------------- #
    make_constant(grp_case,'contact_channel')
    
    print(f"saved modeling base table: {mbt_path}")
    return df