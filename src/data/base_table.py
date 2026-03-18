import pandas as pd
import numpy as np
from src.utils.load import load, dump
from src.utils.data import tag_feature_map, add_suffix, valid_cols
from typing import Dict, List, Any, Tuple

def build_flag(data: pd.Series | pd.DataFrame, missing_flags: bool = True) -> pd.Series | pd.DataFrame:
    """Create data missing/presence flags of pandas series or dataframe"""
    if missing_flags:
        return data.isna().astype(int)
    return data.notna().astype(int)

# features which have changes due to data migraton/sourcing
def make_constant(df, cols: str | list, method = 'first') -> pd.Series:
    """Create events series with case_level constant value"""
    if method == 'mode':
        return df.groupby('case_id')[cols].transform(lambda x: x.mode().iloc[0])
    return df.groupby('case_id')[cols].transform(method)

def drop_missing(df, features):
    """Drop cases (rows) where field missing"""
    missing_table = df.isna().groupby(df['case_id']).all()
    cases_to_drop = missing_table[features].any(axis=1)
    drop_ids = cases_to_drop[cases_to_drop].index
    
    return df[~df['case_id'].isin(drop_ids)]
    

def fill_missing(df, cols: list | str, keyword: str = "Unknown"):
    """Fills the nan columns with `Unknown` (default) keyword"""
    if type(cols) is str:
        return df[[cols]].fillna("Unknown")
    return df[cols].fillna("Unknown")

def apply_mappings(df: pd.DataFrame, cols: List[str], profile: Dict[str, Any]) -> pd.DataFrame:
    """Apply ordinal mappings from profile to given columns, adds encoded cols to df."""
    result = pd.DataFrame()
    for col in cols:
        mapping = profile["columns"][col].get("mapping")
        if mapping:
            result[col] = df[col].map(mapping)
    return result

def build_base_table(events_path: str = r'data\canonical\events.parquet', 
                     mbt_path: str = r'data\base_table\mbt.parquet'):
    """Create feature engineering ready base table, feedable to model

    Args:
        path (str): Relative path from project root of canonical events data 

    Returns:
        pd.DataFrame: data table ready to perform feature encoding for modeling
    """
    df_raw = load(events_path)
    profile = load('configs/feature_profile.yaml')
    tags = tag_feature_map(profile)
    
    # ----------------- preserve outcome variable from imputation ---------------- #
    guardrail_cols = valid_cols(tags['guardrail'], df_raw)
    df = df_raw.drop(guardrail_cols, axis=1)
    outcome_df = df_raw[guardrail_cols]
    
    # ----------------- ref. notebook section [Inspect uid cols] ----------------- #
    grouped = df['assigned_team_gid'].groupby([df['case_id'], df['reassignment_count']])
    df['assigned_team_gid'] = grouped.transform('ffill').fillna(grouped.transform('bfill'))
    
    # ------------------- highly-missing feature (99% missing) ------------------- #
    sparse_cols = valid_cols(tags['sparse'], df)
    df[add_suffix(sparse_cols, "pflag", trim=2)] = build_flag(df[sparse_cols], 
                                                          missing_flags=False)
    df.drop(sparse_cols, axis=1, inplace=True)
    
    # ------------------ feature missing for few (<0.01%) cases ------------------ #
    minor_missing_cols = valid_cols(tags['minor_missing'], df)
    df = drop_missing(df, minor_missing_cols)
    
    # ------------------ features missing for major (<5%) cases ------------------ #
    major_null_cols = valid_cols(tags['major_null'], df)
    
    cols_for_ffill = list(set(major_null_cols) - set(tags['uid'])) # removed uid columns
    cols_for_ffill = valid_cols(cols_for_ffill, df)
    
    # ------------------- avoiding ffill and mising flag on uid ------------------ #
    # -------------- if they're null means its unknown or unassigned ------------- #
    df[add_suffix(cols_for_ffill, "_mflag")] = build_flag(df[cols_for_ffill], 
                                                          missing_flags=True) # capture original missingness
    df[cols_for_ffill] = make_constant(df, cols_for_ffill, 'ffill')
    
    df[major_null_cols] = fill_missing(df, major_null_cols)
    
    # ---------------------- features missing for <1% cases ---------------------- #
    few_null_cols = valid_cols(tags['1pct_null'], df)
    df[few_null_cols] = fill_missing(df, few_null_cols)
    
    # --------- features <90% constant -> create constant proxy features --------- #
    minor_changing_cols = valid_cols(tags['minor_change'], df) # removed flag as it capture original data
    cproxy_cols = add_suffix(minor_changing_cols, "_cproxy") #constant proxy cols
    df[cproxy_cols] = make_constant(df, minor_changing_cols, "first") # only first can avoid data leakage
    
    # ------------------------- ordinal feature encoding ------------------------- #
    oridinal_cols = valid_cols(tags['ordinal'], df)
    df[add_suffix(oridinal_cols, "encoded", trim=5)] = apply_mappings(df, oridinal_cols, profile)
    df = df.drop(oridinal_cols, axis=1)
    
    # -------------------------- bool feature to binary -------------------------- #
    bool_cols = valid_cols(tags['boolean'], df)
    df[bool_cols] = df[bool_cols].astype(int) # True: 1, False: 0
    
    
    df_combine = df.merge(outcome_df, left_index=True, right_index=True, how="left")
    
    print(f"saved modeling base table: {mbt_path}")
    return df_combine