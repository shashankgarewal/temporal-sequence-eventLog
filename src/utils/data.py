import pandas as pd
from collections import defaultdict
from typing import Dict, List, Any


def tag_feature_map(profile: Dict[str, Any], key = "columns") -> Dict[str, List[str]]:
    """
        Creates a tag → columns lookup from the column schema profile.
        
        Args:
            profile: A dictionary containing a "columns" key with metadata.
            
        Returns:
            A dictionary with tag are keys and values are lists of columns.
        """
    tag_map = defaultdict(list)
    columns = profile.get(key, {})
    for col, meta in columns.items():
        for tag in meta.get("tags", []):
            tag_map[tag].append(col)
    return dict(tag_map)

def valid_cols(cols: List[str], df: pd.DataFrame) -> List[str]:
    """Remove any cols not present in dataframe"""
    return [c for c in cols if c in set(df.columns)]

def add_suffix(cols: List[str] | str, suffix: str = "", trim:int = 0) -> List[str]:
    """Trim characters from the end and appends a suffix."""
    trim = abs(trim)
    if type(cols) is str:
        if trim == 0:
            return (cols + suffix)
        else:
            return (cols[:-trim] + suffix)
    if trim == 0:
        return [f"{col}{suffix}" for col in cols]
    return [f"{col[:-trim]}{suffix}" for col in cols]

def compare_dfs(df1, df2):
    """Diagnose whether 2 dataframes are same or not"""
    if df1.shape != df2.shape:
        print("shape mismatch")
        return False
    
    if (set(df1.columns) ^ set(df2.columns)) != set():
        print("different column names")
        return False
    
    if (df1.dtypes != df2.dtypes).sum() != 0:
        print("columns dtypes mismatch")
        return False
    
    df1 = df1.sort_index().sort_index(axis=1)
    df2 = df2.sort_index().sort_index(axis=1)
    df1.compare(df2)
    return df1.equals(df2)