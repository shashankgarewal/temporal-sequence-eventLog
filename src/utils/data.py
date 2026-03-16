import pandas as pd
from collections import defaultdict
from typing import Dict, List, Any, Tuple


def tag_feature_map(profile: Dict[str, Any]) -> Dict[str, List[str]]:
    """
        Creates a tag → columns lookup from the column schema profile.
        
        Args:
            profile: A dictionary containing a "columns" key with metadata.
            
        Returns:
            A dictionary with tag are keys and values are lists of columns.
        """
    tag_map = defaultdict(list)
    columns = profile.get("columns", {})
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
