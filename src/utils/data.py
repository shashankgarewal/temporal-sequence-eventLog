import pandas as pd
from collections import defaultdict
from typing import Dict, List, Any, Tuple


def tag_feature_map(profile: Dict[str, Any]) -> Dict[str, List[str]]:
    """
        Creates an inverted index mapping tag names to lists of column names.
        
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

def add_suffix(cols: List[str], suffix: str = "") -> List[str]:
    """Appends a suffix to a list of column names."""
    return [f"{col}{suffix}" for col in cols]