# staging.py
"""Ingests raw source data and normalizes syntax — nulls, dtypes, and formats for downstream processing."""

import pandas as pd
import numpy as np
from src.utils.load import load, dump

import logging
logger = logging.getLogger(__name__)

import warnings
warnings.filterwarnings("ignore")

def build_staging(raw_data_path = "data/raw/incident_event_log.csv", 
                  snapshots_path = "data/staging/snapshots.parquet", 
                  return_staged: bool = False
                  ):
    """Transform raw CSV into staged snapshots table."""
    
    logger.info("building staging snapshots...")
    schema = load("configs/schema.yaml")
    schema_col = schema["raw_to_canonical"]

    df = load(raw_data_path)
    # "?" Unknown information with NaN
    df = df.replace(schema.get("missing_values", {}).get("token", "?"), np.nan)

    # Rename columns
    df = df.rename(columns=schema_col)

    # Parse datetimes (day-first)
    dayfirst = schema.get("parsing", {}).get("timestamp_day_first", True)
    for c in schema.get("timestamp_cols"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=dayfirst)

    # Clean magic values in case status
    status_col_name = schema_col.get("incident_state", "")
    if status_col_name in df.columns:
        df[status_col_name] = df[status_col_name].replace({"-100": "Unknown", 
                                                           -100: "Unknown"})

    # Clean magic values in vendor id
    vendor_col_name = schema_col.get("vendor", "")
    if vendor_col_name in df.columns:
        df[vendor_col_name] = df[vendor_col_name].replace({"code 8s": "Vendor Code8s"})
        
    # notify email to boolean 
    notify_email_col = schema_col.get("notify", "")
    if notify_email_col in df.columns:
        df[notify_email_col] = (df[notify_email_col] == 'Send Email').astype("boolean")

    # Save
    dump(df, snapshots_path)
    
    if return_staged:
        return df
    return snapshots_path

if __name__ == "__main__":
    #main()
    pass