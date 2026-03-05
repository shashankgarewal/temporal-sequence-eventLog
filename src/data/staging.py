import pandas as pd
import numpy as np
import yaml
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAW  = ROOT / "data/raw/incident_event_log.csv"
OUT  = ROOT / "data/staging/snapshots.parquet"
SCHEMA = ROOT / "configs/schema.yaml"

def main():
    schema = yaml.safe_load(open(SCHEMA, "r", encoding="utf-8"))
    schema_col = schema["raw_to_canonical"]

    df = pd.read_csv(RAW)

    # "?" Unknown information with NaN
    df = df.replace(schema.get("missing_values", {}).get("token", "?"), np.nan)

    # Rename columns
    df = df.rename(columns=schema_col)

    # Parse datetimes (day-first)
    dayfirst = schema.get("parsing", {}).get("timestamp_day_first", True)
    for c in ["opened_timestamp", "creator_timestamp", "update_timestamp", 
              "resolved_timestamp", "closed_timestamp"]:
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
    Path(OUT).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT, index=False)
    print(f"staging saved at: {OUT}")

if __name__ == "__main__":
    main()