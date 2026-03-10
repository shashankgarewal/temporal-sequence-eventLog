import pandas as pd
import numpy as np
import yaml
from pathlib import Path
import warnings
from sklearn.metrics import normalized_mutual_info_score as nmi_scorer
warnings.filterwarnings('ignore')

ROOT        = Path(__file__).resolve().parents[2]
IN          = ROOT / "data/staging/snapshots.parquet"
OUT_EVENTS  = ROOT / "data/canonical/events.parquet"
OUT_CASES   = ROOT / "data/canonical/cases.parquet"
SCHEMA      = ROOT / "configs/schema.yaml"

OUT_EVENTS.parent.mkdir(parents=True, exist_ok=True)
OUT_CASES.parent.mkdir(parents=True, exist_ok=True)

schema = yaml.safe_load(open(SCHEMA, "r", encoding="utf-8"))

NMI_THRESHOLD = 0.6 # used for filling missing assignment group

##--------------------------- load data -------------------------------##
df      = pd.read_parquet(IN)
df      = df.sort_values(['case_id', 'updated_at', 'system_update_count'], 
                         kind='mergesort')
waiting_status  = schema['waiting_status']


##---------------------- fix anomaly in opened_at -------------------------##

###  75% of diff between opened_at and created_at
diff_p75                = (df[df['updated_at'] > df['opened_at']]
                           .groupby('reported_by_uid')
                           .apply(lambda x: (x['created_at'] - x['opened_at'])
                                  .quantile(0.75))
                           )
### flag impossible (anomaly) in opened_at
anomaly_flag            = df['updated_at'] < df['opened_at']

### impute opened_at with 75% diff of created_at
df['diff_p75']          = df['reported_by_uid'].map(diff_p75)
df.loc[anomaly_flag, 
       'opened_at']     = df.loc[anomaly_flag, 'created_at'] - df.loc[anomaly_flag, 'diff_p75']

df.drop(columns='diff_p75', inplace=True) 


##------------------------ fill missing resolved_at --------------------------##

####  flag last resolved status is shown
resolved_entries        = df['case_status'].eq('Resolved')
missing_mask            = df['resolved_at'].isna() # mask known values
### get max updated_at
resolved_last_updated   = (df['updated_at'].
                            where(resolved_entries).
                            groupby(df['case_id']).
                            transform('max') # (max - in single resolved 'last' was giving nan)
                            )
### impute resolved_at with value of last (~max) updated_at
df.loc[missing_mask, 
       'resolved_at']   = resolved_last_updated[missing_mask]

### remaining nan with 
reamining_mask          = df['resolved_at'].isna() 
df.loc[reamining_mask, 
       'resolved_at']   = df.loc[reamining_mask, 'closed_at'] # cases without Resolved status


## ----------------------- fill missing created_at ---------------------------##

### for simulation only
df['created_at_missing_flag']   = df['created_at'].isna().astype('int8')

### for modeling and simulation
df['created_at_proxy']          = df['created_at'].combine_first(df['opened_at'])


## -------------------------fix & fill location_id-----------------------------##

### fill nan record with location_id in case if available
df['location_id']           = (df.groupby('case_id')['location_id']
                               .transform(lambda x: x.ffill().bfill())  # ffill handles NaNs mid-case too
                               )

### impute changing location_id with last value of case eventlog
ulocation                   = df.groupby('case_id')['location_id'].nunique(dropna=False) 
ulocation_cases             = ulocation[ulocation > 1].index
change_loc_mask             = df['case_id'].isin(ulocation_cases)
df.loc[change_loc_mask,
        'location_id']      = (df.loc[change_loc_mask]
                               .groupby('case_id')['location_id']
                               .transform('last')
                               )
        
        
## ---------------------------fill all nan affected_uid ------------------------------##

### no such event in dataset, however for new data can be effective
df['affected_uid']          = (df.groupby('case_id')['affected_uid']
                                .transform(lambda x: x.ffill().bfill())  # ffill handles NaNs mid-case too
                                )

### all nan per case_id
user_mask                   = df['affected_uid'].isna()
nan_user_location           = df.loc[user_mask, 'location_id'].unique()
loc_nunique_users           = (df.groupby('location_id')['affected_uid']
                               .apply(lambda x: x.nunique(dropna=False)) # per locations - no of unique users
                               .loc[nan_user_location] # fiter out locations with nan user
                               )
#### logic - single location links to single user in my dataset => assumes that's valid across population
valid_impute_loc             = (loc_nunique_users == 1)


## ------------------------ category and sub category features ------------------------------##

df['category_id']              = df.groupby('case_id')['category_id'].transform(lambda x: x.ffill().bfill())
df['subcategory_id']           = df.groupby('case_id')['subcategory_id'].transform(lambda x: x.ffill().bfill())


## ---------------------------- reported_by_uid feature --------------------------------------##

### created_by_uid values that NEVER appear alongside a valid reported_by_uid
system_accounts         = (set(df[df['reported_by_uid'].isna()]['created_by_uid'].dropna()) 
                           - set(df[df['reported_by_uid'].notna()]['created_by_uid'].dropna()))

### Extract number from 'Created by X' map to 'Opened by Xs'
sys_mapping             = {acc: f"Opened by {acc.split()[-1]}s" for acc in system_accounts}

### copy and mask where reported_by_uid is null
result                  = df['reported_by_uid'].copy()
null_mask               = result.isna()

### fill system accounts
result[null_mask]       = df.loc[null_mask, 'created_by_uid'].map(sys_mapping)
df['reported_by_uid']   = result

df.drop(['created_by_uid'], axis=1, inplace=True) # prevent multi-collinearity and even for business/human its just redundant.


##----------------------------- fill assigned_team_gid -----------------------------------------##

agent_cols          = ['assigned_uid', 'resolved_by_uid', 'updated_by_uid']

# NMI per (status, col)
nmi_scores          = {status: {col: nmi_scorer(df.loc[mask, 'assigned_team_gid'], 
                                                df.loc[mask, col]) 
                                if (mask := (df['case_status'] == status) 
                                    & df['assigned_team_gid'].notna() 
                                    & df[col].notna()).sum() >= 10 
                                else 0.0 for col in agent_cols} 
                       for status in df['case_status'].unique()}

# drop cols that are below threshold across ALL statuses — globally useless
useful_cols         = [col for col in agent_cols if max(nmi_scores[s][col] 
                                                        for s in nmi_scores) >= NMI_THRESHOLD]

# lookup per useful col
lookups             = {col: df[df['assigned_team_gid'].notna() 
                               & df[col].notna()].groupby(col)['assigned_team_gid'].agg(lambda x: x.mode()[0]) 
                       for col in useful_cols}

# Vectorized status-aware fill
result              = df['assigned_team_gid'].copy()

for status in df['case_status'].unique():
    sorted_cols = sorted(useful_cols, key=lambda c: nmi_scores[status][c], reverse=True)
    for col in sorted_cols:
        if nmi_scores[status][col] < NMI_THRESHOLD:
            continue
        still_null = result.isna() & (df['case_status'] == status)
        result.loc[still_null] = df.loc[still_null, col].map(lookups[col])

df['assigned_team_gid'] = result

## ----------------------- new features ---------------------------------- ##
df['time_taken'] = df.groupby('case_id')['updated_at'].diff(1)
df['time_taken']        = df['time_taken'].fillna((df['updated_at'] - df['opened_at']))
df['time_taken']        = df['time_taken'].dt.total_seconds() / (60*60)

df['active_work_hours'] = df['time_to_next_event_hours']

df.loc[df['case_status'].isin(waiting_status), 'active_work_hours'] = 0

# save
df.to_parquet(OUT_EVENTS, index=False)
print(f"canonical saved at: {OUT_EVENTS.relative_to(ROOT)}")



## --------------------- ### build cases table ### ----------------------- ##

# ensure sorted
df              = df.sort_values(['case_id','updated_at','system_update_count'])

# first and last events
first_events    = df.groupby('case_id').first()
last_events     = df.groupby('case_id').last()

cases = pd.DataFrame({
    
    "case_id"              : first_events.index,
    "finally_met_deadline" : last_events["met_deadline"],
    
    "opened_at"            : first_events["opened_at"],
    "created_at"           : first_events["created_at_proxy"],
    
    "resolved_at"          : last_events["resolved_at"],
    "closed_at"            : last_events["closed_at"],
    
    "location_id"          : first_events["location_id"],
    "category_id"          : first_events["category_id"],
    "subcategory_id"       : first_events["subcategory_id"],
    
})

cases.reset_index(drop=True, inplace=True)

cases["duration_in_hours"]  = ((cases["closed_at"] - cases["opened_at"])
                              .dt.total_seconds() / 3600
                              )

cases["only_active_hours"]  = (df.groupby("case_id")["active_work_hours"].sum().values)

cases.to_parquet(OUT_CASES, index=False)
print(f"cases saved at: {OUT_CASES.relative_to(ROOT)}")