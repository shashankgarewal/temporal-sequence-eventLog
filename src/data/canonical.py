# canonical.py
"""Transforms staged data into authoritative business form — fills and validates based on domain knowledge."""

import pandas as pd
import numpy as np
import yaml
from pathlib import Path
from src.utils.load import load, dump

import warnings
warnings.filterwarnings('ignore')

import logging
logger = logging.getLogger(__name__)

def build_canonical_events(snapshots_path: str = "data/staging/snapshots.parquet",
                           events_path: str = 'data/canonical/events.parquet', 
                           return_events: bool = False): 

       """Transform staged snapshots by applying business rules and product knowledge to produce the team-wide events data.

       Returns:
           events_path (str): relative path to events table (default) 
                            | transformed canonical events table
       """
       # ------------------------------ load and setup ------------------------------ #
       schema = load("configs/schema.yaml")
       event_order = schema['parsing']['event_order']
       
       df                   = load(snapshots_path)
       df                   = df.sort_values(['case_id'] + event_order, kind='mergesort')

       logger.info("building canonical events table")
       # ------------------------------ fill created_at ----------------------------- #
       # from notebook analysis -> created_at is missing for all case events/entries when missing. 

       # when case status is new, the action would be create case 
       #      -> implying updated_at time is the time case created
       result                      = (df.groupby('case_id').first()
                                          .query('case_status == "New" and created_at.isna()')
                                          ['updated_at'].to_dict()
                                      )
       flag                        = df['case_id'].isin(result.keys())
       df.loc[flag,'created_at']   = df.loc[flag, 'case_id'].map(result)

       # when case open and update time is same -> create time is same
       result                      = (df.query('opened_at == updated_at and created_at.isna()')
                                          .groupby(df['case_id'])
                                          .first()['opened_at'].to_dict()
                                          )
       flag                        = df['case_id'].isin(result.keys())
       df.loc[flag, 'created_at']  = df.loc[flag, 'case_id'].map(result)

       # captures the impute pattern - may come relavant in discussion with stakeholders+data-science team
       df['created_at_is_imputed'] = flag


       ##---------------------- fix anomaly in opened_at -------------------------##

       ### flag impossible (anomaly) in opened_at
       anomaly_flag            = df['created_at'] < df['opened_at'] 

       ###  75% of diff between opened_at and created_at
       diff_p75                = (df[~anomaly_flag]
                                   .groupby('reported_by_uid')
                                   .apply(lambda x: (x['created_at'] - x['opened_at'])
                                          .quantile(0.75))
                                   )

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
       valid_impute_loc            = (loc_nunique_users == 1)

       ### fix changing affected_uid cases
       varying_cases               = (df.groupby('case_id')['affected_uid'].nunique(dropna=True))
       suspicious_case_flag        = varying_cases[varying_cases > 1].index
       suspicious_modes            = (df[df['case_id'].isin(suspicious_case_flag)]
                                   .groupby('case_id')['affected_uid']
                                   .apply(lambda x: x.mode().iloc[0] 
                                          if not x.mode().empty else np.nan
                                          )
                                   )
       mask                        = df['case_id'].isin(suspicious_case_flag)
       df.loc[mask, 
              'affected_uid']      = df['case_id'].map(suspicious_modes)

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

       # prevent multi-collinearity and even for business/human its just redundant.
       df.drop(['created_by_uid'], axis=1, inplace=True) 

       # df.to_parquet(events_path, index=False)
       dump(df, events_path)
       
       if return_events:
              return df
       return events_path



## --------------------- ### build cases table ### ----------------------- ##
def build_canonical_cases(snapshots_path: str = "data/staging/snapshots.parquet",
                          events_path: str = 'data/canonical/events.parquet',
                          cases_path: str = 'data/canonical/cases.parquet',
                          return_cases: bool = False): 


       """Transform staged snapshots by applying business rules and product knowledge to produce the team-wide case_level data.

       Returns:
           cases_path (str): relative path to cases table (default) 
                            | transformed canonical cases table
       """
       
       try:
              df            = load(events_path)
       except:
              events_path   = build_canonical_events(snapshots_path, events_path)
              df            = load(events_path)
       
       # ensure sorted
       schema        = load("configs/schema.yaml")
       event_order   = schema['parsing']['event_order']
       df            = df.sort_values(['case_id'] + event_order)
       
       print("building canonical cases table")
       
       # first and last events
       first_events  = df.groupby('case_id').first()
       last_events   = df.groupby('case_id').last()

       cases = pd.DataFrame({
       
       "case_id"            : first_events.index,
       "met_deadline"       : last_events["met_deadline"],
       
       "opened_at"          : first_events["opened_at"],
       "created_at"         : first_events["created_at"],
       
       "resolved_at"        : last_events["resolved_at"],
       "closed_at"          : last_events["closed_at"],
       
       "location_id"        : first_events["location_id"],
       "category_id"        : first_events["category_id"],
       "subcategory_id"     : first_events["subcategory_id"],
       
       })

       cases.reset_index(drop=True, inplace=True)

       dump(cases, cases_path)
       if return_cases:
              return cases_path
       else:
              return cases