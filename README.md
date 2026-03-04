# temporal sequence Event Log 

## Scope of the project
* Problem: Many cases are missing deadlines or took too long to complete, which leads to inferior experience.  
* Objective: Predict early which cases are likely to miss the dealine or take unusally long.
* Decision: Escalate or reassign case under limited capacity.
* Impact: Reduce no of cases that misses time-target, faster resolution, and better workload balance.
> Exact constraint values live in `configs/constraints.yaml`.
--- 

## Dataset
A timestamped event log (audit trail / change log) of case updates. 
Each case (case_id) appears multiple times — each row is one recorded update at `update_timestamp`, showing the case’s current status, assignment, and counters up to that moment.

**Entity definitions**
* Case: one support request / issue (unique case_id)
* Event/Record: one logged update to that case (a snapshot at `update_timestamp`)

**Target labels (for prediction)**
* On-time resolution (`met_deadline`): whether the case completed within an expected time window. 

* (future scope) `time-to-finish`: duration from `opened_timestamp` to `closed_timestamp` (or `resolved_timestamp`)

