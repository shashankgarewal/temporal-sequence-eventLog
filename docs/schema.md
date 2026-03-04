# Schema (ServiceNow Event Log)

## Entities
- **Case**: one support request (`case_id`)
- **Record**: one snapshot row at `update_timestamp` for that case

## Missing values
- `"?"` → `NaN` (publisher: missing means **unknown information**)

## Ordering
- Sort records within a case by:
  1) `update_timestamp`
  2) `count_sys_updates` (tie-breaker)

## Leakage guardrails'
Outcome fields have leakage-risk:
- `resolved_timestamp`, `closed_timestamp`, `resolution_id`, `resolved_by_agent_id`

Also, total / final count for cases will have leakage-risk:
- `total_reassigment`, `total_reopen`

## Raw → Canonical rename map
(number → case_id etc.) is defined in `configs/schema.yaml`.