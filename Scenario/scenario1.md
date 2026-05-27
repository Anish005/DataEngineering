# Incident Note: PySpark 11TB Pipeline Failure

## The Incident
- **Time:** 2:00 AM
- **Issue:** 11TB pipeline failed mid-run (Stage 4/9), blocking 6 downstream teams.
- **Error:** Spark executor OOM (Exit code 137).

## Root Cause
- **Action:** Added `.repartition(2000)` before a wide aggregation (`groupBy.agg`) to "improve parallelism."
- **Result:** Forced an unnecessary 11TB full shuffle before aggregation, causing the OOM.
- **Miss:** Tested on 50GB of clean dev data, failing to account for production data volume and skew.

### Code Comparison
**Bad:** `df.repartition(2000).groupBy("region").agg(sum("revenue"))`
**Good:** `df.groupBy("region").agg(sum("revenue"))` *(Let Spark/AQE decide)*

## The Fix
- Deleted the `.repartition(2000)` line.
- Restarted pipeline from an existing checkpoint at Stage 3.
- Pipeline completed, meeting the 8:00 AM business deadline.

## Lessons Learned
1. **Data Testing:** Always test with a 1–5% real production sample to account for actual skew and nulls.
2. **Shuffle Mechanics:** Never `.repartition()` before a wide shuffle. Trust Adaptive Query Execution (AQE) or use `.coalesce()` *after* if needed.
3. **Resilience:** Checkpoint placement is a deliberate design decision, not an afterthought.

## Post-Mortem Actions
Built a monitoring framework tracking:
- Shuffle size alerts
- Executor memory watermarks
- Stage duration drift
- Partition skew detection
*(Caught 4 similar errors in the subsequent 14 months)*