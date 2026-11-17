# Interview Guide: explain it like you're talking to a 5-year-old (then level up)

## The 30-second pitch
"I built a pipeline for an online-learning company. Students answer quiz questions, and those answers
arrive as files. My pipeline loads them incrementally with Auto Loader, cleans them, and builds tables
that tell teachers which students need help and which questions are too hard. I used the
Bronze/Silver/Gold pattern, enforced schemas, quarantined bad data, and orchestrated it with a Databricks Job."

## The toy-room story (ELI5)

| Real thing | 5-year-old version |
|---|---|
| Landing volume | A big **box where kids dump all their toys** (raw files). |
| Auto Loader | A **helper who remembers which toys he already picked up**, so he only grabs the new ones. |
| Checkpoint | A **bookmark** in the helper's notebook: "I stopped here." |
| Schema enforcement | A **shape-sorter**: a toy must fit its hole. |
| `_rescued_data` | The **"mystery bag"** for toys that don't fit. We don't throw them away, we look later. |
| **Bronze** | A **"just arrived" shelf**. Every toy is kept exactly as it came, with a sticker: *where from, what time*. |
| **Silver** | The **cleaning station**: broken toys go to a "fix later" bin, twins are removed, everything gets sorted properly. |
| Quarantine table | The **"fix later" bin**. |
| MERGE on event_id | **Name tags**: if a toy with this name tag is already on the shelf, don't add it again. |
| **Gold** | The **trophy board** in class: "who's doing great", "which puzzle is hardest". Teachers read this, not the messy box. |
| Job / workflow | A **to-do chain**: wash hands → eat → brush teeth. Step 2 starts only when step 1 is done; if one fails, try again. |

## Why three layers? (the answer interviewers want)
- **Bronze** = replayable source of truth. If my cleaning logic has a bug, I can rebuild Silver without asking the source again.
- **Silver** = trusted, deduplicated, typed data that many teams can reuse.
- **Gold** = shaped for a specific business question, fast to query.

## Likely questions and crisp answers

**1. How does incremental loading work?**
Auto Loader tracks which files it has already ingested in the checkpoint. Each run (`availableNow`) processes only new files, then stops. Re-running with no new files changes nothing.

**2. What is schema enforcement here, and what if the source adds a column?**
I pass an explicit schema. A new column (`app_version` in my simulator) doesn't crash the job; with `rescue` mode it goes into `_rescued_data`, so nothing is lost and I can promote it to a real column deliberately. (Alternative: `addNewColumns` mode if I want it to evolve automatically.)

**3. How do you handle duplicates and retries?**
Silver writes via `MERGE` on `event_id` inside `foreachBatch`. Retries and replays are idempotent.

**4. What happens to bad records?**
They go to `silver_quarantine` with a reason and timestamp. Nothing is silently dropped, and the data team can audit or reprocess.

**5. Why `availableNow` instead of continuous streaming?**
Same exactly-once, checkpointed semantics as streaming, but the cluster stops when done, so it's cheaper. For a 1-minute SLA I'd switch to a processing-time trigger.

**6. Why Delta?**
ACID transactions, schema enforcement, constraints, time travel, MERGE support.

**7. How would you monitor it?**
Row counts per layer, quarantine rate (alert if above a threshold), freshness of `_ingested_at`, Job failure alerts, and (next step) Lakeflow expectations.

**8. How would you scale it?**
Files → Kafka/Event Hubs source, partition Silver by `event_date`, `OPTIMIZE`/liquid clustering, incremental Gold (MERGE or materialized views) instead of overwrite.

**9. What would you improve?**
Add unit tests for validation rules, move to Lakeflow Declarative Pipelines with expectations, add a dashboard, and make Gold incremental.

**10. Biggest trade-off you made?**
Gold uses full overwrite for simplicity. Fine at this scale; at larger scale I'd make it incremental.

## Demo script (2 minutes)
1. Show the diagram in the README.
2. Run `01` twice -> show 2 files in the landing volume.
3. Run `02` -> Bronze count goes up; run `02` again -> count unchanged (incremental).
4. Show `_rescued_data` rows (schema drift caught).
5. Run `03` -> show Silver vs Quarantine counts, and `COUNT(*) = COUNT(DISTINCT event_id)`.
6. Run `04` -> show `gold_hardest_questions`.
7. Show the Job DAG in the Workflows tab.
