# Databricks notebook source
# MAGIC %md
# MAGIC ### 01 - Source simulator (the "school" sending us quiz results)
# MAGIC - Pulls REAL quiz questions from the public Open Trivia DB API (https://opentdb.com).
# MAGIC - Simulates students answering them and drops ONE new JSON file per run into the landing volume.
# MAGIC - Deliberately injects dirty data (nulls, negative times, duplicates, a surprise new column)
# MAGIC   so Bronze/Silver have something real to defend against.
# MAGIC Run it several times (or let the Job do it) to simulate a stream of arriving files.

# COMMAND ----------
import json, random, uuid, hashlib, html, requests
from datetime import datetime, timezone, timedelta

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "edtech_medallion")
dbutils.widgets.text("n_events", "500")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
n_events = int(dbutils.widgets.get("n_events"))
landing = f"/Volumes/{catalog}/{schema}/landing/quiz_events"

# COMMAND ----------
FALLBACK = [
    {"category": "Science: Computers", "difficulty": "easy", "question": "What does CPU stand for?"},
    {"category": "Mathematics", "difficulty": "medium", "question": "What is the derivative of x^2?"},
    {"category": "History", "difficulty": "hard", "question": "Who signed the Treaty of Westphalia?"},
]

def fetch_questions(n=50):
    """Real public API; falls back to a tiny local list if the API is rate-limited/unreachable."""
    try:
        r = requests.get("https://opentdb.com/api.php",
                         params={"amount": n, "type": "multiple"}, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])
        if results:
            return results
    except Exception as e:
        print(f"OpenTDB unavailable, using fallback: {e}")
    return FALLBACK

questions = fetch_questions()
catalog_rows = []
for q in questions:
    text = html.unescape(q["question"])
    catalog_rows.append({
        "question_id": hashlib.md5(text.encode()).hexdigest()[:10],
        "category": html.unescape(q["category"]),
        "course_id": "C" + hashlib.md5(q["category"].encode()).hexdigest()[:5],
        "difficulty": q["difficulty"],
    })

# COMMAND ----------
students = [f"S{str(i).zfill(4)}" for i in range(1, 201)]
now = datetime.now(timezone.utc)
add_new_field = random.random() < 0.3          # simulate upstream "schema drift"

def make_event():
    q = random.choice(catalog_rows)
    skill = random.random()
    p_correct = {"easy": 0.8, "medium": 0.55, "hard": 0.3}[q["difficulty"]] * (0.6 + 0.6 * skill)
    ev = {
        "event_id": str(uuid.uuid4()),
        "student_id": random.choice(students),
        "course_id": q["course_id"],
        "question_id": q["question_id"],
        "category": q["category"],
        "difficulty": q["difficulty"],
        "is_correct": random.random() < p_correct,
        "time_taken_sec": round(random.uniform(3, 90), 1),
        "event_ts": (now - timedelta(seconds=random.randint(0, 3600))).isoformat(),
        "device": random.choice(["mobile", "laptop", "tablet"]),
    }
    if add_new_field:
        ev["app_version"] = "2.1.0"            # column NOT in our schema -> goes to _rescued_data
    return ev

events = [make_event() for _ in range(n_events)]

# inject dirty data (~4%) and duplicates (~2%)
for ev in random.sample(events, int(n_events * 0.01)):
    ev["student_id"] = None
for ev in random.sample(events, int(n_events * 0.01)):
    ev["time_taken_sec"] = -5.0
for ev in random.sample(events, int(n_events * 0.01)):
    ev["difficulty"] = "impossible"
for ev in random.sample(events, int(n_events * 0.01)):
    ev["event_ts"] = "not-a-date"
events += random.sample(events, int(n_events * 0.02))

# COMMAND ----------
fname = f"{landing}/events_{now.strftime('%Y%m%d_%H%M%S')}.json"
dbutils.fs.put(fname, "\n".join(json.dumps(e) for e in events), True)
print(f"Wrote {len(events)} events -> {fname} (new field injected: {add_new_field})")
