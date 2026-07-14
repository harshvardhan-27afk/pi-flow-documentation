# PI-Flow User Guide

This guide is written for people **authoring DAG files**. For every feature you get:
a short description, edge cases to know before you rely on it, and a code
snippet showing the exact syntax to use in a Python DAG file.



---

## Category: Per-DAG customization

### 1. DAG identity & docs

**Description:** Every DAG needs a unique `dag_id`. You can also attach a
human-readable `description` and `tags` (used for search/filtering in the UI).

**Edge cases:**
- `dag_id` must be unique across the whole repo — duplicate `dag_id`s across two
  files will collide in Postgres (last one ingested wins on upsert).
- `tags` must be a plain list of strings — it is JSON-serialized as-is; empty tags
  default to `[]`.
- **`owners` is *not* wired up from the DAG file today.** The DAG constructor
  accepts arbitrary `**kwargs` "for compatibility," so passing `owners="team-x"`
  will not raise an error, but it is silently dropped — nothing populates the
  `dag.owners` column from the parsed DAG file. If you need owners recorded, set
  it via the Connections/Admin UI or treat it as a documentation-only convention
  in your `description` until this is wired up.
- `description` is a plain string, not Markdown-rendered anywhere special — keep
  it short; it shows up in the DAG list.

**Code snippet:**
```python
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG
from dag_parser.dynamic.operators import PythonOperator

with DAG(
    dag_id="sales_daily_report",
    description="Aggregates daily sales figures and emails a summary report",
    tags=["sales", "reporting", "daily"],
    start_date=datetime(2026, 1, 1),
) as dag:

    def build_report():
        print("building report...")

    build_report_task = PythonOperator(
        task_id="build_report",
        python_callable=build_report,
    )
```

---

### 2. Schedule

**Description:** Controls when a DAG runs automatically. Three modes are
supported, all via the same `schedule` (or `schedule_interval`) parameter, or via
`timetable=` for named non-cron schedules:
1. **Cron expression** — standard 5-field cron or a descriptor (`@daily`,
   `@hourly`, `@weekly`, `@monthly`, `@yearly`).
2. **Named timetable** — `timetable="last_day_of_month"` or
   `timetable="business_days"` (Mon–Fri only). Only these two names exist today;
   anything else resolves to no timetable.
3. **Dataset-driven** — pass a list of `Dataset(...)` objects (or a dict) instead
   of a cron string; the DAG runs when upstream datasets are updated instead of
   on a clock.

**Edge cases:**
- `schedule` and `schedule_interval` are aliases for the same thing — don't set
  both expecting different behavior; `schedule_interval` wins if both are given
  a truthy value... actually the code does `schedule_interval or schedule`, so
  `schedule_interval` takes priority when both are non-empty. Simplest: only set
  one.
- If `timetable` is set, it's evaluated **instead of** cron — a DAG can't mix a
  cron schedule and a named timetable at the same time.
- Dataset-driven scheduling clears `schedule_interval` entirely (set to `None`)
  — a DAG is either time-based or dataset-based, not both.
- Default dataset trigger condition is `"all"` (every listed dataset must update)
  unless you use the explicit dict form with `"trigger_type": "any"`.
- An invalid cron expression fails at scheduler-evaluation time, not at parse
  time — double check syntax since bad DAGs won't get caught during ingestion.

**Code snippet — cron:**
```python
with DAG(
    dag_id="hourly_sync",
    schedule="0 * * * *",   # every hour on the hour
    start_date=datetime(2026, 1, 1),
) as dag:
    ...
```

**Code snippet — cron descriptor:**
```python
with DAG(
    dag_id="daily_job",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
) as dag:
    ...
```

**Code snippet — named timetable:**
```python
with DAG(
    dag_id="month_end_close",
    timetable="last_day_of_month",
    start_date=datetime(2026, 1, 1),
) as dag:
    ...

with DAG(
    dag_id="weekday_check",
    timetable="business_days",
    start_date=datetime(2026, 1, 1),
) as dag:
    ...
```

**Code snippet — dataset-driven:**
```python
from dag_parser.dynamic.dag_context import Dataset

with DAG(
    dag_id="consume_sales_table",
    schedule=[Dataset("s3://bucket/sales_table")],
    start_date=datetime(2026, 1, 1),
) as dag:
    ...

# Explicit form with "any" condition across multiple datasets:
with DAG(
    dag_id="consume_any_upstream",
    schedule={
        "datasets": [Dataset("s3://bucket/table_a"), Dataset("s3://bucket/table_b")],
        "trigger_type": "any",
    },
    start_date=datetime(2026, 1, 1),
) as dag:
    ...
```

---

### 3. Time window

**Description:** `start_date` bounds when a DAG can begin generating scheduled
runs; `end_date` (optional) stops it from scheduling further runs after that
point. `timezone` sets the DAG's local timezone for cron evaluation (defaults to
UTC if left empty).

**Edge cases:**
- `start_date` is required for any DAG that relies on automatic (cron/timetable)
  scheduling — without it there is no reference point to calculate the first run.
- `timezone` affects **wall-clock cron evaluation** (e.g. `"10 11 * * *"` means
  11:10 in the DAG's timezone, not UTC) — mismatched timezones are a common
  source of "why did my DAG run at the wrong hour" confusion.
- `end_date` only stops **future** scheduling; it does not cancel or affect
  already-running/queued runs.
- Leaving `timezone=""` (the default) means UTC — be explicit if your team
  expects local time.

**Code snippet:**
```python
from datetime import datetime

with DAG(
    dag_id="regional_batch_job",
    schedule="0 6 * * *",           # 06:00 daily...
    timezone="America/New_York",    # ...in US Eastern time, not UTC
    start_date=datetime(2026, 1, 1),
    end_date=datetime(2026, 12, 31),  # stop auto-scheduling after this date
) as dag:
    ...
```

---

### 4. Catchup/backfill flag

**Description:** `catchup` controls whether PI-Flow fills in every missed
scheduled interval between `start_date` and now (`catchup=True`, the default) or
only schedules the most recent interval going forward (`catchup=False`).

**Edge cases:**
- Default is `catchup=True` — if you set a `start_date` far in the past on a
  frequent schedule (e.g. hourly, `start_date` a year ago), the scheduler will
  try to create every missed run, capped per iteration by
  `SCHEDULER_MAX_CATCHUP_PER_ITERATION` (default 10 per poll cycle) — it will
  create the backlog gradually over multiple scheduler ticks, not all at once.
- For most "just run going forward" DAGs, explicitly set `catchup=False` to
  avoid an unexpected flood of historical runs the first time the DAG is
  ingested.
- `catchup=False` still respects `max_active_runs` and other guardrails; it just
  starts from "most recent fire" instead of "every missed fire since
  `start_date`."
- Catchup runs are still subject to `max_active_runs` — if the cap is hit,
  remaining catchup runs simply wait for a slot to free up on the next iteration.

**Code snippet:**
```python
# Backfill every missed run since start_date (default behavior)
with DAG(
    dag_id="full_history_backfill",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True,
) as dag:
    ...

# Only run going forward — ignore anything missed before now
with DAG(
    dag_id="forward_only_job",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ...
```

---

### 5. Run concurrency limits

**Description:** `max_active_runs` caps how many **DAG runs** of this DAG can be
`running`/`queued` at the same time. `max_active_tasks` caps how many **task
instances** belonging to this DAG can be `running` concurrently across all its
active runs.

**Edge cases:**
- `max_active_runs` defaults to `None` in the DAG constructor, but the Postgres
  schema itself defaults the column to `16` if nothing is supplied — don't
  assume "unlimited" just because you didn't set it.
- If `max_active_runs` is reached, new cron-triggered runs are **not created**
  (they wait for a running slot to free up) — this can silently delay a
  scheduled run if a previous run is stuck.
- `max_active_tasks` is enforced across **all runs of the DAG combined**, not
  per-run — with a low value and several concurrent runs, tasks from different
  runs will compete for the same task-concurrency budget.
- `max_active_tasks=None` (the default) means no DAG-level cap — tasks are still
  bounded indirectly by pool slots (`pool` table) and per-task `task_concurrency`.
- Setting these too low on a wide/fan-out DAG (many parallel tasks) will throttle
  a single run's own internal parallelism, not just cross-run parallelism.

**Code snippet:**
```python
with DAG(
    dag_id="throttled_ingest",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,     # never more than 1 run of this DAG in flight at once
    max_active_tasks=5,    # never more than 5 tasks (across all its runs) running at once
) as dag:
    ...
```

---

### 6. Run timeout

**Description:** `dagrun_timeout_seconds` force-fails an entire DAG run if it is
still `running` after that many seconds from its start, regardless of whether
individual tasks are still actively heartbeating.

**Edge cases:**
- This is a **strict wall-clock timeout** — unlike the global stale-run fallback
  (24h, only triggers if no task is actively heartbeating), an explicit
  `dagrun_timeout_seconds` fires even if tasks are healthy and progressing. Set
  it generously if your DAG legitimately runs long.
- If you don't set `dagrun_timeout_seconds` at all, the DAG still isn't
  unbounded forever — it falls back to a global 24h threshold, but that global
  fallback **only** force-fails the run if no task in it is actively
  heartbeating (i.e., it's truly abandoned/stuck, not just slow).
- On timeout, the run and its still-active child tasks are force-failed and a
  DLQ (`dead_letter_task`) entry is written — there's no partial "soft warning"
  state; it's a hard fail.
- This is a run-level timeout, distinct from **task**-level
  `execution_timeout_seconds` (per-task customization, covered separately) —
  set both if you want both layers of protection.

**Code snippet:**
```python
with DAG(
    dag_id="nightly_etl",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    dagrun_timeout_seconds=3600 * 4,   # force-fail the whole run if it exceeds 4 hours
) as dag:
    ...
```

---

### 7. DAG-level SLA

**Description:** `expected_duration_seconds` flags (but does not fail) a run
that takes longer than expected. When breached, PI-Flow records an SLA event
and — if you attach `on_sla_miss_callback` — enqueues that callback.

**Edge cases:**
- SLA miss is **detection only** — it never changes the run's state or stops
  it. Use `dagrun_timeout_seconds` (feature 6) if you actually want to kill a
  long-running run.
- Fires **once per run** — deduplicated via the `dag_run.sla_miss_fired` flag,
  so you won't get repeat notifications for the same slow run on subsequent
  scheduler ticks.
- This is DAG-level (whole-run elapsed time). There's a separate **task-level**
  SLA (`sla_seconds` on a task) for flagging individual slow tasks — the two
  are independent and can both be set on the same DAG.
- **Known limitation (verified in code):** `on_sla_miss_callback` (and
  `on_success_callback` / `on_failure_callback` — see feature 9) go through the
  same DAG-level callback pipeline, which currently has a serialization gap —
  see feature 9's edge cases before relying on this for real alerting. The SLA
  *detection* and `sla_miss` row always get recorded correctly regardless; it's
  only the callback-dispatch side that's affected.

**Code snippet:**
```python
with DAG(
    dag_id="revenue_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    expected_duration_seconds=600,   # flag as an SLA miss if a run takes > 10 minutes
) as dag:
    ...
```

---

### 8. Typed run parameters

**Description:** Declare a `params` schema on the DAG using `Param(...)` from
`dag_parser.dynamic.params`. This defines the shape of the `conf` a user must
supply when manually triggering the DAG (via UI or API) — types, defaults,
enums, numeric/string ranges, and required-ness. The UI renders a form from this
schema; the values themselves are **not** set in the DAG file, only the schema.

**Edge cases:**
- Supported `type` values are exactly: `string`, `integer`, `number`, `boolean`,
  `array`, `object`, `date` (`YYYY-MM-DD`, 10 chars), `datetime` (ISO string,
  ≥19 chars). Anything else raises `ValueError` at parse time (this **will**
  break ingestion for that DAG file).
- `required` is **inferred** if you don't set it explicitly: no `default` →
  treated as required; any `default` (including falsy ones like `0`, `False`,
  `""`) → treated as optional. If you want a param that's optional with no
  default, pass `required=False` explicitly.
- `enum`, `minimum`/`maximum`, `min_length`/`max_length`, and `pattern` (regex)
  are all validated **at trigger time**, not at DAG-parse time — a bad `conf`
  is rejected when someone tries to trigger a run, not when the DAG is ingested.
- `boolean` values are checked strictly (`isinstance(v, bool)`), and Python's
  `bool` is a subtype of `int` — the validator explicitly excludes booleans
  from passing as `integer`/`number`, so don't rely on `True`/`False` sneaking
  through as `1`/`0`.
- Values referencing these params inside tasks are read from
  `dag_run.conf`/templating (e.g. `{{ .Params.key }}` or Jinja `params.key`),
  not from the `Param` object itself — the `Param` object only exists to build
  the schema.

**Code snippet:**
```python
from dag_parser.dynamic.params import Param

with DAG(
    dag_id="etl_orders",
    schedule=None,             # manually triggered only
    start_date=datetime(2026, 1, 1),
    params={
        "run_date": Param(type="string", required=True, description="Business date, YYYY-MM-DD",
                           pattern=r"^\d{4}-\d{2}-\d{2}$"),
        "customer_id": Param(type="integer", default=0, minimum=0),
        "mode": Param(type="string", enum=["full", "incremental"], default="incremental"),
        "full_load": Param(type="boolean", default=False),
        "threshold": Param(type="number", default=0.95, minimum=0.0, maximum=1.0),
    },
) as dag:
    ...
```

---

### 9. DAG-level callbacks

**Description:** Attach `on_success_callback` / `on_failure_callback` (and
`on_sla_miss_callback`) to the DAG so an alert fires when a DAG **run** reaches
that terminal state — mirroring Airflow's DAG-level callback API.

**Edge cases — read before relying on this:**
- Syntactically, PI-Flow accepts the same shapes Airflow does: a plain Python
  function, or a notifier object like `SmtpNotifier(...)`. These get captured
  by the parser and stored as JSON on the `dag.callbacks` column, and the
  scheduler's finalizer correctly enqueues a `callback_request` row for
  `on_success_callback`/`on_failure_callback` when a run finishes.
- **However**, verified against the current code: the parser serializes a bare
  function as `{"type": "function", "name": "..."}` and a `SmtpNotifier(...)`
  as `{"type": "SmtpNotifier", ...}`. The `CallbackDispatcher` that actually
  sends alerts only recognizes `type` values `"email"`, `"slack"`,
  `"http_webhook"`, and `"pagerduty"` — neither `"function"` nor
  `"SmtpNotifier"` matches, so the dispatcher logs `unknown callback type` and
  silently drops it. **No email/Slack/webhook actually goes out today via
  `on_success_callback`/`on_failure_callback`/`on_sla_miss_callback`,** even
  though the row is created and marked processed.
- If you need a DAG run outcome to reliably trigger a real alert today, use a
  **task-level** callback instead (`params={"_callbacks": {"on_failure": {...}}}`
  on a task, using the dict shapes the dispatcher understands directly — e.g.
  `{"type": "email", "to": [...], "subject": "...", "html_content": "..."}`),
  or add a dedicated final task in the DAG (e.g. with `trigger_rule="all_done"`)
  that itself uses `SlackAPIPostOperator`/`EmailOperator`/HTTP to notify.
- Keep the `on_success_callback=my_function` / `on_sla_miss_callback=...` DAG
  syntax in your file if you like — it won't break ingestion — but don't treat
  it as your alerting mechanism until this gap is closed.

**Code snippet (documented syntax — see caveat above for current alert delivery):**
```python
from dag_parser.dynamic.dag_context import SmtpNotifier

def notify_failure(context):
    print(f"DAG failed: {context}")

with DAG(
    dag_id="critical_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    on_success_callback=SmtpNotifier(
        to=["team@company.com"],
        subject="critical_pipeline succeeded",
        html_content="<p>Run completed successfully.</p>",
    ),
    on_failure_callback=notify_failure,
) as dag:
    ...
```

---

### 10. Access control in DAG file

**Description:** Declare per-role permissions for this specific DAG directly in
code via `access_control={role_name: [permissions...]}`. On ingestion these are
synced into the `dag_permission` table with `source='dag_file'`, layered on top
of (and separate from) permissions set manually via the Admin UI
(`source='admin'`).

**Edge cases:**
- Valid permission strings are exactly: `can_read`, `can_trigger`, `can_edit`,
  `can_delete`, `can_clear`. Anything else in the list is simply not recognized
  (no error, no effect) — there's no `can_admin` at the per-DAG level.
- `role_name` must match an **existing** role by name (seeded roles: `Admin`,
  `Op`, `Editor`, `Viewer`, `Public`, or any custom role created via the Admin
  UI). A typo'd or nonexistent role name is skipped with a warning at ingestion
  time — it does not fail the DAG parse, so a silent typo means that role
  simply never gets the grant.
- Re-ingesting the DAG file **replaces** all `source='dag_file'` rows for that
  DAG each time — if you remove `access_control` from the file entirely (or
  delete the DAG file), any previously-synced `dag_file` grants are deleted on
  the next ingestion pass, falling back to whatever global role permissions
  apply.
- `source='admin'` grants (set via the UI/API rather than code) are **never**
  touched or overwritten by ingestion — DAG-file and admin-set permissions
  coexist per role/DAG (`UNIQUE(dag_id, role_id)` means one row per role, and
  the DAG-file sync only ever touches rows it owns, i.e. rows with
  `source='dag_file'`).
- This only scopes access to *this* DAG. Global, cross-DAG permissions (e.g. a
  role's default DAG visibility) are still governed by `piflow_permission` —
  `access_control` is an additive/overriding per-DAG layer, not a replacement
  for RBAC as a whole.

**Code snippet:**
```python
with DAG(
    dag_id="finance_close",
    schedule="@monthly",
    start_date=datetime(2026, 1, 1),
    access_control={
        "Editor": ["can_read", "can_trigger", "can_edit"],
        "Viewer": ["can_read"],
        "Op": ["can_read", "can_trigger", "can_clear"],
    },
) as dag:
    ...
```

---

### 11. Partitioning

**Description:** `partitions=` on the DAG declares that its runs are scoped to a
partition key (e.g. a date bucket, a region, a customer segment), using one of
the built-in partition classes: `DailyPartition()`, `HourlyPartition()`,
`WeeklyPartition()`, `MonthlyPartition()`, or `StaticPartition([...])`. Each run
gets a computed `partition_key`, and `partition_status` tracks per-partition
state (`pending`/etc.) independent of run history.

**Edge cases:**
- For the time-based partition types (`Daily`/`Hourly`/`Weekly`/`Monthly`), the
  `partition_key` is **derived automatically** from the run's `execution_date`
  (e.g. `DailyPartition()` → `"2026-07-09"`, `WeeklyPartition()` →
  `"2026-W28"`) — you don't set it yourself.
- `StaticPartition(keys)` is different: it does **not** derive a key from
  `execution_date` at all — the partition evaluator returns an empty key for
  `"static"` type, meaning **you must supply `partition_key` explicitly at
  trigger time** (via the trigger `conf`/API) for static-partitioned DAGs;
  there's no automatic mapping from a cron tick to one of your static keys.
- `StaticPartition(keys)` requires a non-empty list/tuple — an empty list raises
  `ValueError` at DAG-parse time (breaks ingestion for that file).
- Partitioning is orthogonal to scheduling — you still need a `schedule`/
  `timetable`/dataset trigger (or manual triggers) to actually create runs;
  `partitions=` only changes how those runs are tagged/tracked, not when they
  fire.
- Changing the partition type on an existing DAG (e.g. `Daily` → `Weekly`)
  changes how future `partition_key`s are computed but does not retroactively
  reclassify existing `partition_status` rows.

**Code snippet:**
```python
from dag_parser.dynamic.dag_context import DailyPartition, StaticPartition

# Time-based: partition_key auto-derived from execution_date, e.g. "2026-07-09"
with DAG(
    dag_id="daily_regional_load",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    partitions=DailyPartition(),
) as dag:
    ...

# Static: partition_key must be supplied explicitly at trigger time
with DAG(
    dag_id="per_region_backfill",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    partitions=StaticPartition(["us-east", "us-west", "eu-central"]),
) as dag:
    ...
```

---

### 12. Jinja customization

**Description:** Four DAG-level knobs that change how Jinja2 renders Python
operator fields at execution time: `render_template_as_native_obj` (return real
Python objects like `int`/`list` instead of strings when a template resolves to
a single native value), `user_defined_macros` (extra names available inside
`{{ }}` expressions), `user_defined_filters` (extra `|filter` functions), and
`template_undefined` (what Jinja does when a variable is missing — e.g.
`jinja2.StrictUndefined` to fail loudly instead of rendering an empty string).

**Edge cases:**
- These settings only apply to **Python-family operators**
  (`PythonOperator`/`ExternalPython`/`PythonVirtualenv`) — specifically their
  `op_args`, `op_kwargs`, and `templates_dict` fields, which are the only fields
  the parser marks for Jinja rendering. Bash/SQL/HTTP/etc. operator params are
  rendered with **Go templating** instead (`{{ .DS }}`, `{{ .Var.key }}` style),
  which does not read these Jinja options at all — setting
  `user_defined_macros` will have zero effect on a `BashOperator`'s `bash_command`.
- The worker **re-imports the DAG file itself** at task-execution time to read
  these four attributes straight off the live `DAG` object (they are not shipped
  through the DB as serialized task params). That means the DAG file must still
  exist and be importable on the worker node's checked-out repo at execution
  time — a DAG file that was valid at ingestion but is now broken/deleted on
  disk will fail Python task execution even if Postgres still has the DAG
  metadata.
- `render_template_as_native_obj=True` uses Jinja's `NativeEnvironment` — a
  template that resolves to a single expression (e.g. `"{{ params.count }}"`
  where `count` is an int) yields the real `int`, not the string `"5"`. Mixed
  text+expression templates (e.g. `"count={{ params.count }}"`) still render as
  a string regardless of this flag.
- `user_defined_macros` / `user_defined_filters` must be plain dicts of
  `{name: callable}` — passing something else won't raise at parse time (they
  aren't validated by the parser) but will fail with a Jinja/Python error at
  task-execution time.
- `template_undefined` expects a Jinja2 `Undefined` subclass (e.g.
  `jinja2.StrictUndefined`), not an instance — pass the class itself.

**Code snippet:**
```python
import jinja2

def to_upper(value):
    return str(value).upper()

with DAG(
    dag_id="native_templated_job",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    render_template_as_native_obj=True,
    user_defined_macros={"env_name": "production"},
    user_defined_filters={"upper": to_upper},
    template_undefined=jinja2.StrictUndefined,   # fail loudly on unknown template vars
) as dag:

    def process(count, label):
        print(f"{label}: {count} (type={type(count)})")

    PythonOperator(
        task_id="process",
        python_callable=process,
        op_kwargs={
            "count": "{{ params.count }}",              # renders as a real int (native obj)
            "label": "{{ env_name | upper }}",           # macro + custom filter
        },
    )
```

---

## Category: Per-task customization

> Note: the next 3 features (13–15) configure individual **tasks/operators**,
> not the `DAG` object itself — , but syntactically they go on the operator call
> (e.g. `PythonOperator(...)`), not inside `DAG(...)`.

### 13. Retry policy

**Description:** Per-task retry behavior: `retries` (max retry attempts),
`retry_delay_seconds` (fixed delay between attempts), `retry_exponential_backoff`
(switch to exponential delay instead of fixed), and `max_retry_delay_seconds`
(cap on how large the exponential delay can grow).

**Edge cases:**
- `retries` semantics are attempt-based, not retry-count-confusing: `try_number`
  is 1-based, and a task retries while `try_number <= retries`. So
  `retries=2` allows **3 total attempts** (1 initial + 2 retries) before it's
  marked `failed` and written to the dead-letter queue.
- Exponential backoff formula (only when `retry_exponential_backoff=True`):
  `delay = min(retry_delay_seconds * 2^(try_number-1), max_retry_delay_seconds)`,
  then a random **±10% jitter** is applied to avoid thundering-herd retries. If
  `retry_delay_seconds` is `0` or unset, the base is floored to `1` second
  before the exponential math runs — you won't get `delay=0` forever.
  With `retry_exponential_backoff=False` (the default), the delay is always the
  flat `retry_delay_seconds`, no growth.
- If `max_retry_delay_seconds` is not set, exponential backoff has **no
  ceiling** — a task with many retries can end up waiting a very long time
  between later attempts.
- You can set DAG-wide retry defaults once via `default_args=` at the DAG level
  instead of repeating them per task — a task-level value always wins over the
  DAG default when both are set; unset fields fall back to `retries=0`,
  `retry_delay_seconds=0`, `retry_exponential_backoff=False`.
- Retries are **not** applied to every failure mode uniformly — e.g. a task that
  hits its `execution_timeout_seconds` still goes through the same retry check
  (try_number vs retries), but a task that is explicitly skipped (via
  `PiFlowSkip`) does not consume a retry attempt at all.

**Code snippet:**
```python
# DAG-wide defaults via default_args...
with DAG(
    dag_id="resilient_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    default_args={
        "retries": 3,
        "retry_delay_seconds": 30,
        "retry_exponential_backoff": True,
        "max_retry_delay_seconds": 600,
    },
) as dag:

    # ...inherits retries=3, exponential backoff, 30s base, 600s cap
    default_behavior_task = PythonOperator(task_id="a", python_callable=lambda: None)

    # ...overrides just the retry count for this one task, keeps the rest
    flaky_task = PythonOperator(
        task_id="b",
        python_callable=lambda: None,
        retries=5,
    )
```

---

### 14. Trigger rule

**Description:** `trigger_rule=` on a task decides when it becomes eligible to
run based on the states of its upstream tasks, instead of the Airflow default
"all upstreams must succeed." PI-Flow implements all 12 Airflow trigger rules:
`all_success` (default), `all_failed`, `all_done`, `all_skipped`,
`all_done_setup_success`, `one_success`, `one_failed`, `one_done`,
`none_failed`, `none_failed_min_one_success`, `none_skipped`, `always`.

**Edge cases:**
- A task with **no upstream dependencies at all** is always immediately ready,
  regardless of what `trigger_rule` you set on it — the rule only matters once
  there's at least one upstream edge.
- `always` makes a task run unconditionally, independent of upstream state —
  useful for cleanup/notification tasks, but be aware it will also run even if
  every upstream task failed or was skipped.
- `none_failed_min_one_success` is stricter than `none_failed`: it additionally
  requires **at least one** upstream to have actually succeeded (not just "no
  failures") — a branch where every upstream was skipped will NOT satisfy
  `none_failed_min_one_success` but WILL satisfy plain `none_failed`.
  `all_done_setup_success` is `all_done` plus the additional requirement that any
  upstream `is_setup` tasks succeeded.
- An unrecognized/typo'd trigger rule string doesn't error at parse time — the
  scheduler logs a warning and **silently falls back to `all_success`
  semantics**, which can mask a typo (e.g. `"al_success"`) as a real bug in your
  DAG logic rather than an obvious config error.
- Trigger-rule evaluation only fires once the upstream state counts are no
  longer ambiguous for that rule (e.g. `one_success` can resolve early once one
  upstream succeeds, even if siblings are still running; `all_success` must wait
  for every upstream to reach a terminal state) — don't assume all rules resolve
  at the same point in a run.

**Code snippet:**
```python
with DAG(dag_id="branch_convergence", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    extract = PythonOperator(task_id="extract", python_callable=lambda: None)
    branch_a = PythonOperator(task_id="branch_a", python_callable=lambda: None)
    branch_b = PythonOperator(task_id="branch_b", python_callable=lambda: None)

    # Runs once EITHER branch finishes successfully, without waiting for both
    join = PythonOperator(
        task_id="join",
        python_callable=lambda: None,
        trigger_rule="one_success",
    )

    # Always runs a cleanup step, even if upstream tasks failed/were skipped
    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=lambda: None,
        trigger_rule="always",
    )

    extract >> [branch_a, branch_b] >> join
    [branch_a, branch_b] >> cleanup
```

---

### 15. Cross-run dependencies

**Description:** `depends_on_past=True` blocks a task instance until the *same
task* in the DAG's **previous run** finished as `success`. `wait_for_downstream=True`
blocks a task instance until every task **downstream of it** in the previous
run has reached a terminal state — protecting against a fast-moving new run
racing ahead of unfinished work from the last one.

**Edge cases:**
- Both checks look at the **immediately preceding run of the same DAG**, not
  "the last successful run" — if the previous run itself never got created (DAG
  just started, or was paused), there's no previous instance to compare
  against, so the check is effectively a no-op for the first run.
  a `depends_on_past` task simply waits until a previous-run row exists to
  evaluate against — it does not automatically retry forever if it never will.
- `wait_for_downstream=True` on a task with **no downstream tasks** in the DAG
  has nothing to wait on — it behaves like `wait_for_downstream=False` in that
  case.
- These flags are evaluated during the scheduler's re-evaluation phase (tasks
  sit in `none` state, gated, until the previous run's relevant task(s) reach a
  terminal state) — they do not fail the task if the previous run's task is
  still running; they simply hold it pending indefinitely until that resolves,
  which can look like a "stuck" DAG if a previous run itself is stuck.
- Both can be set as DAG-wide `default_args` and overridden per task, same
  inheritance pattern as retry policy (feature 13): task-level value wins,
  otherwise falls back to the DAG's `default_args`, otherwise `False`.
- Combining `depends_on_past=True` with `max_active_runs=1` effectively forces
  fully serial execution of that task across runs — useful for strictly
  sequential state machines, but be aware it removes any inter-run parallelism
  for that lineage.

**Code snippet:**
```python
with DAG(
    dag_id="sequential_state_machine",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
) as dag:

    # Won't start until this same task succeeded in the PREVIOUS run
    load_incremental = PythonOperator(
        task_id="load_incremental",
        python_callable=lambda: None,
        depends_on_past=True,
    )

    # Won't start until every downstream task from the PREVIOUS run's
    # "publish" task finished, preventing overlapping publishes
    publish = PythonOperator(
        task_id="publish",
        python_callable=lambda: None,
        wait_for_downstream=True,
    )

    load_incremental >> publish
```

---

### 16. Execution timeout

**Description:** `execution_timeout=<seconds>` on a task kills that task
attempt if it's still running past the given number of seconds.

**Edge cases:**
- **Kwarg naming trap:** the Python-facing argument is `execution_timeout`
  (no `_seconds` suffix), even though it lands in the DB column
  `execution_timeout_seconds`. Don't write `execution_timeout_seconds=` on the
  operator — it won't be recognized as that keyword (it'll just be swallowed
  into the operator's generic `**params`, silently doing nothing to the actual
  timeout).
- If you don't set it, the task isn't unbounded — it falls back to the global
  `WORKER_DEFAULT_TASK_TIMEOUT_SECS` (default 3600s / 1 hour).
- On expiry, the task's whole process group is signaled SIGTERM then SIGKILL —
  this is a hard kill, not a cooperative cancellation; anything the task was
  mid-write on may be left partially done if it wasn't atomic.
- A timeout counts as a normal failure for retry purposes — it goes through the
  same `try_number <= retries` check as any other failure (see feature 13), so
  set `retries` if you want a timed-out attempt to get another chance.
- This is a **task**-level timeout, independent from the DAG-level
  `dagrun_timeout_seconds` (feature 6) — a task can individually time out well
  before the whole run's timeout is reached.

**Code snippet:**
```python
slow_but_bounded = PythonOperator(
    task_id="slow_but_bounded",
    python_callable=lambda: None,
    execution_timeout=300,   # kill this task if it runs past 5 minutes
    retries=1,               # give it one more shot if it times out
)
```

---

### 17. Task-level SLA

**Description:** `sla=<seconds>` on a task flags an SLA miss if that task
instance hasn't reached a terminal state within `sla_seconds` of the DAG run's
logical/execution date. Unlike execution timeout, this never kills or fails the
task — it's a monitoring/detection signal, recorded once per task instance in
the `sla_miss` table.

**Edge cases:**
- **Kwarg naming trap (same pattern as feature 16):** the Python argument is
  `sla`, not `sla_seconds` — it maps to the DB column `sla_seconds` internally.
- The clock starts at the DAG run's **logical date**, not the task's own start
  time — a task scheduled later in a chain effectively has less of its own SLA
  budget left by the time it actually starts, since earlier tasks already ate
  into the same window.
- Deduplicated per `(dag_id, run_id, task_id)` — you get at most one `sla_miss`
  row and one callback attempt per task instance, regardless of how many times
  it's re-evaluated on later scheduler ticks.
- There's no dedicated per-task SLA-miss callback field — a miss reuses the
  **DAG's** `on_sla_miss_callback` (falling back to the DAG's
  `on_failure_callback` if no SLA callback is set). You cannot configure a
  different alert per task for its own SLA miss.
- **Verified limitation:** even when a DAG-level SLA/failure callback is set,
  the enqueued callback payload for a task SLA miss is wrapped as
  `{"callback": ..., "context": {...}}` without a top-level `"type"` field —
  which the `CallbackDispatcher` requires to route the alert (see feature 9's
  caveat). In the current code, task-level SLA misses do **not** result in a
  real email/Slack/webhook/PagerDuty alert being sent; treat this feature as an
  audit/monitoring signal (queryable via `sla_miss` / landing-time analytics in
  the UI) rather than a live alert channel today.

**Code snippet:**
```python
critical_step = PythonOperator(
    task_id="critical_step",
    python_callable=lambda: None,
    sla=120,   # flag an SLA miss if not done within 2 minutes of the run's logical date
)
```

---

### 18. Concurrency & pools

**Description:** `pool=<name>` assigns a task to a named concurrency pool
(shared slot budget across the whole system); `task_concurrency=<int>` caps how
many instances of that **specific task_id** can be running at once, across all
active runs of its DAG.

**Edge cases:**
- The `"default"` pool (used automatically if you don't set `pool=`) is seeded
  with **128 slots** — every task that doesn't explicitly opt into a different
  pool competes for that same shared 128-slot budget alongside everything else
  in the system.
- Assigning `pool="etl_heavy"` to a task **does not create that pool.** If no
  row for `"etl_heavy"` exists in the `pool` table, the scheduler treats it as
  having no configured limit (falls back to the configured default slot count)
  — meaning it silently behaves as effectively unlimited, not as a strict cap.
  Create the pool first (Pools UI/API) if you actually want a smaller ceiling.
- Pool slots are shared across **all DAGs** that reference the same pool name —
  a very active DAG can starve a lower-volume DAG's tasks if they share a pool.
- `task_concurrency` is scoped to one `(dag_id, task_id)` pair across all its
  runs — this is different from `max_active_tasks` (feature 5), which caps the
  whole DAG's concurrent tasks regardless of task_id. Both checks apply
  simultaneously; a task needs free room under pool, `max_active_tasks`, AND
  `task_concurrency` to be dispatched.
- Neither limit touches tasks already `running` — hitting a limit just means an
  eligible-but-blocked task stays `scheduled` and waits for a slot on a later
  scheduler tick; it's never skipped or failed because of pool/concurrency
  pressure.

**Code snippet:**
```python
heavy_task = PythonOperator(
    task_id="heavy_transform",
    python_callable=lambda: None,
    pool="etl_heavy",        # create this pool via the Pools UI/API for a real cap
    task_concurrency=2,      # never more than 2 concurrent instances of THIS task_id
)
```

---

### 19. Prioritization

**Description:** `priority_weight=<int>` (default `0`) ranks tasks against each
other **when there's contention** for dispatch (queue backlog, admission-level
shedding under load). `weight_rule` (`absolute` / `upstream` / `downstream`)
changes how that weight is computed relative to the task's position in the DAG
graph.

**Edge cases:**
- Priority only matters under contention — dispatch order is
  `ORDER BY priority_weight DESC, id ASC`, and admission-based shedding kicks in
  only at Elevated/Critical load (Critical: only weight ≥ 10 tasks are
  dispatched; Elevated: weight ≤ 0 tasks are skipped). On an idle system with no
  backlog, `priority_weight` has no visible effect at all.
- `weight_rule="upstream"`: effective weight = this task's own weight **plus
  the sum of every ancestor's own `priority_weight`**, computed recursively
  (cycle-safe). A task deep in a long chain automatically inherits a
  compounding boost from everything that came before it.
- `weight_rule="downstream"`: mirror image — effective weight = own weight
  **plus the sum of every descendant's weight**. A task that gates a large
  fan-out gets an automatic priority boost proportional to how much work
  depends on it.
- This upstream/downstream computation is a full dependency-graph walk done on
  **every planning pass**, for any DAG that has at least one non-`"absolute"`
  task — negligible for normal-sized DAGs, but it's real work, not a cached
  one-time value.
- `priority_weight` can be negative — nothing validates it — which pushes a
  task to the back of the dispatch order and makes it an early shedding target
  once load reaches Elevated (weight ≤ 0 is dropped).

**Code snippet:**
```python
with DAG(dag_id="priority_demo", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    # Absolute (default): weight is exactly 50, unaffected by graph position
    urgent = PythonOperator(task_id="urgent", python_callable=lambda: None, priority_weight=50)

    # Upstream: effective weight = 10 + sum of every ancestor's own priority_weight
    gate = PythonOperator(
        task_id="gate", python_callable=lambda: None,
        priority_weight=10, weight_rule="upstream",
    )

    urgent >> gate
```

---

### 20. Lifecycle role

**Description:** `is_setup=True` marks a task as preparing resources for the
DAG run; `is_teardown=True` marks a task as cleaning them up. Teardown tasks get
special scheduling treatment: they are held back from normal dependency-based
scheduling and are only triggered by the run finalizer once every non-teardown
("work") task in the run has reached a terminal state — Airflow-style `finally`
semantics.

**Edge cases:**
- **Teardown tasks never get scheduled during normal graph planning**, even if
  their upstream dependencies are already satisfied — they are explicitly
  skipped in the initial planning pass and only triggered later, in bulk, by
  the DAG-run finalizer once it detects all work tasks are done.
- Because of the above, a teardown task runs **regardless of whether upstream
  tasks succeeded or failed** — it's a true "run this no matter what" cleanup
  step, not gated by its own `trigger_rule` the way a normal task would be. In
  practice, teardown tasks are commonly declared with `trigger_rule="all_done"`
  to make that intent explicit in the DAG code, even though the finalizer drives
  the actual trigger.
- `is_setup=True` does **not** change scheduling behavior on its own — a setup
  task follows the normal dependency graph and its own `trigger_rule` like any
  other task. Its only special effect is that other tasks can use the
  `all_done_setup_success` trigger rule (feature 14), which additionally
  requires that any upstream `is_setup` tasks succeeded, not just reached
  "done."
- A task can't meaningfully be both `is_setup=True` and `is_teardown=True` at
  once — nothing in the parser rejects it, but the teardown scheduling behavior
  (held back until finalization) will simply take over, making the setup flag
  a no-op for that task.
- Setup/teardown pairing is a convention you build with dependency edges — the
  DAG file doesn't have to link a specific setup task to a specific teardown
  task; `is_teardown` tasks as a group are triggered together once work tasks
  finish.

**Code snippet:**
```python
with DAG(dag_id="managed_cluster_job", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    spin_up = PythonOperator(
        task_id="spin_up_cluster",
        python_callable=lambda: None,
        is_setup=True,
    )

    process = PythonOperator(task_id="process_data", python_callable=lambda: None)

    tear_down = PythonOperator(
        task_id="tear_down_cluster",
        python_callable=lambda: None,
        is_teardown=True,
        trigger_rule="all_done",   # documents intent; finalizer drives the actual trigger
    )

    spin_up >> process >> tear_down
```

---

### 21. Per-task callbacks

**Description:** `on_success_callback` / `on_failure_callback` /
`on_retry_callback` / `on_skipped_callback` on any operator fire per that
individual task instance's outcome, separate from the DAG-level callbacks in
feature 9. Unlike the DAG-level version, these ARE correctly wired end-to-end
into the working `params._callbacks` mechanism the parser/worker already use
(doc 03) — the parser auto-populates `task.params["_callbacks"]` from these
kwargs at ingestion time, and `task_runner.go` reads that key directly to
enqueue a `callback_request` per event.

**Edge cases:**
- **The plumbing is correct, but the payload format still isn't** — this is the
  same underlying gap as features 9 and 17. Whatever you pass
  (`on_failure_callback=my_function` or `on_failure_callback=SmtpNotifier(...)`)
  gets serialized by the parser into `{"type": "function", ...}` or
  `{"type": "SmtpNotifier", ...}`, and the `CallbackDispatcher` only recognizes
  `type` values `"email"`, `"slack"`, `"http_webhook"`, `"pagerduty"`. So a
  `callback_request` row is correctly created and marked processed, but no
  actual email/Slack/webhook/PagerDuty is sent — confirmed by tracing
  `_serialize_callback` in `dag_context.py` against the dispatcher's switch in
  `callback_dispatcher.go`. This applies uniformly regardless of whether you
  pass a function, a `SmtpNotifier`, or even a raw dict (a plain dict is
  neither "has `to_dict`" nor "callable," so it also collapses to
  `{"type": "unknown"}`).
- **A reliable workaround today:** add a dedicated notification task using an
  operator that actually executes as a task (not through the callback
  pipeline) — e.g. `SlackAPIPostOperator` or `EmailOperator` — wired with
  `trigger_rule="one_failed"` (fires only if something upstream failed) or
  `trigger_rule="all_done"` (always runs, success or failure). This sends a
  real message because it's a normal task execution via the Remote executor
  class, not a `callback_request` row.
- If you don't set a given event's callback, nothing is added for that event —
  `task_params["_callbacks"]` is only populated when at least one of the four
  is set on the task.
- These stack with DAG-level `default_args` the same way retries do (feature
  13): a task-level callback wins; otherwise the DAG's `default_args` value
  (if any) is inherited.
- `on_execute_callback` also exists on `BaseOperator` (fires when a task starts,
  not on a terminal state) but is **not** one of the events serialized into
  `_callbacks` (`TASK_CALLBACK_EVENTS` only covers success/failure/retry/skipped)
  — setting it is accepted but has no observable effect today.

**Code snippet (documented syntax — see caveat above for real alert delivery):**
```python
def log_retry(context):
    print(f"retrying: {context}")

flaky_call = PythonOperator(
    task_id="flaky_call",
    python_callable=lambda: None,
    retries=2,
    on_retry_callback=log_retry,
    on_failure_callback=log_retry,
)

# Reliable alternative for a REAL Slack alert on failure:
notify_on_failure = SlackAPIPostOperator(
    task_id="notify_on_failure",
    slack_conn_id="slack_default",
    text="flaky_call failed in {{ .DagID }} / {{ .RunID }}",
    trigger_rule="one_failed",
)

flaky_call >> notify_on_failure
```

---

### 22. Environment control

**Description:** Four related knobs control what a Python/Bash task actually
runs with: `env={}` (extra environment variables for the subprocess),
`append_env` (whether those vars are layered on top of a safe default
allowlist, or replace it down to bare essentials), `python=` on
`ExternalPythonOperator` (run under a specific pre-provisioned interpreter
path), and `venv=`/`requirements=` on `PythonVirtualenvOperator` (run under a
managed virtualenv materialized from a Snowflake stage).

**Edge cases:**
- **Env is never inherited from the orchestrator, by design** — this is a
  deliberate divergence from Airflow (which inherits the *full* parent process
  env when `env` is unset). PI-Flow tasks get a small fixed allowlist
  (`PATH`, `HOME`, `TZ`, `LANG`, `LC_ALL`, `DAGS_REPO_PATH`, `PYTHONPATH`) by
  default — orchestrator secrets (`POSTGRES_PASSWORD`, `JWT_SECRET`,
  `CONNECTION_PASSWORD_AES_KEY`, `SMTP_PASSWORD`, ...) are structurally
  excluded from that list, so there's no way to leak them into a task via `env=`
  even if you name them explicitly — you'd only be setting your own literal
  value, never reading the parent's.
- `append_env` defaults to `True` (overlay mode): your `env={}` vars are added
  **on top of** the default allowlist. Set `append_env=False` for a "pristine"
  subprocess — then only the bare essentials (`PATH`, `HOME`, `DAGS_REPO_PATH`,
  `PYTHONPATH`) plus your own vars are present; note even in pristine mode
  `TZ`/`LANG`/`LC_ALL` are dropped unless you set them yourself in `env=`.
- `env=` is only actually forwarded if you set it — passing
  `append_env=False` with **no** `env=` dict is a no-op (the code only branches
  into custom env-building when `env is not None`); you'll still get the normal
  default allowlist.
- `ExternalPythonOperator(python=...)` does **not** serialize/pickle the
  callable like Airflow does — the worker re-imports the DAG module fresh
  inside the target interpreter and looks up the callable by name. That target
  interpreter must therefore be able to `import dag_parser` (the SDK) and any
  third-party packages your callable needs — an arbitrary fully isolated venv
  that can't import `dag_parser` will silently degrade skip/defer behavior to
  no-ops rather than erroring clearly.
- `PythonVirtualenvOperator` requires **exactly one** of `venv=` (point at an
  already-built named environment) or `requirements=[...]` (Airflow-style list;
  PI-Flow derives a deterministic `auto_<hash>` env name from it) — passing
  both, or neither, raises `ValueError` at DAG-parse time (breaks ingestion).
- **Managed venvs are not built on demand per run.** `requirements=[...]`
  only *names* the environment (via a hash) — an admin still has to actually
  build that exact `auto_<hash>` environment once via `POST /api/admin/envs`
  (staged from a curated wheelhouse, not live PyPI) before any task can use it.
  A DAG referencing an environment that was never built will fail at task
  execution time, not at ingestion time.
- `venv`/`python` names must match `[A-Za-z0-9_-]+` — anything else (spaces,
  slashes typed directly into `venv=`, etc.) raises `ValueError` at parse time.

**Code snippet:**
```python
# env + append_env — overlay mode (default): allowlist + your vars
BashOperator(
    task_id="with_extra_env",
    bash_command="echo $STAGE $PATH",
    env={"STAGE": "staging"},
    append_env=True,   # default; PATH/HOME/TZ/... still present alongside STAGE
)

# env + append_env=False — pristine mode: only essentials + your vars
BashOperator(
    task_id="pristine_env",
    bash_command="echo $STAGE",
    env={"STAGE": "staging"},
    append_env=False,  # TZ/LANG/LC_ALL are dropped unless added to env= yourself
)

# ExternalPythonOperator — run under a specific pre-provisioned interpreter
ExternalPythonOperator(
    task_id="run_under_custom_interpreter",
    python_callable=lambda: None,
    python="/opt/piflow/venv-extra/bin/python3",
)

# PythonVirtualenvOperator — named pre-built managed venv
PythonVirtualenvOperator(
    task_id="run_in_managed_venv",
    python_callable=lambda: None,
    venv="pandas_env",   # must already be built via POST /api/admin/envs
)

# PythonVirtualenvOperator — Airflow-style requirements (auto-derived env name)
PythonVirtualenvOperator(
    task_id="run_with_requirements",
    python_callable=lambda: None,
    requirements=["pandas==2.2.0", "requests==2.31.0"],
)
```

---



## Category: Scheduling & triggering

### 23. Automatic runs

**Description:** DAGs with a `schedule`/`schedule_interval` or `timetable` get
`dag_run` rows created automatically by the scheduler's cron-evaluation phase —
no manual action needed. This is the run-creation mechanics underneath features
2 and 4 (schedule syntax, catchup).

**Edge cases:**
- **DST is handled by dedup, not by skipping.** During a "fall back" transition,
  a cron library can fire twice for the same wall-clock hour (e.g. 1:30 AM
  happens twice). PI-Flow deduplicates by wall-clock time within a single
  scheduler pass, so you'll only ever get one run for that repeated hour — but
  only *within the same batch evaluation*; this is a defense specific to how
  the batch evaluator processes catchup ranges.
- **`schedule="@once"`** is a special case: it creates exactly one run, at
  `start_date`, the first time the DAG is seen with no existing runs — after
  that, it never creates another run automatically no matter how long the DAG
  stays active.
- **Invalid cron/timetable never raises an ingestion error** — a bad cron
  string or a `timetable=` name that doesn't match a registered timetable
  (only `"last_day_of_month"` and `"business_days"` exist) is caught at
  *scheduling* time, logged as a warning, and that DAG simply never gets
  automatic runs — it won't show up as a parse failure, so check scheduler logs
  (or just "why hasn't this DAG ever run") if a new DAG seems inactive.
- **`{{ data_interval_start }}` / `{{ data_interval_end }}` are only populated
  for real cron/descriptor schedules** (and `@once`, as a degenerate
  `[start, start]`). DAGs using a named `timetable=` or with no schedule at all
  get `NULL` data-interval columns on their runs — templating those fields on a
  timetable-scheduled DAG fails loudly at the worker rather than silently
  returning something wrong.
- `max_active_runs` (feature 5) is enforced **at run-creation time** for
  automatic runs — if the DAG is already at its cap, the scheduler simply skips
  creating the next due run this tick and retries on the next poll; it doesn't
  queue up a backlog of "missed creations" beyond what catchup already tracks.

**Code snippet:**
```python
with DAG(
    dag_id="auto_hourly",
    schedule="0 * * * *",         # scheduler creates a run every hour, no action needed
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
    ...
```

---

### 24. Manual trigger

**Description:** Trigger a specific DAG on demand (via the UI "Trigger" button
or the API) with a custom `conf`. If the DAG declares a `params` schema
(feature 8), the values you supply are validated and merged with declared
defaults before the run is created.

**Edge cases:**
- **A paused DAG cannot be manually triggered** — the trigger request is
  rejected outright; you must unpause it first (feature: Pause/unpause,
  in-app DAGs).
- **Validated `params` vs. unvalidated `conf` are two different code paths.**
  If you supply typed `params` (matching the DAG's declared schema), they go
  through full validation (required/type/enum/range checks). But if you supply
  raw `conf` directly instead (bypassing `params`), it is inserted **with no
  schema validation at all** — "legacy" pass-through. `params` takes priority
  if both are supplied in the same request.
- **Verified gap: the trigger-time type validator only understands `string`,
  `integer`, and `boolean`.** `Param`'s Python-side schema supports 8 types
  (`string, integer, number, boolean, array, object, date, datetime` — feature
  8), but the Go validator that runs when you actually *supply a value* for a
  `number`/`array`/`object`/`date`/`datetime` param at trigger time falls into
  an "unsupported type" error path and rejects the request. In practice this
  means: params of those types can be **declared** fine and will parse/ingest
  fine, but manually triggering the DAG while actually **providing a value**
  for one of them via the typed `params` field will fail validation today.
  Workaround: pass such values via the legacy `conf` field instead (skips
  validation, but works).
- Triggering is **idempotent by execution date**: the run_id is deterministically
  derived from `dag_id` + `execution_date` (defaulting to "now" if you don't
  supply one). Firing the same manual trigger twice in immediate succession can
  collapse into the same run if the resolved execution dates match, rather than
  creating two separate runs.
- A manually-triggered run is pinned to the DAG's **latest** version hash at
  trigger time (same `dag_version` pinning as feature 1/6) — it does not adopt
  a newer version if the DAG file changes again before the run actually starts.

**Code snippet:**
```python
from dag_parser.dynamic.params import Param

with DAG(
    dag_id="manual_report",
    schedule=None,   # manual-trigger-only DAG
    start_date=datetime(2026, 1, 1),
    params={
        "region": Param(type="string", enum=["us", "eu", "apac"], default="us"),
        "record_limit": Param(type="integer", default=1000, minimum=1),
    },
) as dag:
    ...

# At trigger time (UI or API), supply:
#   {"params": {"region": "eu", "record_limit": 5000}}
# -> validated against the schema above, merged with any unset defaults.
```

---

### 25. Backfill

**Description:** Create runs for every scheduled interval in a historical
`[start_date, end_date]` range in one request, using the DAG's own cron
schedule to compute each `execution_date` — useful for re-running history after
a fix, independent of whether the DAG has already run those intervals.

**Edge cases:**
- **Requires an actual cron/descriptor `schedule_interval`.** DAGs with no
  schedule, `"@once"`, or (verified in code) a **named `timetable=`** all get
  rejected with a "no schedule" error — backfill computes dates by re-running
  the cron parser directly, so a timetable-scheduled DAG (`last_day_of_month`,
  `business_days`) currently **cannot** be backfilled through this endpoint,
  even though it schedules automatic runs fine.
- **Hard cap: 500 runs per backfill request.** A wide date range on a
  frequent schedule (e.g. a full year of an hourly DAG ≈ 8,760 runs) will be
  rejected outright rather than partially executed — narrow the range or split
  the request.
- **`reset_existing=True` deletes existing runs in the range first** — this is
  a real, irreversible delete of `dag_run` rows (and their task instances via
  cascade) before recreating them. Treat this option with the same caution as
  any destructive operation; don't set it unless you specifically intend to
  wipe and redo history for that range.
- **Verified gap: backfill does not respect `max_active_runs`.** Unlike
  automatic cron-created runs (feature 23), the backfill endpoint bulk-inserts
  every computed run directly, without checking the DAG's `max_active_runs`
  cap. A backfill over a wide range can create far more concurrently-eligible
  runs than the DAG's own concurrency limit would normally allow, and they'll
  all be planned/dispatched together, gated only by pool/worker capacity — not
  by `max_active_runs`.
- Backfill runs get `run_type='backfill'` and a deterministic run_id derived
  from `dag_id` + type + execution date. The bulk insert uses
  `ON CONFLICT (dag_id, run_id) DO NOTHING`, so re-running the same backfill
  request without `reset_existing` is safely idempotent — runs that already
  exist in the range are just left alone, and only genuinely missing ones get
  created.

**Code snippet (conceptual — issued via API/UI, not DAG-file syntax):**
```python
# The DAG itself needs nothing special beyond a real cron schedule:
with DAG(
    dag_id="daily_aggregation",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,   # catchup is irrelevant to backfill; this only affects auto-scheduling
) as dag:
    ...

# Backfill request (via UI "Backfill" action or API):
#   POST /api/dags/daily_aggregation/backfill
#   {"start_date": "2026-01-01", "end_date": "2026-01-31"}
# -> creates up to 31 daily runs for January, run_type='backfill'
```

---

### 26. Dataset-driven trigger

**Description:** A DAG scheduled with `schedule=[Dataset(...)]` (feature 2)
runs automatically when its upstream dataset(s) receive new events, evaluated
each scheduler tick against an `"any"` or `"all"` condition.

**Edge cases:**
- **Only one dataset-triggered run in flight at a time, regardless of
  `max_active_runs`.** Before evaluating conditions, the evaluator skips the
  DAG entirely if it already has a `running`/`queued` run — rapid repeated
  dataset updates while a run is in progress do not queue up additional runs;
  they're effectively coalesced (the next check after the current run finishes
  will pick up all events accumulated since the *last successful* run).
- **"Since" is anchored to the last *successful* run, not the last run.** If
  the most recent run **failed**, the "since" timestamp does not advance — the
  same dataset events that triggered the failed run are still considered "new"
  on the next evaluation, so the DAG will be re-triggered again by the same
  events rather than waiting for a fresh update.
- **`trigger_type` is matched case-sensitively against exactly `"any"` or
  `"all"`.** Using the explicit dict form (`schedule={"datasets": [...],
  "trigger_type": "Any"}`) with any casing/spelling other than lowercase `any`/
  `all` silently falls into an unrecognized-type branch that never triggers —
  no error at ingestion, the DAG just never fires automatically. Double-check
  spelling/case if a dataset DAG never runs.
- `"all"` requires every listed dataset to have **at least one** event since
  the last successful run — it does not require they update in the same
  request/moment, just that each independently has *some* event queued up by
  the time the check runs.
- A task produces a dataset event by declaring `outlets=[Dataset(...)]` on the
  operator and completing successfully — a failed producer task does not emit
  a dataset event, so a failure upstream naturally withholds downstream
  dataset-triggered DAGs (no partial/error events are emitted).

**Code snippet:**
```python
from dag_parser.dynamic.dag_context import Dataset

# Producer: emits a dataset event on success
with DAG(dag_id="produce_sales", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:
    load = PythonOperator(
        task_id="load_sales_table",
        python_callable=lambda: None,
        outlets=[Dataset("s3://bucket/sales_table")],
    )

# Consumer: runs automatically once the dataset above gets a new event
with DAG(
    dag_id="consume_sales",
    schedule=[Dataset("s3://bucket/sales_table")],
    start_date=datetime(2026, 1, 1),
) as dag:
    ...
```

---

### 27. Cross-DAG trigger

**Description:** `TriggerDagRunOperator(trigger_dag_id=..., conf=..., ...)`
lets one task start a run of a **different** DAG. Optionally,
`wait_for_completion=True` makes the parent task wait (non-blockingly — see
edge cases) until the child run reaches an allowed or failed state.

**Edge cases:**
- **The child DAG must exist and be unpaused**, or the task fails immediately
  with an explicit error — a paused child DAG is treated the same as a
  nonexistent one from the trigger task's perspective.
- **`conf` passed to the child is NOT validated against the child DAG's
  `params` schema** — unlike a UI/API manual trigger's `params` field (feature
  24), `TriggerDagRunOperator`'s `conf` goes straight into the child's
  `dag_run.conf` unchecked. A typo'd key or wrong type in `conf` won't be
  caught until the child DAG's own tasks try to use it (likely a template
  rendering failure deep inside the child run, not an obvious error up front).
- **`wait_for_completion=True` does NOT hold a worker slot** — the parent task
  returns immediately into a `waiting_for_child` state; a background scheduler
  phase (the deferred-task reconciler) polls the child's `dag_run` state and
  flips the parent to success/failed later. This means it's safe even on a
  single-worker cluster with deeply nested trigger chains — no deadlock risk
  from blocking slots.
- **`allowed_states`/`failed_states` default to `["success"]`/`["failed"]`.**
  If the child ends up in some other state that's in neither list, the parent
  simply **stays in `waiting_for_child` indefinitely** — there's no separate
  timeout on the wait itself (only the DAG-level `dagrun_timeout_seconds` /
  global stale-run fallback would eventually catch a truly stuck parent run).
- **Verified: `poke_interval=` on `TriggerDagRunOperator` is accepted but has
  no effect.** The executor that actually creates the child run and the
  reconciler that later checks its state don't read this parameter at all —
  it's dead syntax carried over from the Airflow-style API surface. The real
  poll cadence is just the scheduler's normal iteration interval.
- The generated child `run_id` is always `triggered__<parent_run_id>__<timestamp>`
  — you don't control or predict it in the DAG file; read it back via XCom
  (`trigger_run_id`) if a downstream task needs it.

**Code snippet:**
```python
from dag_parser.dynamic.dag_context import TriggerDagRunOperator

with DAG(dag_id="parent_pipeline", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="child_pipeline",
        conf={"batch_date": "{{ .DS }}"},   # NOT validated against child's params schema
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )
```

---

### 28. Time-based defer

**Description:** Instead of holding a worker slot while waiting, a task can
call `self.defer(trigger, method_name=..., timeout=...)` to register a trigger
and free the slot immediately; the **triggerer** (a separate polling loop,
default every 5s) evaluates the trigger and flips the task back to `scheduled`
when it fires. Three trigger types: `TimeDeltaTrigger(delta)` (fire after an
elapsed duration), `DateTimeTrigger(moment)` (fire at a specific timestamp),
and `HttpTrigger(endpoint, ...)` (fire when an endpoint returns the expected
status). `TimeSensor(target_time=...)` is the ready-made operator wrapping
similar "wait until a time of day" semantics without writing your own defer
logic.

**Edge cases:**
- **`TimeDeltaTrigger`'s elapsed time is measured from when `defer()` was
  called** (when the `trigger_instance` row was created), not from the DAG
  run's logical date or task start — two tasks in the same run that call
  `defer(TimeDeltaTrigger(60))` at different wall-clock moments fire 60 seconds
  after their own respective defer call, not in sync with each other.
- **`DateTimeTrigger(moment)` accepts a `datetime` or ISO string, but a naive
  (timezone-unaware) `datetime` is treated as UTC** by the triggerer's parser
  — there's no DAG-timezone adjustment applied to this specific trigger the
  way cron scheduling gets DAG-timezone treatment (feature 3). Pass a
  timezone-aware datetime or an explicit UTC-equivalent string if you need
  precision.
- **`HttpTrigger` treats network errors as "not fired yet," not as a
  failure.** An endpoint that's down or unreachable just means the trigger
  keeps polling silently — it will not error out on its own. The only thing
  that eventually stops an endlessly-unreachable `HttpTrigger` is the
  `timeout=` you pass to `defer()` (if set) or the trigger's own
  `trigger_instance.timeout_at`; without a timeout, it can poll forever.
- **`TimeSensor` defaults to `mode="reschedule"`** (frees the worker slot
  between pokes) rather than `mode="poke"` (holds the slot the whole time) —
  this matters for worker capacity planning: many concurrent `poke`-mode
  sensors can exhaust `MaxLocalTasks`/pool slots, while `reschedule`-mode ones
  don't hold anything between checks.
- A deferred/sensor task belongs to the **Sensor** executor class, which is
  only blocked at the `Critical` admission level (not `Elevated`) — sensors get
  more lenient scheduling priority than regular `Local`-class Python/Bash
  tasks under load.

**Code snippet:**
```python
from dag_parser.dynamic.dag_context import BaseOperator, TimeDeltaTrigger
from dag_parser.dynamic.operators import TimeSensor

# Ready-made sensor: wait until a specific time of day
wait_until_6am = TimeSensor(
    task_id="wait_until_6am",
    target_time="06:00",
    mode="reschedule",   # frees the worker slot between checks (default)
)

# Manual defer: a custom operator that waits 5 minutes, non-blockingly
class DelayedStep(BaseOperator):
    operator_name = "DelayedStep"

    def execute(self, context):
        self.defer(
            trigger=TimeDeltaTrigger(300),
            method_name="execute_complete",
            timeout=600,   # give up (fail) if it hasn't fired within 10 minutes
        )
```

---

## Category: Dependencies & flow

### 29. Declare edges

**Description:** Wire task dependencies with `>>` (downstream), `<<`
(upstream), or the explicit `set_upstream(other)` / `set_downstream(other)`
methods. Both operators accept a single task or a list of tasks on either side
for simple fan-out/fan-in.

**Edge cases:**
- `a >> b` returns `b`, which is what makes chaining work: `a >> b >> c` is
  `(a >> b) >> c`. `a << b` (meaning "a depends on b") returns `b` as well —
  so `a << b << c` chains as `(a << b) << c`, producing `c -> b -> a` (each
  further left-shift adds one more upstream layer), matching the intuitive
  reading "a comes after b, which comes after c."
- **Fan-out/fan-in with a list on ONE side works fine**: `a >> [b, c]`
  (one-to-many), `[b, c] >> d` (many-to-one), and the `<<` equivalents all
  work correctly — verified.
- **Verified limitation: a list on BOTH sides raises a `TypeError` at DAG
  parse time**, breaking ingestion for that file. `[a, b] >> [c, d]` is
  **not** supported — neither `list` has a `>>`/`<<` operator implementation
  on either side, so Python has no method to dispatch to. Airflow supports
  this pattern via internal `EdgeModifier` machinery; PI-Flow's parser mock
  does not replicate it. Expand it into explicit pairs instead (see snippet).
- `set_downstream`/`set_upstream` accept `list`, `tuple`, or `set` for the
  `other` argument — mixing task objects and something else (e.g. a raw
  string task_id) in that collection will fail, since the code always calls
  `.task_id` on each element.
- These calls only build the **dependency graph** — they don't return
  anything meaningful about execution order at DAG-authoring time; the actual
  ready/blocked/skip decision per task is governed by `trigger_rule`
  (feature 3 below, and feature 14 in Per-task customization).

**Code snippet:**
```python
with DAG(dag_id="edges_demo", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:
    extract = PythonOperator(task_id="extract", python_callable=lambda: None)
    validate = PythonOperator(task_id="validate", python_callable=lambda: None)
    transform = PythonOperator(task_id="transform", python_callable=lambda: None)
    load_a = PythonOperator(task_id="load_a", python_callable=lambda: None)
    load_b = PythonOperator(task_id="load_b", python_callable=lambda: None)
    report = PythonOperator(task_id="report", python_callable=lambda: None)

    # Simple chain
    extract >> validate >> transform

    # Fan-out: one task to many (list on ONE side — supported)
    transform >> [load_a, load_b]

    # Fan-in: many tasks to one (list on ONE side — supported)
    [load_a, load_b] >> report

    # NOT supported: [load_a, load_b] >> [report, some_other_task] (list >> list
    # raises TypeError at parse time). Expand it explicitly instead:
    # for src in [load_a, load_b]:
    #     for dst in [report, some_other_task]:
    #         src >> dst

    # Equivalent explicit-method form of the chain above:
    validate.set_upstream(extract)
```

---

### 30. Edge labels

**Description:** `Label("text")` is meant to visually annotate an edge for the
task-graph UI — e.g. `task_a >> Label("on_success") >> task_b` — without
affecting execution semantics.

**Edge cases:**
- **Verified bug: the documented `>>`-chaining syntax for `Label(...)` raises
  an `AttributeError` and crashes DAG parsing.** Tracing (and empirically
  running) `task_a >> Label("mylabel") >> task_b` against the current
  `dag_context.py` throws
  `AttributeError: 'Label' object has no attribute 'task_id'`. This happens
  because `Task.__rshift__` unconditionally calls `self.set_downstream(other)`
  on whatever sits on the right of `>>`, and `set_downstream` tries to read
  `.task_id` off it — a `Label` object has no such attribute, and Python
  resolves `task_a >> Label(...)` to `Task.__rshift__`, not the `Label`
  class's own `__rrshift__`, because `Label` isn't a subclass of `Task`. Using
  the docstring's own example syntax will break ingestion for that DAG file.
- **Working alternative (verified):** call `set_downstream`/`set_upstream`
  directly with the `label=` keyword argument instead of the `>> Label(...) >>`
  chain — this correctly populates `edge_labels` and does not crash:
  `task_a.set_downstream(task_b, label="on_success")`.
- Even when populated correctly, edge labels are **purely cosmetic** — they
  show up in the task-graph visualization only and have zero effect on
  trigger-rule evaluation, scheduling, or execution order.

**Code snippet:**
```python
with DAG(dag_id="edge_labels_demo", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:
    check = PythonOperator(task_id="check", python_callable=lambda: None)
    proceed = PythonOperator(task_id="proceed", python_callable=lambda: None)

    # DO NOT use: check >> Label("on_success") >> proceed
    #   -> raises AttributeError: 'Label' object has no attribute 'task_id'
    #      (verified against the current dag_context.py)

    # Use this instead — functionally identical, and it actually works:
    check.set_downstream(proceed, label="on_success")
```

---

### 31. Convergence control

**Description:** When multiple upstream branches/edges converge on one task
(a "join" point), that task's `trigger_rule` (feature 14 in Per-task
customization) decides what upstream state combination makes it ready,
skipped, or permanently blocked — this is the mechanism that actually governs
convergence behavior, not the edges themselves.

**Edge cases:**
- Declaring the edges (feature 1) only builds the graph; it never implies a
  join semantics on its own. A task with 3 upstream edges and the default
  `trigger_rule="all_success"` requires **all 3** to succeed — if you want
  "any one of these 3 finishing is enough," you must explicitly set
  `trigger_rule="one_success"` (or another applicable rule) on the converging
  task — the edges alone never imply "any" semantics.
- **Classic branch-then-join pattern**: after a `BranchPythonOperator` skips
  every path except one, a naive `all_success` join downstream of all
  branches would never be satisfied (the skipped branches aren't "success").
  Use `none_failed` or `none_failed_min_one_success` on the join task so
  skip-propagation from the unchosen branches doesn't permanently block it —
  this is the single most common reason a "task never runs" turns out to be a
  convergence trigger-rule mismatch, not a scheduling bug.
- A converging task only re-evaluates its rule once its upstream state counts
  change meaningfully — for rules like `all_success`/`all_done` it effectively
  waits for every incoming edge's task to reach a terminal state before it can
  resolve, even if 9 of 10 upstream branches finished quickly and one is slow.
- Mixing trigger rules across parallel converging tasks fed by the *same*
  upstream fan-out is fine and common — e.g. one join task using
  `one_success` for a "fast path" notification, and another using
  `all_success` for the "everything must have worked" gate, both reading the
  same set of upstream edges independently.

**Code snippet:**
```python
with DAG(dag_id="convergence_demo", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    choose_path = PythonOperator(task_id="choose_path", python_callable=lambda: None)
    # (BranchPythonOperator would return one of these task_ids at runtime)
    path_a = PythonOperator(task_id="path_a", python_callable=lambda: None)
    path_b = PythonOperator(task_id="path_b", python_callable=lambda: None)

    # Converges after a branch — must tolerate the unchosen path being 'skipped'
    join_after_branch = PythonOperator(
        task_id="join_after_branch",
        python_callable=lambda: None,
        trigger_rule="none_failed_min_one_success",
    )

    choose_path >> [path_a, path_b] >> join_after_branch
```

---

## Category: Dynamic & conditional

### 32. Dynamic task mapping

**Description:** `operator.expand(**kwargs)` (optionally chained after
`.partial(**fixed_kwargs)`) fans a single task definition out into N task
instances at **runtime**, one per element of an iterable — either a literal
Python list or an upstream task's XCom output (`XComArg`, e.g. a TaskFlow
task's return value). Expansion happens in the scheduler's Phase 2.7, after
the template task's upstreams are all terminal; each resulting instance gets
`map_index=0..N-1` and its own slice of args in `mapped_params`.

**Edge cases:**
- **Only a single `.expand()` key is actually honored at runtime.** The parser
  will serialize however many kwargs you pass to `.expand(a=[...], b=[...])`
  into the `expand_args` column, but `task_expander.go` (Go) only resolves
  **the first key it iterates in the JSON map, then `break`s** — Go map
  iteration order is randomized per run, so with more than one expand kwarg
  you'll non-deterministically get only one of them applied to
  `mapped_params`, and the rest silently dropped from execution. Pass exactly
  one kwarg to `.expand()` (Airflow's own single-key-expand convention);
  anything else is not a supported pattern here despite parsing successfully.
- **Empty iterable → the whole mapped task is skipped, not zero-instance
  no-op.** If the resolved list (literal or XCom) has length 0, the template
  instance (`map_index=-1`) is marked `skipped` directly and **no** `map_index
  0..N-1` rows are created — a downstream join needs `none_failed` /
  `none_failed_min_one_success` (feature 14/31) to not get permanently blocked
  by this.
- **`xcom_ref` expansion requires the upstream XCom value to already be a
  JSON array.** If the referenced upstream task's `return_value` is a scalar
  (e.g. a plain int or string) rather than a list, `task_expander.go` fails to
  parse it as a JSON array and the expansion attempt errors out — it is
  **not** auto-wrapped into a single-element list despite what the code
  comment nearby suggests. On this failure the template instance is simply
  left in `none` state and retried on every subsequent scheduler tick
  (logged as a warning), which looks like a silently stuck task rather than
  an explicit failure — check scheduler logs, not the task's own state, if a
  mapped task never expands.
- `.partial(**fixed_kwargs)` values are merged into every expanded instance's
  `mapped_params` first, then the expand key/value for that slice is merged
  on top — a partial arg sharing the same name as the expand key is
  overwritten per-instance, not combined.
- Retries, timeouts, pool, and trigger rule are **not** per-slice — they come
  from the shared `task` row (same definition for every `map_index`), only
  `mapped_params` differs per instance. There is no way to give one mapped
  slice a different timeout than its siblings.

**Code snippet:**
```python
from dag_parser.dynamic.dag_context import XComArg

with DAG(dag_id="fanout_files", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    def list_files():
        return ["file_a.csv", "file_b.csv", "file_c.csv"]

    list_task = PythonOperator(task_id="list_files", python_callable=list_files)

    def process_file(filename, region):
        print(f"processing {filename} in {region}")

    # partial() holds the fixed arg; expand() supplies ONE varying arg per instance
    process = PythonOperator.partial(
        task_id="process_file",
        python_callable=process_file,
    ).expand(
        # single expand key only — see edge case above
        filename=XComArg("list_files"),   # or a literal list: filename=["a.csv", "b.csv"]
    )

    list_task >> process
```

---

### 33. Branching

**Description:** `BranchPythonOperator(python_callable=...)` runs a Python
function whose return value — a single `task_id` string or a list of
`task_id`s — selects which of its **direct** downstream tasks actually run;
every other direct downstream task is marked `skipped` by the scheduler
(`BranchEvaluator`, Phase 2.5), with the skip cascading further downstream
through any tasks exclusively reachable via the unselected path.

**Edge cases:**
- **Returning `None` (or forgetting to `return` at all) fails the task, it
  does not skip everything.** The worker serializes a Python `None` return as
  an empty string, and `executor_branch.go` explicitly rejects an empty
  `return_value` with `"branch operator must return a task_id or list of
  task_ids"` — a branch callable must always return something.
- **A returned `task_id` that isn't a direct downstream of the branch task is
  silently ignored (not an error).** `BranchEvaluator` validates selected IDs
  against the branch task's actual downstream edges and just logs a warning
  for anything that doesn't match — a typo'd task_id in your branch function
  won't fail the run; that path just never gets selected and nothing
  downstream runs unless another branch selects it too.
- **Skip-cascading is trigger-rule aware, not blanket.** A task further
  downstream of an unselected branch is only cascade-skipped once *all* of
  its upstreams are terminal AND its own `trigger_rule` (feature 14) would be
  permanently blocked given that mix — e.g. a join using
  `none_failed_min_one_success` fed by both the chosen and unchosen branch is
  correctly left alone (not skipped) as long as the chosen branch can still
  succeed. Tasks with `trigger_rule="always"` are never cascade-skipped.
- Branch evaluation only touches downstream tasks that are still in `none`
  state — it runs every scheduler iteration across all running runs, so a
  branch decision made mid-run is enforced on the very next tick, not
  instantly in the same pass that recorded it.
- The branch decision is pushed as the task's normal `return_value` XCom
  (same key other tasks use) — nothing stops a downstream task from also
  reading it via `xcom_pull`, but be aware it's consumed by the scheduler for
  routing regardless of whether you also read it yourself.

**Code snippet:**
```python
from dag_parser.dynamic.operators import BranchPythonOperator

with DAG(dag_id="branch_demo", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    def choose_branch(**context):
        if context["ds"] < "2026-06-01":
            return "legacy_path"          # single task_id
        return ["new_path_a", "new_path_b"]  # or a list of task_ids

    branch = BranchPythonOperator(
        task_id="choose_branch",
        python_callable=choose_branch,
        provide_context=True,
    )

    legacy_path = PythonOperator(task_id="legacy_path", python_callable=lambda: None)
    new_path_a = PythonOperator(task_id="new_path_a", python_callable=lambda: None)
    new_path_b = PythonOperator(task_id="new_path_b", python_callable=lambda: None)

    join = PythonOperator(
        task_id="join",
        python_callable=lambda: None,
        trigger_rule="none_failed_min_one_success",   # tolerates the skipped branch(es)
    )

    branch >> [legacy_path, new_path_a, new_path_b] >> join
```

---

### 34. Voluntary skip

**Description:** Raising `PiFlowSkip("reason")` (imported from
`dag_parser.dynamic.dag_context`) anywhere inside a Python callable marks that
task instance `skipped` instead of `failed` — PI-Flow's equivalent of
Airflow's `AirflowSkipException`. It's the idiomatic way to say "there was
nothing to do this run" without it counting as an error.

**Edge cases:**
- **Confirmed end-to-end: no retry is consumed and `on_failure_callback` does
  not fire.** `task_runner.go`'s `StatusSkipped` branch flushes state
  `"skipped"` directly and fires `on_skipped_callback` instead — it never
  goes through the failure/retry path (feature 13), so `retries=N` is
  irrelevant to a voluntarily-skipped attempt.
- Downstream tasks see this exactly like any other `skipped` state for
  trigger-rule purposes (feature 14/31) — a default `all_success` downstream
  task will be blocked/cascade-skipped the same as if a `BranchPythonOperator`
  had explicitly skipped it; use `none_failed`-family rules on anything that
  should still proceed.
- `PiFlowSkip` only works from **inside the Python callable's own exception
  flow** — raising it from an unrelated thread you spawned yourself, or after
  the callable has already returned normally, has no effect (the worker only
  catches it around the callable's direct invocation).
- The message you pass to `PiFlowSkip("reason")` is written to the task log
  (stderr, `"[python] task skipped: {reason}"`) but is **not** persisted
  anywhere queryable (no dedicated "skip reason" column) — if you need the
  reason available later for reporting, also write it via `ti.xcom_push` or a
  task note before raising.
- This is Python-only — there's no equivalent "voluntary skip" mechanism
  documented for Bash/SQL/HTTP-family operators; those only have
  failure/success as outcomes.

**Code snippet:**
```python
from dag_parser.dynamic.dag_context import PiFlowSkip

def maybe_process(**context):
    if context["ds"] in ("2026-01-01", "2026-12-25"):
        raise PiFlowSkip("holiday — nothing to process today")
    print("processing normally...")

conditional_task = PythonOperator(
    task_id="conditional_process",
    python_callable=maybe_process,
    provide_context=True,
)
```

---

### 35. Setup/teardown

**Description:** Same underlying mechanism as feature 20 (Per-task
customization — Lifecycle role): `is_setup=True` / `is_teardown=True` mark
resource-lifecycle tasks that bookend the "real" work in a DAG. Listed again
here because the catalog groups it under **Dynamic & conditional** — the
angle that matters in this category is that a teardown task's trigger
behavior is **not** driven by the normal dependency graph or its own
`trigger_rule` at all; it's driven unconditionally by the run finalizer once
every non-teardown task is done, which is a different activation mechanism
than everything else in this category (branching, skip, mapping all operate
through the graph/trigger-rule machinery).

**Edge cases:**
- See feature 20 for the full breakdown (teardown tasks are excluded from
  normal planning and only bulk-triggered by the finalizer; `is_setup` alone
  changes nothing except enabling `trigger_rule="all_done_setup_success"`
  downstream; setting both `is_setup` and `is_teardown` on the same task lets
  the teardown behavior silently win).
- Relevant to this category specifically: a teardown task **always** runs
  even along a path a `BranchPythonOperator` skipped, or after a task raised
  `PiFlowSkip` — because it isn't gated by upstream state at all, it doesn't
  matter whether the "work" tasks upstream succeeded, failed, or were
  skipped via any of the mechanisms in features 32–34.
- A teardown task **cannot** itself be the target of a `.expand()` mapping
  gated the normal way — since it's exempted from the standard "upstreams
  terminal → ready" planning pass, don't rely on the task-expander's readiness
  check (feature 32) to gate a teardown; it will be triggered by the
  finalizer regardless of whether its own upstreams are terminal enough for a
  normal candidate check.

**Code snippet:**
```python
with DAG(dag_id="cluster_lifecycle", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    spin_up = PythonOperator(task_id="spin_up", python_callable=lambda: None, is_setup=True)

    def maybe_process(**context):
        raise PiFlowSkip("no data today")  # even if every "work" task skips...

    process = PythonOperator(
        task_id="process", python_callable=maybe_process, provide_context=True,
    )

    tear_down = PythonOperator(
        task_id="tear_down",
        python_callable=lambda: None,
        is_teardown=True,       # ...this still runs, triggered by the finalizer
        trigger_rule="all_done",
    )

    spin_up >> process >> tear_down
```

---

## Category: Data passing & templating

### 36. XCom (Python)

**Description:** Inside a Python callable, `ti.xcom_push(key, value, map_index=None)`
and `ti.xcom_pull(task_ids=None, key="return_value", map_indexes=None)` read/write
small values through Postgres over an internal HTTP API (`localhost:8083/internal/xcom/*`).
`xcom_pull` defaults to pulling the calling task's **own** key, but accepts a
specific `task_ids` string, a **list** of `task_ids` (returns a list, same
order), and `map_indexes` (an int for one slice, `"all"` for every mapped
slice, or a list zipped against `task_ids`). Every non-`None` return value is
auto-pushed to `return_value` on success; `@task(multiple_outputs=True)`
additionally fans a dict return out into one XCom row per key.

**Edge cases:**
- **Loud failure on repeated transport error, not silent loss.** Both
  `xcom_push` and `xcom_pull` retry the internal HTTP call up to
  `_xcom_max_retries` (default 3) times with backoff, then **raise** —
  failing the task — rather than swallowing the error. A key that's
  genuinely absent (never written) still returns `None` from `xcom_pull`
  (that's a normal "not found" response, not a transport failure).
- **A mapped caller (`map_index >= 0`) defaults to pulling the matching index
  from its upstream**, not `map_index=-1` — if you call
  `ti.xcom_pull(task_ids="upstream")` from inside mapped instance `map_index=2`
  with no explicit `map_indexes=`, you get index `2`'s value from `upstream`
  (a "mapped→mapped zip"), not the template or index 0. Pass
  `map_indexes="all"` explicitly if you want every slice's value as a list
  regardless of your own index.
- **Every value is JSON-encoded, always** — pushing `5` and pushing `"5"` are
  stored (and round-trip) as distinct JSON types (`5` vs `"5"`), so
  `xcom_pull` returning `5` vs `"5"` is meaningful and not just a formatting
  quirk. A value that can't be JSON-serialized falls back to
  `json.dumps(str(value))` (its string repr, silently) rather than erroring.
- **`do_xcom_push=False` suppresses the auto `return_value` push (and the
  `multiple_outputs` fan-out) but never blocks an explicit
  `ti.xcom_push(...)` call** — the two are independent; the flag only governs
  the automatic behavior, not manual pushes inside your callable.
- `return None` from a callable pushes **nothing** (no `return_value` row at
  all) — this differs from returning `""`, which **does** get pushed (as the
  JSON string `"\"\""`), matching Airflow's own None-vs-empty-string
  distinction.

**Code snippet:**
```python
def extract(**context):
    ti = context["ti"]
    ti.xcom_push(key="row_count", value=42)
    return {"status": "ok"}   # auto-pushed to "return_value"

def consume(**context):
    ti = context["ti"]
    count = ti.xcom_pull(task_ids="extract", key="row_count")      # specific key
    upstream_return = ti.xcom_pull(task_ids="extract")              # default key="return_value"
    many = ti.xcom_pull(task_ids=["extract", "load"])               # list -> list of values
    all_slices = ti.xcom_pull(task_ids="mapped_task", map_indexes="all")  # every mapped slice
    print(count, upstream_return, many, all_slices)

extract_task = PythonOperator(task_id="extract", python_callable=extract, provide_context=True)
consume_task = PythonOperator(task_id="consume", python_callable=consume, provide_context=True)
extract_task >> consume_task
```

---

### 37. TaskFlow XCom refs

**Description:** With the `@task` decorator (`dag_parser.dynamic.dag_context.task`),
calling one task-decorated function and passing its result directly as an
argument to another auto-wires both the dependency edge **and** the XCom
pull — no manual `xcom_pull`/`xcom_push` needed. Each call to a `@task`
function returns an `XComArg` (a reference to that task's `return_value`),
and passing an `XComArg` as an argument to another `@task` call resolves it
to the real value at execution time.

**Edge cases:**
- **Only works at the top level of keyword/positional arguments.** The
  parser only inspects each bound argument value with `isinstance(value,
  XComArg)` directly — an `XComArg` nested inside a list or dict you pass in
  (e.g. `transform(data=[extract_a(), extract_b()])`) is **not** detected: no
  upstream dependency edge is wired, and the raw (non-JSON-serializable)
  `XComArg` object gets passed through, which will fail DAG ingestion's
  JSON-serializability check on `op_kwargs`. Pass each upstream result as its
  own top-level argument instead.
- **Only `op_kwargs` are ever populated by `@task` calls** — positional
  arguments to your decorated function are internally re-bound to their
  parameter names and always end up as `op_kwargs`, never `op_args`. This
  matters if you're mixing `@task`-decorated functions with plain
  `PythonOperator(op_args=[...])` elsewhere in the same DAG — the XCom-ref
  resolution logic in the worker (`_run_task.py`) itself also only scans
  `op_kwargs`.
- **Resolved values are opportunistically JSON-decoded.** The worker tries
  `json.loads()` on whatever string comes back from the XCom pull before
  handing it to your function — if the upstream task returned a plain string
  that also happens to be valid JSON (e.g. `"123"` or `"null"`), your
  downstream function receives the *decoded* type (`int 123`, `None`), not
  the original string, which can be surprising if you genuinely meant a
  numeric-looking string.
- **Calling the same `@task`-decorated function more than once in a DAG
  auto-suffixes the `task_id`** (`_2`, `_3`, ...) so each call gets a distinct
  task — you don't need to pass `task_id=` yourself for repeated calls, but
  you also can't rely on the first call's task_id being reused.
- Dependency wiring from an `XComArg` argument is **one-directional and
  automatic only** — you can still add extra manual edges (`>>`) alongside
  it; TaskFlow-inferred edges and explicit edges coexist without conflict.

**Code snippet:**
```python
from dag_parser.dynamic.dag_context import task

with DAG(dag_id="taskflow_demo", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    @task
    def extract():
        return [1, 2, 3]

    @task
    def transform(data):          # `data` will be the real list at execution time
        return [x * 2 for x in data]

    @task
    def load(data):
        print(f"loading {data}")

    # Calling these wires both the dependency edges AND the XCom pulls automatically
    raw = extract()
    doubled = transform(raw)
    load(doubled)
```

---

### 38. Auto XCom from operators

**Description:** Several built-in operators push a `return_value` XCom
automatically on success, without any Python code — useful for wiring a
non-Python task straight into a downstream `ti.xcom_pull` or TaskFlow
`XComArg`. Verified per-operator shapes: **SQL-family** (SQLExecuteQuery /
Snowflake / MySql / Postgres / Redshift) push the **first result row** as a
JSON object (`{"col": value, ...}`); **BashOperator** pushes raw **stdout**
as a plain string; **HTTP** pushes the raw **response body** as a string;
**SSH** pushes the remote command's **combined stdout+stderr**; the
**S3ToRedshift** loader pushes `{"rows_loaded": N}`; **SlackAPIPostOperator**
pushes `{"ts": "<message timestamp>"}`.

**Edge cases:**
- **The auto-push is unconditional on "is `ReturnValue` non-empty," not
  operator-specific.** `task_runner.go` pushes `return_value` for *any*
  successful task whose executor sets a non-empty `ReturnValue` — so a Bash
  command with genuinely empty stdout, a SQL query returning zero rows, or an
  HTTP response with an empty body simply produces **no** XCom row at all
  (not an empty-string row) — `xcom_pull` for that task returns `None`, not
  `""`.
- **SQL/Snowflake only capture the FIRST row** — if your query returns
  multiple rows, everything after row 1 is silently discarded from
  `return_value`; there's no auto "all rows as a list" option today. Use an
  explicit multi-row export mechanism (or a Python task) if you need more
  than one row passed downstream.
- **HTTP response bodies are truncated at 1MB** (`io.LimitReader`), silently
  — a larger response doesn't error, it just hands your downstream task a
  truncated `return_value` with no indication that data was cut off.
- **SSH's auto-XCom is `CombinedOutput` — stdout AND stderr interleaved into
  one string** — unlike BashOperator, which keeps stdout (pushed to XCom) and
  stderr (log-only) strictly separate. A remote command that writes
  diagnostic noise to stderr will have that noise mixed into the
  `return_value` an SSH task's downstream consumers pull.
- Bash's `return_value` is the **raw stdout string**, not JSON-parsed — if
  your script prints JSON, a downstream Python task must `json.loads()` it
  itself; a downstream `.expand(xcom_ref=...)` (feature 32) additionally
  requires that string to already parse as a JSON *array*.

**Code snippet:**
```python
# SQL: first row auto-pushed as {"id": 1, "name": "acme"} etc.
get_customer = SQLExecuteQueryOperator(
    task_id="get_customer",
    connection_id="warehouse_pg",
    sql="SELECT id, name FROM customers WHERE id = 1",
)

# Bash: raw stdout auto-pushed as a plain string
count_files = BashOperator(task_id="count_files", bash_command="ls /data | wc -l")

def use_upstream(**context):
    ti = context["ti"]
    row = ti.xcom_pull(task_ids="get_customer")        # {"id": 1, "name": "acme"}
    stdout_text = ti.xcom_pull(task_ids="count_files")  # raw stdout string, e.g. "42\n"
    print(row, stdout_text)

use_task = PythonOperator(task_id="use_upstream", python_callable=use_upstream, provide_context=True)
[get_customer, count_files] >> use_task
```

---

### 39. Go templating

**Description:** Bash/SQL/HTTP/SSH/etc. (every non-Python-family operator)
have their `params` rendered through Go's `text/template` before execution,
exposing `{{ .DS }}` (YYYY-MM-DD), `{{ .TS }}` (YYYY-MM-DDTHH:MM:SS),
`{{ .DSNodash }}`/`{{ .TSNodash }}`, `{{ .ExecutionDate }}`/`{{ .LogicalDate }}`
(full ISO8601), `{{ .DagID }}`/`{{ .TaskID }}`/`{{ .RunID }}`/`{{ .TryNumber }}`/
`{{ .MapIndex }}`, `{{ .Params.key }}`/`{{ .Conf.key }}` (the triggering
`dag_run.conf`), `{{ .Var.key }}` (PI-FLOW Variables), plus three macros:
`ds_add .DS <days>`, `ds_format .DS "<Go layout>"`, `ts_add .ExecutionDate "<duration>"`.

**Edge cases:**
- **`.DS`/`.TS` are computed in the DAG's own `timezone` (feature 3), not
  UTC** — even though everything is stored in UTC in Postgres, an
  `America/New_York`-timezoned DAG sees its local calendar date in `{{ .DS }}`.
- **A typo in a struct field fails the task loudly; a typo in a map key
  renders silently empty.** `{{ .Prams }}` (misspelling the `TemplateContext`
  field itself) is an execute-time error that **fails the task** — but
  `{{ .Params.typo }}` (a missing key inside the `Params`/`Var` map) just
  renders as an empty string, matching Airflow's default (non-strict)
  Undefined behavior. These look like the same class of mistake but behave
  completely differently.
- **The three macros (`ds_add`, `ds_format`, `ts_add`) fail silently on bad
  input, returning the original string unchanged** rather than erroring —
  e.g. `{{ ds_add .TS 7 }}` (passing a full timestamp where a bare
  `YYYY-MM-DD` is expected) just returns `.TS` untouched with no warning.
  Double-check you're feeding `ds_add`/`ds_format` a `YYYY-MM-DD` string and
  `ts_add` an RFC3339 timestamp.
- **`{{ .Var.key }}` reads the exact same global Variables table as the
  Python `var.value.x`/`var.json.x` accessor (feature 41), but with the
  opposite failure mode on a missing key** — Go silently renders empty
  (per above), while the Python Jinja accessor raises `AttributeError` on
  the same lookup. Don't assume a Bash task and a Python task will fail (or
  not fail) the same way for the same typo'd variable name.
- Rendering is skipped entirely — for the whole param blob, cheaply — if the
  raw JSON doesn't contain the literal substring `{{ ` anywhere, so a param
  with no template markers has zero rendering overhead.

**Code snippet:**
```python
regional_extract = BashOperator(
    task_id="regional_extract",
    bash_command=(
        "python extract.py --date={{ .DS }} "
        "--window_start={{ ds_add .DS -7 }} "
        "--compact_date={{ ds_format .DS \"20060102\" }} "
        "--region={{ .Params.region }} "
        "--api_key={{ .Var.extract_api_key }}"
    ),
)
```

---

### 40. Jinja2 templating

**Description:** Python-family operators (`PythonOperator`, `ExternalPythonOperator`,
`PythonVirtualenvOperator`) render their `op_args`/`op_kwargs`/`templates_dict`
fields with a **real Jinja2** engine inside `_run_task.py` — not the Go
renderer (feature 39) — giving access to the full Airflow-style context:
`ds`, `ts`, `dag_run`, `ti`, `params`, `conf`, `var.value.*`/`var.json.*`,
`macros.*` (`ds_add`, `ds_format`, `datetime`, `timedelta`), `dag`/`task`
mock objects, and (when present) real `datetime` objects for
`data_interval_start`/`data_interval_end`.

**Edge cases:**
- **Only fields the parser lists in `params["_template_fields"]` get Jinja
  treatment, and the Go renderer explicitly skips exactly those same fields**
  to avoid a double-render — this handoff is what makes features 39 and 40
  mutually exclusive per-field rather than both trying to render the same
  string. Non-Python operators have no `_template_fields`, so the Go renderer
  processes their entire `params` map as-is (feature 39).
- **`var.value.x` / `var.json.x` raise `AttributeError` on a missing key —
  loud, not empty.** This is the DAG-level `template_undefined` knob's
  default-adjacent behavior (feature 12) applied specifically to the `var`
  accessor: unlike the Go side's silent-empty (feature 39), a typo'd variable
  name here crashes the task with a clear error naming the missing attribute.
- **`var.json.x` parses the variable's stored string value as JSON** — if
  the variable wasn't actually stored as valid JSON, this raises a JSON
  decode error at render time; use `var.value.x` for a plain string instead.
- **`data_interval_start`/`data_interval_end` are only real `datetime`
  objects when the scheduler actually computed an interval** (cron/descriptor
  schedules — feature 23); for a manually-triggered, timetable-scheduled, or
  dataset-triggered run these are absent, and referencing them in a template
  still fails loudly (per feature 12's "loud Undefined" design) rather than
  silently returning `None`.
- `dag`/`task` mock objects (`{{ dag.dag_id }}`, `{{ task.retries }}`, etc.)
  are rebuilt by **re-importing the live DAG file** at execution time
  (feature 12's caveat applies identically here) — if the DAG file on disk
  has since been broken/deleted, these attributes fall back to the raw
  context ids rather than crashing, which is more forgiving than the
  `_template_fields` rendering itself (which does fail loudly on bad syntax).

**Code snippet:**
```python
def report(**context):
    print(f"ds={context['ds']}, region={context['params'].get('region')}")
    print(f"api_key={context['var'].value.get('extract_api_key')}")

report_task = PythonOperator(
    task_id="report",
    python_callable=report,
    provide_context=True,
    op_kwargs={
        "note": "{{ ds }} run for {{ dag.dag_id }}, "
                "window={{ macros.ds_add(ds, -7) }} to {{ ds }}, "
                "threshold={{ var.json.threshold_config }}",
    },
)
```

---

### 41. Variables & Connections

**Description:** **Variables** (`variable` table, key→string value, optional
`is_encrypted`) are a flat global key-value store, readable from both Go
templates (`{{ .Var.key }}`, feature 39) and Python Jinja (`var.value.x` /
`var.json.x`, feature 40). **Connections** (`connection` table) store
reusable external credentials (host/port/username/password/extra JSON) keyed
by `connection_id`, referenced by name from operators like
`SnowflakeOperator(connection_id=...)`, `SQLExecuteQueryOperator(connection_id=...)`,
`S3ToRedshiftOperator`, etc. — resolved and decrypted by the executor at
execution time, not through templating.

**Edge cases:**
- **Every task gets every Variable, with no per-DAG or per-role scoping.**
  The worker loads the **entire** `variable` table (decrypting any encrypted
  rows) into every task's context on every execution — there's no mechanism
  to restrict which DAGs/tasks can see which variable, unlike DAG-level
  `access_control` (feature 10). Treat Variables as workspace-wide
  configuration, not per-team secrets isolation.
- **`is_encrypted` variables are decrypted before ever reaching a task** —
  the plaintext is what a Bash/SQL/Python task actually sees in
  `{{ .Var.x }}` / `var.value.x`; encryption only protects the value at rest
  in Postgres and in the Admin UI's list view (masked as `***`), not from any
  task author who references that key.
- **Connections are looked up directly by the executor via `connection_id`,
  not through the Go/Jinja template engines at all** — you cannot do
  `{{ .Var.some_conn_password }}` to read a connection's password; the only
  way to use a connection's credentials is to pass its `connection_id` to an
  operator that knows how to look it up (Snowflake/SQL/S3/etc. executors each
  query the `connection` table and call `security.DecryptPassword` themselves).
- **A `connection_id` that doesn't exist fails at task-execution time, not
  DAG-parse time** — `connection_id` is just a string param; ingestion never
  validates it against the `connection` table, so a typo'd or deleted
  connection only surfaces as a "connection lookup failed" error when the
  task actually runs.
- Both Variables and Connections are managed exclusively through the
  Admin/Config UI or API (`/api/variables`, `/api/connections`) — there is no
  DAG-file syntax to *create* a Variable or Connection, only to *reference*
  one by key/`connection_id`.

**Code snippet:**
```python
# Reference a Variable from a Bash task (Go templating)
notify = BashOperator(
    task_id="notify",
    bash_command="curl -X POST {{ .Var.slack_webhook_url }} -d 'text=job done'",
)

# Reference a Variable from a Python task (Jinja/context)
def use_variable(**context):
    threshold = context["var"].value.get("threshold_config")
    print(f"threshold={threshold}")

use_var_task = PythonOperator(
    task_id="use_variable", python_callable=use_variable, provide_context=True,
)

# Reference a Connection by connection_id (created beforehand via the UI/API)
extract = SnowflakeOperator(
    task_id="extract",
    connection_id="snowflake_prod",   # must already exist in the connection table
    sql="SELECT * FROM sales.orders WHERE order_date = '{{ .DS }}'",
)
```

---

## Category: Alerting & notifications

### 42. Email alerts

**Description:** A task-level callback of `{"type": "email", "to": [...],
"subject": "...", "html_content": "..."}` (under `params={"_callbacks":
{event: {...}}}`) sends a real email via SMTP, through the same
`EmailExecutor` the standalone `EmailOperator` task uses. Requires SMTP to be
configured (`SMTP_HOST`, etc. — doc 05).

**Edge cases:**
- **`to` must be a list, and `subject` is required** — `EmailExecutor`
  explicitly rejects a request with `"params.to is required (list of
  recipients)"` or `"params.subject is required"` if either is missing;
  there's no default subject line.
- **"with tokens" means exactly four literal placeholders, not a real
  templating engine.** `subject` and `html_content` go through
  `CallbackDispatcher.templateReplace`, a plain `strings.NewReplacer` that
  only recognizes `{{dag_id}}`, `{{task_id}}`, `{{run_id}}`, `{{event}}`
  (both with and without inner spaces). Airflow-style Jinja (`{{ ds }}`,
  `{{ ti.xcom_pull(...) }}`) or Go templating (`{{ .DS }}`) **do not** work
  here — anything else in the string is left completely as-is, un-rendered.
- **No connection is involved at all** — email alerts read SMTP server
  credentials from the global `SMTP_*` env vars/config (doc 05), not from a
  `connection_id`; if `SMTP_HOST` isn't configured, the dispatcher logs
  `"email executor not configured"` and drops the alert (the `callback_request`
  row is still marked processed).
- The callback fires **per task instance**, once per triggering event — a
  mapped task (feature 32) fires its own email once per `map_index` that
  reaches the configured event, not once for the whole mapped group.

**Code snippet:**
```python
notify_failure = PythonOperator(
    task_id="risky_step",
    python_callable=lambda: None,
    params={
        "_callbacks": {
            "on_failure": {
                "type": "email",
                "to": ["oncall@company.com"],
                "subject": "PI-Flow failure: {{dag_id}}.{{task_id}}",
                "html_content": "<p>Run <b>{{run_id}}</b> failed on event {{event}}.</p>",
            }
        }
    },
)
```

---

### 43. Slack alerts

**Description:** A task-level callback of `{"type": "slack", "connection_id":
"..."}` posts a fixed-format Block Kit message (header + DAG/Task/Run/Event
fields) to the Slack **incoming webhook URL** stored on that connection.

**Edge cases:**
- **Webhook-only — a token-auth Slack connection will not work here.** The
  Connections UI's Slack connector supports both `auth_type="webhook"` and
  `auth_type="token"` (bot token, for the task-level `SlackAPIPostOperator`,
  feature 21's workaround), but the **callback** dispatcher
  (`resolveConnectionWebhookURL`) only ever reads `extra.webhook_url` — a
  connection created with `auth_type="token"` (no `webhook_url` set) fails
  with `"connection ... missing webhook_url in extra"` when used for an alert
  callback, even though it's perfectly valid for a `SlackAPIPostOperator`
  task. Create a separate webhook-mode connection specifically for alerting
  if your primary Slack connection uses a bot token.
- **The message content is NOT configurable** — unlike Email/HTTP webhook,
  the Slack alert's Block Kit text is entirely auto-generated
  (`"Callback {event} for task {task_id} in DAG {dag_id} (run: {run_id})"`);
  any other keys you put in the callback config dict besides `connection_id`
  are silently ignored — there's no `text`/`message`/`blocks` override for
  this channel.
- **`webhook_url` is stored AES-encrypted and decrypted at send time** — same
  encryption path as connection passwords (feature 05); a malformed/
  un-encrypted value in `extra.webhook_url` (e.g. hand-inserted directly into
  Postgres) will fail decryption rather than being used as plaintext.
- A non-200 response from the Slack webhook URL is treated as a failed send
  (logged, not retried) — there's no automatic retry/backoff for a
  transient Slack outage; the `callback_request` row is still marked
  processed either way.

**Code snippet:**
```python
notify_failure = PythonOperator(
    task_id="risky_step",
    python_callable=lambda: None,
    params={
        "_callbacks": {
            "on_failure": {
                "type": "slack",
                "connection_id": "slack_alerts_webhook",  # must be auth_type="webhook"
            }
        }
    },
)
```

---

### 44. HTTP webhook alerts

**Description:** A task-level callback of `{"type": "http_webhook", "url":
"...", "headers": {...}, "body": "..."}` POSTs to any custom URL. If `body`
is set, it's sent as the literal request body (after token replacement); if
omitted, a default JSON `AlertPayload` (`dag_id`, `task_id`, `run_id`,
`map_index`, `event`, `message`, `timestamp`) is sent instead.

**Edge cases:**
- **`url` and `body` both get the same 4-token replacement as Email**
  (`{{dag_id}}`, `{{task_id}}`, `{{run_id}}`, `{{event}}` — feature 42's
  caveat applies identically here) — you can parameterize the URL path
  itself (e.g. `.../hooks/{{dag_id}}`), not just the body.
- **No `connection_id` / auth support** — unlike Slack/PagerDuty, this
  channel has no connection lookup at all; any auth (bearer token, API key)
  must be baked directly into the `headers` dict in the callback config,
  which is stored in plaintext in `callback_request.callback_config` (not
  AES-encrypted like a `connection` row's password).
- **Success is any 2xx status** — `resp.StatusCode < 200 || >= 300` is
  treated as failure and logged; there's no configurable "expected status"
  the way `HttpSensor` allows (feature covered separately under Sensors).
- If you supply a custom `body` that isn't valid JSON, PI-Flow still sends
  it as the raw request body with `Content-Type: application/json` — the
  dispatcher doesn't validate that `body` is actually JSON before sending.
- Same 10-second request timeout as every other alert channel (Slack,
  PagerDuty) — a slow endpoint fails the alert delivery rather than hanging
  the dispatcher's poll loop.

**Code snippet:**
```python
notify_failure = PythonOperator(
    task_id="risky_step",
    python_callable=lambda: None,
    params={
        "_callbacks": {
            "on_failure": {
                "type": "http_webhook",
                "url": "https://ops.company.com/hooks/{{dag_id}}",
                "headers": {"Authorization": "Bearer secret-token-value"},
                "body": '{"dag": "{{dag_id}}", "task": "{{task_id}}", "event": "{{event}}"}',
            }
        }
    },
)
```

---

### 45. PagerDuty alerts

**Description:** A task-level callback of `{"type": "pagerduty",
"connection_id": "..."}` triggers a PagerDuty Events API v2 incident, with
`severity` auto-mapped from the callback event: `on_failure_callback` →
`critical`, `on_retry_callback` → `warning`, `on_success_callback` → `info`,
anything else → `error`.

**Edge cases:**
- **There is no dedicated "PagerDuty" connector type in the Connections
  UI** — the registered connector list is snowflake/databricks/mysql/
  postgres/redshift/s3/gcs/bigquery/slack/ssh only. A PagerDuty connection
  must be created directly via the API (`POST /api/connections` with an
  arbitrary `connection_type`, e.g. `"pagerduty"`) and its routing key placed
  in `extra.routing_key` — there's no UI form/validation/"Test Connection"
  button for it the way the 10 registered types get.
- **Routing key resolution has a fallback: `extra.routing_key`, else the
  connection's `password` field.** If `extra.routing_key` is absent or
  empty, the dispatcher falls back to decrypting the connection's regular
  `password` column as the routing key — meaning you can alternatively store
  the PagerDuty routing key as if it were a "password" on a generic
  connection row, if that's more convenient than crafting `extra` JSON by
  hand.
- **Severity is fixed by event name and not configurable** — you cannot
  send a `critical` PagerDuty event from an `on_success_callback`; the
  mapping (`pagerDutySeverity`) is hardcoded. If you need custom severity
  logic, use the HTTP webhook channel (feature 44) directly against
  PagerDuty's Events API instead.
- The incident's `summary` and `custom_details` are auto-generated
  (`"PiFlow: {event} on {dag_id}.{task_id}"` plus dag/task/run IDs) — like
  Slack, there's no way to inject a custom message into the PagerDuty
  payload via the callback config.
- Every trigger uses `event_action: "trigger"` — there's no built-in
  "resolve" callback to auto-close the PagerDuty incident when a
  subsequently-retried task later succeeds; you'd need a separate mechanism
  (e.g. a downstream task calling the Events API with `event_action:
  "resolve"`) for that.

**Code snippet:**
```python
notify_failure = PythonOperator(
    task_id="risky_step",
    python_callable=lambda: None,
    params={
        "_callbacks": {
            "on_failure": {
                "type": "pagerduty",
                "connection_id": "pagerduty_prod",  # extra.routing_key or password field
            }
        }
    },
)
```

---

### 46. Event scope

**Description:** Callbacks can be scoped to fire on different task/DAG
outcomes. At the **task** level, the four events are `on_success`,
`on_failure`, `on_retry`, `on_skipped` (keys inside `params={"_callbacks":
{...}}}`, matching `on_success_callback`/etc. semantically). At the **DAG**
level (feature 9), only `on_success_callback`, `on_failure_callback`, and
`on_sla_miss_callback` exist — there is no DAG-level retry or skip event.

**Edge cases:**
- **Verified: `on_failure` fires on EVERY failed attempt, not just the final
  one.** `task_runner.go`'s `StatusFailed` case calls `handleFailure` (which
  decides `up_for_retry` vs terminal `failed`) **and then unconditionally**
  fires `on_failure_callback` regardless of which one it picked. With
  `retries=3`, a task that fails all 4 attempts fires its `on_failure`
  callback **4 times** — once per attempt — not once at the end. Don't build
  alerting logic that assumes "one failure event = one final failure."
- **Verified: `on_retry` is effectively dead today for standard
  operators.** It only fires on the Go-internal `StatusRetry` result, which
  is reachable in code (`executor_python.go` maps a `{"status": "retry"}`
  response to it) — but nothing in `_run_task.py` (or any other executor)
  ever actually emits `{"status": "retry"}`. In practice, every normal
  retryable failure goes through the `on_failure`-firing path above instead;
  `on_retry` is reserved plumbing with no current producer.
- **`on_skipped` only fires for a task's own terminal skip** (raising
  `PiFlowSkip`, feature 34, or a soft-fail sensor timeout) — it does **not**
  fire for a task skipped by branch-evaluation (feature 33) or by
  trigger-rule skip-propagation (feature 14/31). Those paths update
  `task_instance.state` directly via a raw SQL `UPDATE` in the scheduler
  (`branch_evaluator.go`), bypassing `task_runner.go`'s callback-firing logic
  entirely — a task that never actually executed has no callback event at
  all, by design.
- **DAG-level event scope is narrower than task-level on purpose** — there's
  no DAG-level "some task retried" or "some task skipped" callback; if you
  need alerting on a partial-failure DAG shape, attach task-level callbacks
  to the specific tasks you care about, or add a dedicated notifier task
  (feature 21's workaround) with `trigger_rule="one_failed"` /
  `"all_done"`.
- Every event above fires **per task instance** (including per `map_index`
  for a mapped task) — there is no built-in "fire once for the whole mapped
  group" aggregation; a 100-way `.expand()` with 3 failing slices fires 3
  separate `on_failure` events (times however many attempts each took).

**Code snippet:**
```python
with DAG(dag_id="alerting_scope_demo", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    flaky = PythonOperator(
        task_id="flaky_step",
        python_callable=lambda: None,
        retries=3,
        params={
            "_callbacks": {
                # Fires on EVERY failed attempt (verified), not just the final one
                "on_failure": {"type": "slack", "connection_id": "slack_alerts_webhook"},
                # Fires only if THIS task raises PiFlowSkip itself
                "on_skipped": {"type": "email", "to": ["team@company.com"], "subject": "skipped"},
            }
        },
    )

    # DAG-wide "final outcome" alert needs a dedicated task, not a DAG-level callback
    # (feature 9's gap) or an unreliable retry-count assumption:
    notify_final = SlackAPIPostOperator(
        task_id="notify_final",
        slack_conn_id="slack_alerts_webhook",
        text="alerting_scope_demo finished: check grid view for final status",
        trigger_rule="all_done",
    )

    flaky >> notify_final
```

---

## Category: Sensors & waiting

### 47. HttpSensor

**Description:** `HttpSensor(endpoint=..., method="GET", ...)` polls an HTTP
endpoint until it returns a 2xx status code, optionally also requiring the
response body to match `response_check_regex`.

**Edge cases:**
- **`http_conn_id`/`connection_id` is accepted, stored, and completely
  ignored at execution time.** The Python class resolves and stores a
  connection id, but `checkHTTP` in `executor_sensor.go` never reads
  `params.ConnectionID` at all — there is no connection-based auth injection
  for HttpSensor. Any authentication (bearer token, API key, basic auth
  header) must be inlined yourself via `headers={...}` or a hardcoded query
  string in `endpoint`.
- **A network/DNS error is treated as "condition not met," not a poke
  error** — `checkHTTP` returns `false, nil` on any request failure (same
  design as `HttpTrigger`, feature 28), so a typo'd hostname silently retries
  every `poke_interval` until the overall `timeout` is hit rather than
  failing fast with a clear "could not resolve host" message.
- **`response_check_regex` compiles with Go's RE2 engine (`regexp.MatchString`),
  not Python's `re`.** Patterns using lookahead/lookbehind or backreferences
  (valid Python regex) fail to compile here — surfacing as a poke error, but
  only once a 2xx response is actually received (a regex typo won't show up
  until the endpoint starts responding successfully).
- Response body is read up to 1MB for the regex check (same truncation as
  feature 38's HTTP auto-XCom) — a match target beyond that limit can never
  succeed.
- **`mode` defaults to `"poke"`** (inherited straight from `BaseSensorOperator`,
  with no HttpSensor-specific override) — unlike Sql/Time/ExternalTask
  sensors, which all default to `"reschedule"` (feature 52). An HttpSensor
  you don't explicitly set `mode=` on holds a worker slot for its entire wait.

**Code snippet:**
```python
from dag_parser.dynamic.operators import HttpSensor

wait_for_api = HttpSensor(
    task_id="wait_for_api",
    endpoint="https://api.example.com/v1/status",
    headers={"Authorization": "Bearer hardcoded-token-here"},  # connection_id is NOT wired up
    response_check_regex=r'"status"\s*:\s*"ready"',
    poke_interval=30,
    timeout=1800,
    mode="reschedule",   # override the poke default explicitly
    soft_fail=True,      # timeout -> skipped, not failed
)
```

---

### 48. SqlSensor

**Description:** `SqlSensor(sql=..., connection_id=...)` runs a SQL query on
every poke and succeeds once the result has at least one row whose first
column is truthy.

**Edge cases:**
- **Verified, critical: `connection_id`/`conn_id` is accepted and stored on
  the task, but the Go sensor executor completely ignores it — every
  SqlSensor query runs directly against PI-Flow's OWN internal Postgres
  metadata database.** `NewSensorExecutor` is constructed with
  `opts.PostgresDB` (`executor.go`) — the exact same connection pool backing
  `dag_run`/`task_instance`/etc. — not a lookup keyed by `connection_id` the
  way SQL executor tasks (feature 38) or Snowflake/Sql executors resolve a
  target database. There is currently **no way to point a SqlSensor at an
  external database** (Snowflake, MySQL, your own Postgres) despite the
  Airflow-parity constructor signature suggesting otherwise. Practical
  consequence: only use `sql=` here to query PI-Flow's own `metadata` schema
  tables (and even then, `ExternalTaskSensor`, feature 50, is the intended,
  purpose-built way to wait on another DAG/task's state) — for waiting on an
  actual external data condition, use a Python task with your own polling
  loop (or `self.defer(HttpTrigger(...))` against a small check endpoint)
  instead.
- **Truthiness rules for the first cell:** `NULL`, `0`, `""`, `"0"`, and
  case-insensitive `"false"` are falsy; any other non-null value (any other
  non-empty string, non-zero number, or non-scalar/JSON type) is truthy. A
  query returning **zero rows** is also falsy — indistinguishable in
  behavior from a query that returns a row with a falsy first column.
- A malformed/invalid SQL string is a genuine poke **error**, not "condition
  not met" — in poke mode it's silently retried every interval (logged as a
  warning) until `timeout`; in reschedule mode the same error only surfaces
  once cumulative `timeout` is reached. There's no separate "bad SQL, fail
  immediately" behavior.
- **`mode` defaults to `"reschedule"`** (explicit override in `SqlSensor.__init__`,
  unlike HttpSensor's inherited `"poke"` default — feature 52).

**Code snippet:**
```python
from dag_parser.dynamic.operators import SqlSensor

# NOTE: this SQL runs against PI-Flow's OWN internal Postgres, not an
# external database, regardless of connection_id — see edge case above.
wait_for_flag = SqlSensor(
    task_id="wait_for_flag",
    sql="SELECT value FROM variable WHERE key = 'ready_flag'",
    connection_id="unused_today",   # accepted but ignored — do not rely on this
    poke_interval=60,
    timeout=3600,
    mode="reschedule",
)
```

---

### 49. TimeSensor

**Description:** `TimeSensor(target_time="HH:MM")` (or `"HH:MM:SS"`) waits
until the current wall-clock time reaches or passes `target_time`.

**Edge cases:**
- **Verified: `target_time` is evaluated against the container's own local
  system clock (`time.Now()`), NOT the DAG's configured `timezone`
  (feature 3).** `checkTime` in `executor_sensor.go` never reads
  `task.Timezone` at all — unlike Go-templated `{{ .DS }}`/`{{ .TS }}`
  (feature 39), which explicitly localize to the DAG's timezone. A DAG
  declared `timezone="America/New_York"` with `TimeSensor(target_time="09:00")`
  actually waits for 09:00 in whatever timezone the **container** runs in
  (`TZ` env var, doc 05) — likely UTC by default, not US Eastern. Convert
  your intended local time to the container's timezone yourself, or align
  `TZ` deployment-wide with your DAGs' timezone.
- **Always targets TODAY's date, not the run's `execution_date`.** The
  condition builds `time.Date(now.Year(), now.Month(), now.Day(),
  targetHour, ...)` using the CURRENT date at poke time — a backfill run
  "representing" a date in the past still waits for `target_time` on
  whatever day it actually executes, not the historical date it's backfilling.
- **No "already past today" wait-until-tomorrow logic** — if a run starts at
  14:00 and `target_time="09:00"`, the condition is satisfied immediately on
  the first poke (`now.After(target)`); it does not wait for the next day's
  09:00.
- **`mode` defaults to `"reschedule"`** (like Sql/ExternalTask) — cheap to
  leave waiting for hours (e.g. "wait until market open") since it only
  claims a worker slot for each brief poll, not the whole intervening period.

**Code snippet:**
```python
from dag_parser.dynamic.operators import TimeSensor

# TZ env var / container clock must actually be the timezone you mean —
# this does NOT read the DAG's own `timezone=` setting.
wait_until_market_open = TimeSensor(
    task_id="wait_until_market_open",
    target_time="09:30:00",
    poke_interval=60,
    timeout=3600 * 2,
    mode="reschedule",
)
```

---

### 50. ExternalTaskSensor

**Description:** `ExternalTaskSensor(external_dag_id=..., external_task_id=...)`
waits for a task (or, if `external_task_id` is omitted, an entire DAG run)
in another DAG to reach one of `allowed_states` (default `["success"]`),
failing fast if it reaches one of `failed_states`.

**Edge cases:**
- **Without `execution_date`, it always resolves to the LATEST run of the
  external DAG/task** (`ORDER BY execution_date DESC LIMIT 1`), not a run
  computed to correspond with your own DAG's logical date. If your DAG runs
  hourly and the external DAG runs daily, every hourly run checking without
  `execution_date` ends up polling the SAME latest daily run — pass
  `execution_date` (an ISO string) explicitly if you need a specific,
  matched run.
- **`failed_states` is checked before `allowed_states` and short-circuits
  with a distinct "fail fast" path** (`errExternalTaskFailed`) rather than
  just returning "not met yet." Downstream, `soft_fail` (feature 51) still
  governs whether this ends the WAITING task as `failed` or `skipped` —
  there's no way to tell from the resulting state alone whether the sensor
  timed out versus the external target actively failed; only the log
  message differs.
- **If the external state matches neither `allowed_states` nor
  `failed_states`** (e.g. it's `running`), the sensor just keeps waiting —
  there's no special "stuck forever" handling beyond the sensor's own
  `timeout`.
- **`external_task_id` omitted → checks the DAG RUN's own `state` column**,
  not an aggregate/derived status of its tasks. A run whose tasks have all
  finished but hasn't yet been flipped to `success` by the scheduler's
  finalizer phase (doc 02, Phase 4) will not satisfy `allowed_states=["success"]`
  until that finalization actually happens — there can be a real (if usually
  short) lag between "tasks look done" and "run state says success."
- Matching against `allowed_states`/`failed_states` is case-insensitive
  (`strings.EqualFold`) — `"Success"` and `"success"` are treated the same,
  so a typo'd case isn't a silent no-match trap the way some other
  string-matched configs in PI-Flow are (contrast with feature 26's
  case-sensitive `trigger_type`).

**Code snippet:**
```python
from dag_parser.dynamic.operators import ExternalTaskSensor

wait_for_upstream_dag = ExternalTaskSensor(
    task_id="wait_for_upstream_dag",
    external_dag_id="daily_ingest",
    external_task_id="load_final_table",   # omit to wait on the whole DAG run instead
    allowed_states=["success"],
    failed_states=["failed", "upstream_failed"],
    execution_date="{{ ds }}T00:00:00",    # pin to a specific run rather than "latest"
    poke_interval=60,
    timeout=3600,
    mode="reschedule",
)
```

---

### 51. Sensor tuning

**Description:** Three knobs common to every sensor: `poke_interval`
(seconds between checks, default 60), `timeout` (total seconds before
giving up, default 3600), and `soft_fail` (turn a timeout/failure into
`skipped` instead of `failed`).

**Edge cases:**
- **`timeout` is cumulative across the sensor's ENTIRE wait, including
  across every reschedule — not per-poke and not reset each time.** The Go
  code explicitly preserves the sensor's original `start_date` across every
  `up_for_reschedule → scheduled → queued → running` cycle
  (`COALESCE(start_date, NOW())` on claim) and compares `time.Since(task.StartDate)`
  against `timeout` on every poke (a deliberate guard, commented `L3-00b` in
  `executor_sensor.go`, against a naive per-execution deadline that would
  never fire in reschedule mode). Don't assume switching to reschedule mode
  gives you a fresh timeout clock each time.
- **`soft_fail` only changes TWO outcomes: sensor timeout, and (for
  ExternalTaskSensor) the external target entering a failed state.** A
  genuine poke error — bad SQL, an uncompilable regex, a missing required
  param — still fails the task outright (in poke mode) or after `timeout`
  (in reschedule mode) regardless of `soft_fail`; it doesn't turn every
  possible sensor problem into a skip.
- **No built-in minimum on `poke_interval` or backoff/jitter.** Setting
  `poke_interval=1` on a `mode="poke"` HTTP/SQL sensor hammers the target
  every second for the entire `timeout` window with a fixed cadence — there's
  no automatic backoff the way retry delays (feature 13) can grow
  exponentially.
- `timeout` and `poke_interval` fall back to the Go-side defaults (3600s,
  60s) whenever the value is missing or `<= 0` — this is enforced uniformly
  in `executor_sensor.go`, independent of whatever the Python constructor's
  own default was, so an explicitly-passed `poke_interval=0` silently
  becomes 60, not an immediate/continuous poll.

**Code snippet:**
```python
tight_poll = HttpSensor(
    task_id="tight_poll",
    endpoint="https://api.example.com/v1/status",
    poke_interval=15,     # checked every 15s...
    timeout=600,          # ...for up to 10 minutes total, cumulative
    soft_fail=True,       # timeout -> skipped (downstream must tolerate this via trigger_rule)
)
```

---

### 52. Poke vs reschedule

**Description:** `mode="poke"` (the base-class default) holds the worker
slot for the sensor's entire wait, looping with an in-process sleep between
checks. `mode="reschedule"` does exactly one check per dispatch, then — if
not met — releases the slot entirely; the scheduler's sensor rescheduler
(doc 02, Phase 1.6) re-queues it once `poke_interval` elapses.

**Edge cases:**
- **The four built-in sensors do NOT share a common default mode.**
  `HttpSensor` inherits `mode="poke"` straight from `BaseSensorOperator`
  with no override — but `SqlSensor`, `TimeSensor`, and `ExternalTaskSensor`
  each explicitly hardcode their own default to `mode="reschedule"` in their
  `__init__`. If you don't set `mode=` yourself, an `HttpSensor` behaves
  fundamentally differently under load (holds a slot for the whole wait)
  than the other three (releases it between checks) — verify each type's
  actual default rather than assuming consistency.
- **`mode="poke"` occupies the Sensor admission class for the FULL wait
  duration** (up to `timeout`) — cheap on Postgres (one claim, one running
  row, no re-dispatch churn) but ties up a live worker goroutine the whole
  time. Sensor-class tasks are only blocked at `Critical` admission load
  (doc 03/10), so this is more lenient than `ClassLocal`, but it's still a
  held resource for potentially a very long wait.
- **`mode="reschedule"` re-claims the task on every single poke, which
  increments `try_number` every time** (`claimTask`'s `try_number = try_number
  + 1` runs on every `queued → running` transition, including every
  reschedule cycle) — even though none of these pokes consume the task's
  `retries` budget (the retry-count check in `handleFailure` only runs on
  the `StatusFailed` path, never on `StatusUpForReschedule`). Don't
  interpret a large `try_number` on a reschedule-mode sensor as "it failed
  and retried N times" — it just means it's been poked N times.
- Both modes share the exact same cumulative `timeout` semantics
  (feature 51) — the mode only changes how worker resources are occupied
  while waiting, not how long the sensor is willing to wait overall.

**Code snippet:**
```python
# poke: holds the worker slot the whole time (HttpSensor's default — fine for short waits)
quick_check = HttpSensor(task_id="quick_check", endpoint="https://api.example.com/health",
                          poke_interval=10, timeout=120)   # mode="poke" implicitly

# reschedule: releases the slot between checks (better for long waits)
long_wait = HttpSensor(task_id="long_wait", endpoint="https://api.example.com/status",
                        poke_interval=300, timeout=3600 * 6, mode="reschedule")
```

---

### 53. Deferrable/async

**Description:** Same underlying mechanism as feature 28
(`self.defer(trigger, method_name, timeout)`), covered again here because it
represents a **third** waiting strategy alongside poke/reschedule: a deferred
task consumes **zero** worker-pool resources while waiting — it isn't
dispatched, claimed, or admission-checked at all until its `trigger_instance`
row fires. The triggerer (a completely separate polling loop, doc 02 §6)
owns the entire wait; the worker pool never sees the task again until then.

**Edge cases:**
- See feature 28 for the full trigger-type breakdown (`TimeDeltaTrigger`,
  `DateTimeTrigger`, `HttpTrigger`) and their individual gotchas (naive
  datetime treated as UTC, `HttpTrigger` retrying forever without an
  explicit `timeout`, etc.).
- **None of the four built-in sensors (features 47-50) actually use
  `defer()` — they're all implemented as Go poke/reschedule executors, not
  deferrable Python operators.** There is no `deferrable=True` kwarg on
  `HttpSensor`/`SqlSensor`/`TimeSensor`/`ExternalTaskSensor` to get the
  "zero worker footprint" behavior for free. To actually get it for an
  HTTP- or time-based wait, you must write your own `BaseOperator` subclass
  and call `self.defer(...)` yourself (per feature 28's snippet) — the
  built-in sensors and the defer mechanism are two independent, non-composable
  waiting systems today.
- **`soft_fail` has no equivalent here.** A deferred trigger that times out
  (per the `timeout=` passed to `defer()`, not any sensor `timeout` param) is
  handled directly by the triggerer: it marks the trigger fired, fails the
  task, and inserts a DLQ row — there's no built-in "timeout as skip" option
  for a deferred wait the way sensors get via `soft_fail`.
- **`execute_complete(self, event=None)` is where the fired trigger's result
  is handled** — the `BaseOperator` default is a no-op `pass`. If your custom
  deferrable operator doesn't override it, the task simply succeeds silently
  the moment the trigger fires, without inspecting whatever the trigger
  actually returned.
- Because the task is fully out of the worker pool while deferred, none of
  the admission-class distinctions (Local/Remote/Sensor, doc 03/10) apply to
  it during the wait — it's not competing for `MaxLocalTasks` or any pool
  slot at all until the trigger fires and it re-enters as a normal
  `scheduled` task.

**Code snippet:**
```python
from dag_parser.dynamic.dag_context import BaseOperator, HttpTrigger

class DeferredHealthCheck(BaseOperator):
    """Waits for an endpoint to go healthy WITHOUT holding a worker slot
    (contrast with HttpSensor, feature 47, which always holds/reschedules
    through the worker pool)."""
    operator_name = "DeferredHealthCheck"

    def execute(self, context):
        self.defer(
            trigger=HttpTrigger(endpoint="https://api.example.com/health", expected_status=200),
            method_name="execute_complete",
            timeout=1800,   # triggerer fails the task if this isn't reached
        )

    def execute_complete(self, event=None):
        print(f"endpoint became healthy: {event}")
```

---

## Category: SLA & reliability

> Note: the individual DAG-file knobs for these three features are already
> covered in depth — DAG-level SLA is feature 7, task-level SLA is feature
> 17, DAG run timeout is feature 6, task execution timeout is feature 16,
> and retry policy/backoff is feature 13. This category exists in the
> catalog as the "system reliability" cross-cut, so the three entries below
> focus on the **background subsystems** that actually enforce these knobs
> (`SLAMonitor`, `ZombieDetector`, `StaleRunCleaner`, `RetryRescheduler` —
> doc 02 §7) and verified behavior that only shows up at that level.

### 54. Task/DAG SLA

**Description:** A single scheduler subsystem, `SLAMonitor`, evaluates both
DAG-level (`expected_duration_seconds`, feature 7) and task-level
(`sla_seconds`, feature 17) SLA misses every iteration, batched (up to 100
DAG-level candidates per tick), and dedups DAG-level misses via
`dag_run.sla_miss_fired` and task-level misses via a unique
`(dag_id, run_id, task_id)` index on `sla_miss` (`ON CONFLICT DO NOTHING`).

**Edge cases:**
- **The two SLA clocks measure elapsed time from different reference
  points, and it's easy to conflate them.** DAG-level elapsed is
  `NOW() - dag_run.start_date` — actual wall-clock run start. Task-level
  elapsed (feature 17) is measured from the run's **logical date**, not the
  task's own start or the run's actual start. A DAG that queues for a while
  before actually starting (e.g. waiting on `max_active_runs`, feature 5)
  has task-level SLA budgets already partially consumed by the delay before
  the run even begins, while the DAG-level SLA clock only starts once the
  run genuinely starts.
- **Both levels resolve their alert callback through the exact same helper,
  with the exact same priority: `on_sla_miss_callback` if set, else
  `on_failure_callback`, else nothing dispatched (miss still recorded).**
  This is one shared code path (`resolveCallbackConfig`), not two
  independent implementations — so the callback gap described in features
  7/17 (payload wrapped as `{"callback": ..., "context": {...}}` with no
  top-level `"type"`, meaning `CallbackDispatcher` never actually sends
  anything) applies identically and for the identical reason at both levels.
- **A DAG-level SLA miss with no callback configured at all is silently a
  no-op beyond the log line** — `processSLAMiss` explicitly returns early
  ("no callback configured for SLA miss, skipping dispatch") without even
  creating a `callback_request` row. The **detection** itself (the fact that
  it breached SLA) is still permanently recorded via `sla_miss_fired`/the
  `sla_miss` table regardless — queryable later via landing-time analytics
  — even when no notification of any kind was attempted.
- Both checks only evaluate DAG runs currently in `running` state — a run
  that already finished (successfully or not) before its SLA monitor tick
  can never retroactively be flagged, even if it technically ran past its
  `expected_duration_seconds` right before finishing.

**Code snippet:**
```python
with DAG(
    dag_id="revenue_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    expected_duration_seconds=600,   # DAG-level: elapsed from ACTUAL run start_date
) as dag:

    critical_step = PythonOperator(
        task_id="critical_step",
        python_callable=lambda: None,
        sla=120,   # task-level: elapsed from the run's LOGICAL date, not start_date
    )
```

---

### 55. Timeouts

**Description:** Three independent layers enforce "don't run forever":
task-level `execution_timeout` (feature 16, via `context.WithTimeout` +
SIGTERM/SIGKILL), DAG-level `dagrun_timeout_seconds` (feature 6, strict
wall-clock via `StaleRunCleaner`), and a global 24h fallback (also
`StaleRunCleaner`) for runs with no explicit `dagrun_timeout_seconds` at
all — but only if truly abandoned.

**Edge cases:**
- **The zombie detector (heartbeat staleness, `ZombieDetector`) closes a gap
  the timeout layers don't cover on their own: a worker that dies BEFORE a
  task's first heartbeat flush.** In addition to the documented "stale
  heartbeat" check (>120s default since the last heartbeat), it separately
  catches tasks stuck `running` with `heartbeat_at IS NULL` whose
  `start_date` is already older than the same threshold — closing the case
  where a worker crashes (e.g. an uncaught panic) before ever emitting its
  first heartbeat, which would otherwise linger `running` until the much
  slower 24h global stale-run fallback caught it.
- **Verified: force-failing a stale DAG run does NOT force-fail every kind
  of lingering task instance — only ones in `running`, `scheduled`,
  `queued`, or `up_for_retry`.** A task instance sitting in `deferred`
  (feature 53), `up_for_reschedule` (feature 52), or `waiting_for_child`
  (feature 27) is explicitly **excluded** from `StaleRunCleaner`'s
  force-fail `UPDATE`. Practical consequence: if a run times out
  (`dagrun_timeout_seconds` or the global 24h fallback) while its only
  in-flight task is a deferred trigger or a reschedule-mode sensor, the
  **run** flips to `failed`, but that **task instance is left behind**,
  orphaned in its non-terminal state — it won't be picked up by any
  reconciliation automatically and needs a manual clear/mark action.
- **The global 24h fallback only fires when NO task in the run is actively
  heartbeating** — a legitimately long-running but healthy DAG (every task
  still progressing, heartbeats fresh) is never force-failed by the global
  threshold, only by an explicit `dagrun_timeout_seconds` you set yourself
  (feature 6). The global fallback exists purely to catch genuinely
  abandoned runs, not to cap normal long-running work.
- All three layers write a `dead_letter_task` row on force-fail (zombie
  reap, stale-run cleanup, or a task's own retries-exhausted path) — the DLQ
  is a converging point regardless of which layer actually caught the
  problem, so DLQ review (doc 07/08's "Dead-letter queue" feature) is a
  reasonable single place to look for any of these failure modes.

**Code snippet:**
```python
with DAG(
    dag_id="nightly_etl",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    dagrun_timeout_seconds=3600 * 4,   # explicit strict run-level timeout (feature 6)
) as dag:

    # A deferred/sensor task left in-flight when the run above times out
    # is NOT force-failed itself (see edge case) — only the run is.
    long_wait = HttpSensor(
        task_id="long_wait",
        endpoint="https://api.example.com/status",
        mode="reschedule",
        timeout=3600 * 6,   # longer than the DAG's own dagrun_timeout — a real risk
    )

    process = PythonOperator(
        task_id="process",
        python_callable=lambda: None,
        execution_timeout=1800,   # task-level cap, independent of the run-level one
    )

    long_wait >> process
```

---

### 56. Retry backoff

**Description:** Covered in depth at feature 13 (fixed vs exponential delay,
`±10%` jitter, `max_retry_delay_seconds` cap) for the normal
worker-driven retry path. The scheduler's `RetryRescheduler` (Phase 1.5)
is the other half: a bulk, batched sweep that simply promotes
`up_for_retry` tasks whose `next_retry_at` has passed back to `scheduled`,
capped at 500 per tick (configurable) so a mass-retry event doesn't flood a
single scheduler iteration — excess due tasks roll to the next tick a few
seconds later, not lost.

**Edge cases:**
- **Verified, significant: a task recovered by the ZOMBIE detector (its
  worker died mid-run) always retries with the FLAT `retry_delay_seconds`,
  completely ignoring `retry_exponential_backoff`/`max_retry_delay_seconds`
  — even if the task is configured for exponential backoff.**
  `ZombieDetector.getTaskRetries` only reads the `retries` and
  `retry_delay_seconds` columns from `task` — it never looks at
  `retry_exponential_backoff` or `max_retry_delay_seconds` at all, unlike
  `task_runner.go`'s `computeRetryDelay` (feature 13), which is the only
  place that actually implements the exponential math. Practical
  consequence: a task with `retries=5, retry_exponential_backoff=True,
  retry_delay_seconds=30, max_retry_delay_seconds=600` backs off correctly
  (30s, 60s, 120s, ...) when it fails normally through its own Python/Bash
  process — but if attempt #3 dies because the **worker itself** crashed
  (zombie-reaped) rather than the task failing cleanly, that particular
  retry is scheduled at a flat 30s delay regardless of which attempt number
  it is, silently breaking the backoff curve for that one hop.
- **Jitter (±10%) is computed once, at the moment `computeRetryDelay` runs**
  (a normal failure) — it is never applied to a zombie-reaped retry's flat
  delay, and it's not re-randomized or otherwise touched by
  `RetryRescheduler` itself (which only compares `next_retry_at <= now`,
  doing no delay computation of its own).
- **The 500-per-tick batch cap is a dispatch-pressure safety valve, not a
  correctness limit** — if a transient outage fails 2,000 tasks
  simultaneously (all becoming `up_for_retry` with similar `next_retry_at`
  values), only 500 are promoted to `scheduled` per scheduler tick; the
  remaining 1,500 wait one or more additional ~30s poll intervals (default
  `SCHEDULER_POLL_INTERVAL_SECS`) before they're all eventually rescheduled
  — not silently dropped, just naturally staggered.
- Retry backoff and DAG-level `dagrun_timeout_seconds` (feature 6/55)
  interact: a task with a long exponential backoff chain
  (`max_retry_delay_seconds` set high) can easily outlive its own DAG run's
  timeout — the run gets force-failed by `StaleRunCleaner` mid-backoff,
  and (per feature 55's edge case) an `up_for_retry` task instance IS
  included in that force-fail's state list, so a task waiting out its
  backoff delay when the run times out is correctly swept into `failed`
  too, unlike the deferred/reschedule/waiting_for_child gap noted in
  feature 55.

**Code snippet:**
```python
resilient_task = PythonOperator(
    task_id="resilient_task",
    python_callable=lambda: None,     # respected on a normal task failure...
    retries=5,                        # ...but IGNORED if this attempt is zombie-reaped
    retry_delay_seconds=30,           # instead (worker crash) — that retry uses a
    retry_exponential_backoff=True,   # flat 30s delay regardless of attempt number.
    max_retry_delay_seconds=600,
)
```

---

## Category: Operators / integrations

### 57. Compute & scripts

**Description:** The core scripting/compute layer: `BashOperator` (shell
commands), `PythonOperator` (Python callables), `ExternalPythonOperator`
(pre-provisioned interpreter), `PythonVirtualenvOperator` (managed venv),
and `BranchPythonOperator` (conditional path selection). All five run under
the `ClassLocal` admission class (doc 03/10) — CPU-heavy, subject to
`MaxLocalTasks`. Deep dives on each already exist: retries (13), env
control (22), voluntary skip (34), branching (33), dynamic mapping (32),
Go/Jinja templating (39/40).

**Edge cases:**
- **`BashOperator` runs via `bash -c "<your command>"` with NO explicit
  working directory set** — `executor_bash.go`'s `exec.CommandContext`
  never sets `cmd.Dir`, so the command's cwd is whatever directory the
  go-core **process itself** was launched from (typically `/app` per the
  Dockerfile, doc 05) — NOT the DAGs repo checkout (`$DAGS_REPO_PATH`) and
  NOT any per-task scratch directory. A `bash_command` using relative paths
  (`./script.sh`, `cat data.csv`) will look in the wrong place; use
  `cd $DAGS_REPO_PATH/... && ...` explicitly, or absolute paths, if you need
  to reference files checked out from Git.
- `ExternalPythonOperator`/`PythonVirtualenvOperator` both re-import the DAG
  module fresh inside the target interpreter (no Airflow-style
  dill/pickle serialization) — the target interpreter/venv must be able to
  `import dag_parser` itself, or skip/defer degrade to no-ops (feature 22's
  caveat, same underlying constraint for both).
- `BranchPythonOperator` is the only one of the five that mutates the
  scheduling graph (skip-cascading, feature 33) — the other four are purely
  "run and report success/failure," with no special scheduler-side
  post-processing of their own.
- All five share the exact same env-isolation model (feature 22) — none of
  them inherit orchestrator secrets regardless of which one you pick.

**Code snippet:**
```python
# BashOperator: cwd is the orchestrator's own process dir, NOT the dags repo
fix_path_example = BashOperator(
    task_id="fix_path_example",
    bash_command="cd $DAGS_REPO_PATH/dags && ls -la",  # explicit cd required
)
```

---

### 58. SQL databases

**Description:** `SnowflakeOperator`, `SQLExecuteQueryOperator`,
`MySqlOperator`, `PostgresOperator`, and `RedshiftOperator` run a `sql`
string against a named `connection_id`, with a `parameters` dict meant to
bind values into the query.

**Edge cases:**
- **Verified, critical bug: `SQLExecuteQueryOperator`, `MySqlOperator`,
  `PostgresOperator`, and `RedshiftOperator` are all non-functional as
  written — instantiating ANY of them in a DAG file crashes DAG parsing.**
  `SQLExecuteQueryOperator.__init__` in `dag_context.py` declares a required
  positional `python_callable` parameter and never declares `sql`,
  `connection_id`, `conn_id`, or `parameters` as parameters at all — yet its
  body references all four as if they were in scope. `MySqlOperator` /
  `PostgresOperator` / `RedshiftOperator` each call
  `super().__init__(task_id, sql=sql, connection_id=conn, **kwargs)` without
  ever passing `python_callable`, so calling any of them raises `TypeError:
  __init__() missing 1 required positional argument: 'python_callable'`
  immediately — before even reaching the `NameError` that a hypothetical
  correct call would hit on `sql`/`connection_id` inside
  `SQLExecuteQueryOperator` itself. This is a genuine, confirmed defect in
  the current parser (`sandbox_runner.py` imports these same broken classes
  directly, with no fix or override) — **do not use `SQLExecuteQueryOperator`,
  `MySqlOperator`, `PostgresOperator`, or `RedshiftOperator` in any DAG
  file today**; the whole file will fail to parse/ingest
  (`dag_parse_failure`, doc 01). `SnowflakeOperator` is a separate,
  self-contained class and is **not** affected — it works correctly.
- **Verified, security-relevant: `parameters` is naive `{{key}}` string
  substitution into the raw SQL text, NOT real parameterized-query
  binding.** Both `executor_sql.go` and `executor_snowflake.go` do
  `strings.ReplaceAll(sqlText, "{{"+k+"}}", v)` — literally splicing the
  string value directly into the query — there is no prepared-statement
  placeholder, no type coercion, and no escaping/quoting. If any
  `parameters` value ever derives from user-controlled input (e.g. a
  manually-triggered run's `conf`, feature 24), this is a direct SQL
  injection vector; you must quote/escape values yourself before passing
  them as `parameters`, exactly as if you were building the SQL string by
  hand.
- Only the first result row is captured as `return_value` (feature 38) —
  a query returning many rows silently drops everything after row 1.
- `SnowflakeOperator` maintains a per-`connection_id` cached connection
  pool (`sync.Map` keyed by `connection_id`) that persists across task
  executions on the same worker instance — a connection's account/role
  settings only take effect for pools created after the connection was
  last edited; an already-warm pool on a long-lived worker won't pick up a
  changed password/role until that worker restarts or the pool is otherwise
  evicted.

**Code snippet:**
```python
# Works today:
extract = SnowflakeOperator(
    task_id="extract",
    connection_id="snowflake_prod",
    sql="SELECT * FROM sales.orders WHERE region = '{{region}}'",  # NOT a real bind —
    parameters={"region": "US-WEST"},  # this is literal string substitution;
                                        # quote/escape values yourself if untrusted
)

# BROKEN TODAY — raises TypeError at DAG-parse time, do not use:
# bad = PostgresOperator(task_id="bad", sql="SELECT 1", connection_id="pg_conn")
```

---

### 59. Data movement

**Description:** `S3ToRedshiftOperator` runs a Redshift `COPY` command from
an S3 object (supports IAM role or access-key auth, wildcards in `s3_key`,
optional `truncate`). `GCSToBigQueryOperator` submits a BigQuery load job
from a GCS object via the REST API and polls until it completes, supporting
multiple `source_format`s, `write_disposition` modes, and either schema
`autodetect` or explicit `schema_fields`.

**Edge cases:**
- **`truncate=True` is NOT atomic with the COPY that follows it.**
  `S3ToRedshiftExecutor` runs `TRUNCATE TABLE schema.table` as its own,
  separate statement, then runs `COPY` as a second, independent statement —
  if the COPY subsequently fails (bad file, permissions, malformed data),
  the table has **already been truncated** and stays empty; there is no
  wrapping transaction that rolls the truncate back. Treat `truncate=True`
  as "delete now, then try to reload" — not a safe all-or-nothing swap.
- **`iam_role` and `s3_conn_id` are mutually exclusive in practice, not
  merged.** If `iam_role` is set (non-empty), the executor uses `IAM_ROLE`
  auth exclusively and never even resolves/looks up `s3_conn_id`'s
  credentials — setting both doesn't combine them, `iam_role` simply wins.
- **`region` only falls back to the connection's stored region in
  access-key mode.** When using `iam_role` auth, the connection's
  credentials (and its `region` field) are never resolved at all — if you
  don't pass `region=` explicitly alongside `iam_role`, the generated
  `COPY` command has no `REGION` clause, which can silently fail or behave
  unexpectedly if your bucket isn't in the same region as the Redshift
  cluster.
- **Table/schema/column names and the S3 key are directly string-interpolated
  into the `COPY` SQL** with no escaping — same class of concern as feature
  58's `parameters`; keep these DAG-author-controlled, not derived from
  trigger-time `conf`.
- **`GCSToBigQueryOperator`'s poll loop has no timeout of its own** —
  `pollJob` loops on a plain ticker until the job reaches `DONE` or the
  task's own context is cancelled (i.e., `execution_timeout`, feature 16, or
  the global `WORKER_DEFAULT_TASK_TIMEOUT_SECS` default). `poll_interval`
  only controls the polling cadence, not a maximum wait — set
  `execution_timeout` explicitly if you need a hard cap on how long a load
  job is allowed to run.
- **A single failed poll request fails the whole task immediately** — unlike
  `HttpSensor` (feature 47), which treats a network error as "not ready
  yet" and keeps retrying, `GCSToBigQueryExecutor`'s poll loop has no
  tolerance for a transient HTTP hiccup; one failed status check ends the
  task.
- **`autodetect=True` and non-empty `schema_fields` are not mutually
  exclusive at the PI-Flow level** — both get sent to the BigQuery API in
  the same load-job request if you set both; unlike `PythonVirtualenvOperator`'s
  `venv`/`requirements` (feature 22), there's no validation here forcing
  "exactly one." Whatever ambiguity results is resolved by BigQuery's own
  API behavior, not anything PI-Flow controls.
- `destination_project`/`destination_dataset` fall back to the connection's
  stored `project_id`/`dataset` if omitted from the task itself — the same
  connection can silently target different datasets depending on whether a
  given task fills these fields in or leaves them to the connection default.

**Code snippet:**
```python
load_from_s3 = S3ToRedshiftOperator(
    task_id="load_from_s3",
    s3_conn_id="s3_default",
    redshift_conn_id="redshift_prod",
    s3_bucket="my-data-bucket",
    s3_key="exports/2026/01/*.csv",   # wildcard supported, not validated by PI-Flow
    table="orders",
    schema="public",
    copy_options="CSV IGNOREHEADER 1 GZIP",
    truncate=False,   # True truncates immediately, NOT rolled back if COPY then fails
)

load_from_gcs = GCSToBigQueryOperator(
    task_id="load_from_gcs",
    bigquery_conn_id="bq_prod",
    source_bucket="my-gcs-bucket",
    source_object="exports/*.parquet",
    destination_dataset="analytics",
    destination_table="orders",
    source_format="PARQUET",
    write_disposition="WRITE_TRUNCATE",
    autodetect=True,
    poll_interval=10,
)
load_from_gcs.execution_timeout = 1800   # bound the otherwise-unbounded poll loop
```

---

### 60. HTTP

**Description:** Call any HTTP endpoint (`url`, `method`, `headers`, `body`)
via the Go `HttpExecutor` (`ClassRemote`, doc 03), which returns the raw
response body as `return_value` on any 2xx status (feature 38's auto-XCom
detail applies here).

**Edge cases:**
- **Verified: there is no ready-made `HttpOperator`/`SimpleHttpOperator`
  Python class to import.** The Go executor registry fully supports the
  operator keys `http`, `httpoperator`, `simplehttp`, and
  `simplehttpoperator` (all routed to the same `HttpExecutor`), but
  `dag_context.py` defines no corresponding operator class — only
  `HttpSensor` (feature 47, a polling *sensor*, not a one-shot HTTP call)
  exists out of the box. To make a one-shot HTTP call task, you must
  subclass `BaseOperator` yourself and set `operator_name = "http"` (or any
  of the other three accepted keys — matching is case-insensitive via
  `strings.ToLower`), passing `url`/`method`/`headers`/`body` as constructor
  kwargs so they land in `self.params` via `BaseOperator`'s `**params`
  catch-all.
- **Because this is a non-Python operator, the Go side never calls back
  into Python's `execute()`/`self.defer()` at all** — unlike feature 53's
  deferrable custom operator (which the worker's Python subprocess actually
  runs), a custom `BaseOperator` subclass used purely to construct an
  `http`-keyed params blob doesn't need (and won't have) any `execute()`
  method invoked; the class exists solely as a DAG-parse-time vehicle to
  produce the right JSON for the Go executor to consume directly.
- `url`/`headers`/`body` are **not** in any `_template_fields` list (that
  mechanism is Python-family-only, feature 40) — they render through Go
  templating (feature 39) like Bash/SQL params, not Jinja2.
- Response body truncated at 1MB, only a 2xx status counts as success, and
  no `response_check_regex` option exists here (that's HttpSensor-only,
  feature 47) — a one-shot HTTP call either succeeds outright on 2xx or
  fails outright otherwise; there's no way to additionally validate the
  response body content without a follow-up Python task reading the pushed
  `return_value`.

**Code snippet:**
```python
from dag_parser.dynamic.dag_context import BaseOperator

class HttpCallOperator(BaseOperator):
    """No ready-made HttpOperator exists — define your own thin wrapper."""
    operator_name = "http"   # or "simplehttp" / "httpoperator" / "simplehttpoperator"

notify_downstream = HttpCallOperator(
    task_id="notify_downstream",
    url="https://api.example.com/v1/events",
    method="POST",
    headers={"Content-Type": "application/json", "Authorization": "Bearer token"},
    body='{"event": "pipeline_complete", "dag_id": "{{ .DagID }}"}',
)
```

---

### 61. SSH

**Description:** `SSHOperator(command=..., connection_id=...)` opens an SSH
connection and runs a single remote command, authenticating with either a
password or a private key (whichever the connection provides), returning
combined stdout+stderr as `return_value` (feature 38).

**Edge cases:**
- **Verified, security-relevant: host key verification is completely
  disabled.** `buildSSHConfig` in `executor_ssh.go` sets `HostKeyCallback:
  ssh.InsecureIgnoreHostKey()` unconditionally — there is no host key
  pinning, no known_hosts checking, and no way to configure one via the
  connection or task params. Every SSH connection PI-Flow makes is
  structurally vulnerable to a man-in-the-middle substituting a different
  host at the same address; this is not something a DAG author can opt out
  of or harden today.
- **`environment={}` may silently do nothing, depending on the remote
  server's `sshd_config`.** The executor calls `session.Setenv(k, v)` per
  entry, but most SSH servers reject `SetEnv` requests unless the variable
  name is explicitly allowlisted (`AcceptEnv`) — a rejected `Setenv` call is
  only logged as a warning, never surfaced as a task error. Don't assume
  `environment=` reliably sets remote env vars; verify against your actual
  target server's `sshd_config`, or pass values via the command line/
  command string itself instead.
- **Private key is preferred over password when both are present on the
  connection** — `buildSSHConfig` adds `ssh.PublicKeys(signer)` before
  `ssh.Password(...)` in the auth methods list; if the connection has both
  `private_key` and a `password` set in `extra`/`password`, the key is
  attempted first per SSH's normal auth-method negotiation.
- The connection dial has a hardcoded 30-second timeout with no
  task-level knob to extend it — a slow-to-respond bastion/jump host
  can't be given more time to connect without going through a task-level
  `execution_timeout` (feature 16) that also has to cover the command's
  own runtime.
- `CombinedOutput` means stdout and stderr are interleaved into the single
  `return_value` string, identical to the auto-XCom behavior already
  documented in feature 38 — there's no way to capture stdout and stderr
  separately from an `SSHOperator` task.

**Code snippet:**
```python
from dag_parser.dynamic.operators import SSHOperator

remote_cleanup = SSHOperator(
    task_id="remote_cleanup",
    connection_id="prod_bastion",   # host key checking is NOT performed — see edge case
    command="rm -rf /tmp/staging/{{ .DS }}",
    environment={"STAGE": "prod"},  # may be silently dropped by sshd's AcceptEnv config
)
```

---

### 62. Email / Slack

**Description:** `EmailOperator(to=..., subject=..., html_content=...)` and
`SlackAPIPostOperator(channel=..., text=...)` send a message as a normal
task execution (Remote class) — distinct from the callback-based alert
channels in features 42/43, which fire on a task/DAG *event* rather than
running as their own scheduled step in the graph.

**Edge cases:**
- **Verified: `EmailOperator`'s `from_email`, `files`, and `custom_headers`
  are accepted by the Python constructor but never read by the Go
  executor.** `EmailExecutor`'s params struct only parses `to`, `subject`,
  `html_content`, `cc`, `bcc` from JSON — `from_email` is silently ignored
  (every email sends from the globally-configured `SMTP_FROM`, never a
  per-task override), `files` has **no attachment support at all** despite
  the parameter existing, and `custom_headers` never reaches the outgoing
  MIME message. Only `to`/`subject`/`html_content`/`cc`/`bcc` actually work.
- **`EmailOperator` has its own separate, narrower token-substitution pass
  that only touches `html_content`, never `subject`.** It replaces `{{
  dag_id }}`/`{{dag_id}}`, `{{ task_id }}`/`{{task_id}}`, `{{
  run_id }}`/`{{run_id}}` (3 tokens, no `event`) in the body only — contrast
  with feature 42's callback-based email, whose `templateReplace` covers 4
  tokens (adds `event`) and applies to **both** `subject` and
  `html_content`. A `{{dag_id}}` placeholder in an `EmailOperator`'s
  `subject=` will NOT be replaced and shows up literally in the sent email.
- **`SlackAPIPostOperator`'s `channel` requirement depends on the
  connection's `auth_type`.** With a **token**-auth connection, `channel`
  is required — the executor fails outright if it's empty. With a
  **webhook**-auth connection, `channel` is optional (most incoming
  webhooks are already bound to a fixed channel server-side, so PI-Flow
  simply omits it from the payload if unset).
- **Verified: `unfurl_links=True` only has an effect with a token-auth
  connection.** `postWebhook` never reads `params.UnfurlLinks` at all —
  only `postWithToken` includes it in the API payload. Setting
  `unfurl_links=True` on a task backed by a webhook-mode Slack connection
  is silently a no-op.
- **`return_value`'s shape depends entirely on the connection's
  `auth_type`, not anything in the DAG file** — webhook mode returns
  Slack's raw webhook response body (typically the literal string `"ok"`),
  while token mode returns `{"ts": "<message timestamp>"}` JSON. A
  downstream task reading this task's XCom needs to know which auth mode
  the connection uses to know what shape to expect.

**Code snippet:**
```python
from dag_parser.dynamic.dag_context import EmailOperator
from dag_parser.dynamic.dag_context import SlackAPIPostOperator

send_report = EmailOperator(
    task_id="send_report",
    to=["team@company.com"],
    subject="Report for {{dag_id}}",       # NOT substituted — sent literally
    html_content="<p>Report for {{ dag_id }} run {{ run_id }}</p>",  # IS substituted
    from_email="custom@company.com",       # accepted but IGNORED — uses global SMTP_FROM
    files=["/tmp/report.csv"],              # accepted but NO attachment is actually sent
)

post_to_slack = SlackAPIPostOperator(
    task_id="post_to_slack",
    connection_id="slack_bot_token",   # if auth_type="token": channel is REQUIRED
    channel="#data-alerts",
    text="Report generation complete",
    unfurl_links=True,   # only has an effect if the connection uses token auth
)
```

---

### 63. Databricks

**Description:** `DatabricksSubmitRunOperator` submits a one-off run via
the Jobs API 2.1 (`notebook_task`, `spark_python_task`, or `spark_jar_task`),
against either an existing `cluster_id` or an ephemeral `new_cluster` spec,
and polls until the run reaches a terminal state.

**Edge cases:**
- **`task_type` must be exactly one of `notebook_task`, `spark_python_task`,
  `spark_jar_task`** (default `"notebook_task"`) — anything else fails at
  **execution** time (`"unsupported task_type"`), not DAG-parse time, since
  the Python constructor never validates it.
- **`new_cluster` silently wins over `cluster_id` if both are set** —
  `buildSubmitPayload` checks `new_cluster` first; a non-empty
  `new_cluster` dict is used and `cluster_id` is completely ignored, the
  same "one wins, not merged" pattern as feature 59's `iam_role` vs
  `s3_conn_id`. If neither is set (and the connection has no default
  `cluster_id` either), submission fails outright.
- **`cluster_id` falls back to the connection's stored `extra.cluster_id`**
  if omitted from the task itself — same connection-level-default pattern
  as GCS→BigQuery's project/dataset (feature 59).
- **The poll loop has no timeout of its own and no tolerance for a single
  transient HTTP error** — identical pattern to `GCSToBigQueryOperator`
  (feature 59): `pollRun` loops until `TERMINATED`/`SKIPPED`/`INTERNAL_ERROR`
  or context cancellation, and any single failed status-check HTTP call
  ends the task immediately rather than being retried. Set
  `execution_timeout` (feature 16) explicitly to bound how long you're
  willing to wait for a Databricks job.
- **The XCom `return_value`'s usefulness depends heavily on `task_type`.**
  `getRunOutput` only returns something meaningful for a `notebook_task`
  that calls `dbutils.notebook.exit(...)` (captured as `notebook_output.result`)
  or a run with populated `logs`; a `spark_python_task`/`spark_jar_task` run
  typically has neither, so `return_value` often falls back to the raw,
  unparsed API response JSON.
- **`idempotency_token`, if set, is passed through unchanged on every retry
  attempt** (feature 13) — since it's a static value from the DAG file, a
  task that fails and retries with the same `idempotency_token` may be
  deduplicated by Databricks itself, potentially returning/reusing the
  original (already-failed or still-running) run instead of genuinely
  resubmitting — don't set a static `idempotency_token` on a task that also
  has `retries` configured unless you specifically want that dedup behavior.

**Code snippet:**
```python
from dag_parser.dynamic.dag_context import DatabricksSubmitRunOperator

run_notebook = DatabricksSubmitRunOperator(
    task_id="run_notebook",
    connection_id="databricks_prod",
    task_type="notebook_task",
    notebook_path="/Shared/etl/daily_aggregate",
    base_parameters={"run_date": "{{ .DS }}"},
    new_cluster={  # wins over any cluster_id/connection default if both are set
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2,
    },
    poll_interval=15,
    execution_timeout=3600,   # bound the otherwise-unbounded poll loop
)
```

---

### 64. Orchestration

**Description:** `TriggerDagRunOperator` (feature 27) plus all four sensor
types (`HttpSensor`/`SqlSensor`/`TimeSensor`/`ExternalTaskSensor`, features
47-50) make up PI-Flow's cross-DAG/wait-based orchestration surface —
composing multiple DAGs together and coordinating on external or sibling-DAG
state, rather than operating purely within one DAG's own task graph.

**Edge cases:**
- **Verified, subtle bug: `TriggerDagRunOperator` is misclassified for
  admission control — it lands in `ClassLocal`, not `ClassRemote`.**
  `ClassifyOperator` (`executor_class.go`) checks the lowercased operator
  string against a `remoteKeywords` list containing `"trigger_dagrun"`
  (with an underscore) — but the actual operator string is the class name
  `"TriggerDagRunOperator"`, which lowercases to `"triggerdagrunoperator"`
  (no underscore anywhere). The substring match never succeeds, so
  `TriggerDagRunOperator` falls through to the default `ClassLocal`
  instead of `ClassRemote`. Practical consequence: a `TriggerDagRunOperator`
  task competes for the `MaxLocalTasks` cap (default 20, doc 03/10) and is
  **fully blocked at Critical admission load** — even though, semantically
  and in terms of actual work done (a lightweight DB insert to create a
  child run), it belongs with the always-admitted Remote class alongside
  Snowflake/HTTP/SQL tasks. Under sustained CPU/memory pressure, a DAG
  relying on `TriggerDagRunOperator` to kick off downstream pipelines can
  get throttled right alongside real CPU-heavy Python/Bash work.
- **The four sensors, by contrast, correctly land in `ClassSensor`** (their
  operator names literally contain the substring `"sensor"`, which matches
  cleanly) — blocked only at Critical load, more leniently than
  `ClassLocal`. So within this one "Orchestration" feature grouping,
  `TriggerDagRunOperator` and the sensors are treated very differently by
  admission control, despite both being fundamentally lightweight,
  non-CPU-bound waiting/coordination mechanisms.
- `TriggerDagRunOperator`'s `wait_for_completion=True` (feature 27) uses the
  triggerer/`waiting_for_child` mechanism, not a sensor at all — it holds no
  worker slot while waiting, unlike a `mode="poke"` sensor (feature 52).
  Don't conflate the two: `TriggerDagRunOperator` waiting on a child run and
  a sensor waiting on a condition are implemented by completely separate
  subsystems (deferred-task reconciler vs. sensor executor).
- All the "verified gaps" already documented per-feature still apply here
  unchanged: `TriggerDagRunOperator`'s `conf` isn't validated against the
  child's `params` schema (feature 27), `SqlSensor`'s `connection_id` is a
  dead param that always queries PI-Flow's own metadata Postgres
  (feature 48), and `HttpSensor`'s `connection_id` is similarly unused
  (feature 47) — this "Orchestration" grouping doesn't change or fix any of
  those underlying behaviors, it's simply the catalog's umbrella label for
  this set of five operators.

**Code snippet:**
```python
from dag_parser.dynamic.dag_context import TriggerDagRunOperator
from dag_parser.dynamic.operators import ExternalTaskSensor

with DAG(dag_id="orchestrator", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    # Runs as ClassLocal today (verified bug) — competes with CPU-heavy tasks
    # for MaxLocalTasks and is blocked at Critical admission load.
    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="child_pipeline",
        wait_for_completion=True,
    )

    # Runs as ClassSensor — more leniently admitted than ClassLocal
    wait_for_sibling = ExternalTaskSensor(
        task_id="wait_for_sibling",
        external_dag_id="sibling_pipeline",
        allowed_states=["success"],
        mode="reschedule",
    )

    [trigger_child, wait_for_sibling]
)
```

---

## Category: In-app: DAGs

> Note: this category and everything after it documents the **UI/API surface**
> (what an operator does through the web app or REST API), not DAG-file
> Python syntax — there is no `with DAG(...)` snippet for these; the code
> blocks below show the actual HTTP request/response shape instead.

### 65. Pause/unpause

**Description:** `PATCH /api/dags/{dag_id}` with `{"is_paused": true|false}`
toggles a DAG's paused state — the switch behind the UI's Pause/unpause
toggle. Handled by `DagsHandler.PatchDag` → `DAGRepository.UpdateDAGPausedState`,
a single `UPDATE DAG SET IS_PAUSED = $1 WHERE DAG_ID = $2`.

**Edge cases:**
- **`is_paused` must be explicitly present in the JSON body, not just any
  truthy field.** The handler binds into a `*bool` and checks for `nil` —
  sending `{}` (or omitting the key entirely) returns `400 "is_paused field
  is required"` rather than defaulting to a no-op.
- **This is completely independent of `is_active`** (the ingestion-managed
  flag that flips when a DAG's file is removed from Git — doc 06). The PATCH
  only ever touches `IS_PAUSED`; pausing a DAG whose file was since deleted
  from the repo has no additional effect, since the scheduler already
  excludes it via `is_active`.
- **Verified: the scheduler consults the paused flag in exactly one place —
  `GetActiveDAGs()`'s `WHERE is_paused = FALSE AND is_active = TRUE` clause**
  (feeding the cron-eval phase), and the dataset-trigger evaluator repeats
  the identical `is_paused = FALSE AND is_active = TRUE` filter independently
  in its own query. Pausing takes effect on the **next scheduler tick**
  (~30s), not instantly, and blocks both cron/timetable-driven **and**
  dataset-driven automatic run creation equally.
- **Only new automatic run creation is blocked** — a run already
  `queued`/`running` at the moment you pause keeps going through planning,
  dispatch, and finalization untouched, because those phases operate
  directly on existing `dag_run`/`task_instance` rows, not on the
  paused-DAG filter.
- **A paused DAG also can't be manually triggered** (feature 24) — the
  trigger request is rejected outright, so pause doubles as "block all new
  runs, automatic or manual," not just the cron side.
- No dedicated audit-log entry is written for this action specifically —
  `PatchDag` has no `audit.Log`-style call, unlike some other admin mutation
  endpoints (doc 04's audit-log coverage doesn't reach this one).

**Code snippet:**
```
# Pause a DAG — stops new automatic AND manual runs; already in-flight runs continue
PATCH /api/dags/sales_daily_report
Content-Type: application/json

{"is_paused": true}

# -> 200 {"dag_id": "sales_daily_report", "is_paused": true}

# Unpause
PATCH /api/dags/sales_daily_report
{"is_paused": false}

# Omitting the key entirely is a 400, not a no-op:
PATCH /api/dags/sales_daily_report
{}
# -> 400 {"error": "is_paused field is required"}
```

---

### 66. View source & graph

**Description:** Two independent, unrelated endpoints back this one UI
feature. `GET /api/dags/{dag_id}/tasks/graph` returns the static task
dependency graph (`{dag_id, tasks: [{taskId, dependsOn, operator}]}`), built
fresh on every call from the live `task`/`task_dependency` tables.
`GET /api/github/raw?file=<filename>` serves the DAG's raw Python source —
local checkout first, GitHub raw as fallback — and is **not scoped by
`dag_id` at all**, only by filename.

**Edge cases:**
- **The graph is built from the current `task`/`task_dependency` tables, NOT
  the frozen `dag_version` snapshot** (doc 01/02) that a running `dag_run` is
  actually pinned to — editing and re-ingesting the DAG file changes what
  this endpoint shows immediately, even for a DAG run currently executing
  against an older, frozen version. What you see in the graph view can
  diverge from what a specific in-flight run is actually structured as.
- **`file` is reduced to `filepath.Base(file)` before lookup** (path-traversal
  guard, doc 04) — you cannot pass the DAG's `fileloc` verbatim (e.g.
  `dags/sales/daily_report.py`); only the basename is used. Two DAG files
  with the same filename in different subdirectories are indistinguishable
  to this endpoint — whichever one is actually on disk at that basename is
  what gets served, regardless of which `dag_id` you were viewing when you
  clicked "view source."
- **Local-first, GitHub-fallback, silently.** It tries
  `$DAGS_REPO_PATH/<basename>` first and only reaches out to
  `raw.githubusercontent.com` (hardcoded owner/repo/branch/path via env
  vars, independent of the ingestion service's own `GIT_REPO_URL`) if the
  local file is missing — a stale or out-of-sync local checkout silently
  masks whatever is actually latest on GitHub, with no indication in the
  response of which source was used.
- **The graph endpoint 404s only on a missing `dag_id`** — a DAG that
  exists but has zero task rows (e.g. it failed to fully ingest) returns a
  normal `200` with `tasks: []`, not an error.
- **Dangling dependency rows are dropped silently, not surfaced.** If
  `task_dependency` references a `task_id` no longer present in `task`
  (e.g. a partial/inconsistent ingestion), that edge is skipped with a
  server-side warning log only — the API response simply under-represents
  the real dependency data with no client-visible indication anything was
  dropped.
- Both the top-level task list and each task's `dependsOn` array are sorted
  **alphabetically** for deterministic output — this ordering carries no
  execution-order meaning (it is not a topological sort); don't read graph
  array order as run order.

**Code snippet:**
```
GET /api/dags/sales_daily_report/tasks/graph
-> 200
{
  "dag_id": "sales_daily_report",
  "tasks": [
    {"taskId": "build_report", "dependsOn": [], "operator": "PythonOperator"},
    {"taskId": "email_report", "dependsOn": ["build_report"], "operator": "EmailOperator"}
  ]
}

# Decoupled from dag_id entirely — only the basename matters:
GET /api/github/raw?file=sales_daily_report.py
-> 200 text/plain
# raw .py source: local dags-repo checkout first, GitHub raw fallback
```

---

### 67. Search/filter

**Description:** `GET /api/dags` accepts `search`, `tags`, `owner`,
`is_paused`, `last_run_state`, `sort`, and `order` query params. Supplying
**any** of them switches the handler from the plain `GetAllDags` query to
`GetAllDagsFiltered`, which builds a dynamic `WHERE`/`ORDER BY` clause.

**Edge cases:**
- **`search` is one param matching THREE columns at once** —
  `dag_id ILIKE OR owners ILIKE OR description ILIKE` against the same
  substring. You cannot scope a search to "dag_id only"; a search term that
  happens to appear in a description will surface DAGs you weren't
  necessarily looking for by ID.
- **Verified: `owner=` filters a column that's never actually populated.**
  Per feature 1's confirmed gap, `owners` from the DAG file is silently
  dropped at ingestion, and no webserver write-path was found that sets it
  either — in practice, filtering by `owner=` reliably returns nothing
  today, regardless of what you pass.
- **`tags=` is a raw substring match against the column's JSON-encoded
  string form** (`["sales","reporting","daily"]`, feature 1), not an
  exact-tag match against a real array. `tags=ale` matches a DAG tagged
  `sales`; there is no way to require an exact tag or AND multiple tags —
  only one substring against the whole JSON blob per request.
- **`is_paused=` only recognizes the literal string `"true"`.** Anything
  else you pass — `"1"`, `"True"`, `"yes"`, a typo — is silently coerced to
  `false` (`isPausedStr == "true"`), not rejected or ignored; a caller
  expecting "any truthy value works" will get the opposite of what they
  intended.
- **`sort=` only recognizes exactly two allowlisted values: `dag_id` and
  `last_run_date`.** Anything else (a typo, an unsupported field like
  `owner` or `is_paused`) is silently ignored and falls back to sorting by
  `dag_id` — no error, no indication the sort was dropped.
- **There is no pagination at this endpoint at all** — filtered or not,
  every call returns the **entire** matching result set in one response;
  there's no `limit`/`offset`/cursor param, so a large DAG catalog means a
  correspondingly large single JSON payload.

**Code snippet:**
```
GET /api/dags?search=sales&is_paused=false&sort=last_run_date&order=desc
-> 200 {"dags": [ {...}, {...} ], "total": 2}

# owner= filters a column that's never populated from the DAG file — reliably empty:
GET /api/dags?owner=data-team
-> 200 {"dags": [], "total": 0}
```

---

### 68. Params form

**Description:** `GET /api/dags/{dag_id}/params` returns just
`{dag_id, params: <params_schema JSON>}`, read straight off `dag.params_schema`
— the same schema declared via `Param(...)` in the DAG file (feature 8). The
UI fetches this to render the trigger dialog's form fields before a user
fills them in and triggers a run (feature 24), rather than pulling it from
the fuller `GET /api/dags/{dag_id}` payload.

**Edge cases:**
- **`params: {}` is returned both for "this DAG declares no params" AND for
  "the stored `params_schema` JSON failed to parse."** `GetParamsSchema`
  swallows a `json.Unmarshal` error silently (`if jsonErr == nil`) and just
  leaves the response map at its initialized empty value either way — a
  form-rendering client cannot distinguish a genuinely param-less DAG from
  one whose schema is corrupted, without some other signal.
- **This endpoint only ever returns the declared schema, never
  current/previous trigger values.** Every time the trigger form opens, it
  reflects only the DAG's static `Param(default=...)` values (feature 8) —
  there's no "last used values" or per-user memory merged in here.
- **Not version-pinned** — like feature 66's graph, this always reads the
  **live** `dag` row's `params_schema` (latest ingested DAG file), not a
  frozen `dag_version` snapshot. In practice this doesn't create a mismatch,
  since a manual trigger right afterward also pins to that same latest
  version (feature 24) — but it's worth knowing this route bypasses
  `dag_version` entirely.
- **Same trigger-time validation gap as feature 24 applies to whatever this
  form submits back:** only `string`/`integer`/`boolean` typed params are
  actually validated when supplied through the `params` field at trigger
  time; a form field rendered for a `number`/`array`/`object`/`date`/
  `datetime` param will fail validation if the user actually fills it in
  and submits via `params` — the workaround remains the legacy `conf` field.
- A DAG that doesn't exist returns `404 "DAG not found"` here, same as
  `GET /api/dags/{dag_id}` — but a DAG that exists with truly no `params`
  key in its `params_schema` column still returns a `200` (see first edge
  case), not a 404.

**Code snippet:**
```
GET /api/dags/etl_orders/params
-> 200
{
  "dag_id": "etl_orders",
  "params": {
    "run_date": {"type": "string", "required": true, "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
    "customer_id": {"type": "integer", "default": 0, "minimum": 0},
    "mode": {"type": "string", "enum": ["full", "incremental"], "default": "incremental"}
  }
}

# UI renders form inputs from the schema above, then triggers with:
POST /api/dags/etl_orders/trigger
{"params": {"run_date": "2026-07-14", "customer_id": 42, "mode": "full"}}
```

---

## Category: In-app: Runs

### 69. Trigger run

**Description:** `POST /api/dags/{dag_id}/trigger` (optional body
`{execution_date, conf}` or `{execution_date, params}`) is the UI's
"Trigger" button — the HTTP-facing counterpart to feature 24. Beyond
validating and inserting a `queued` `dag_run` row, this handler additionally
runs an **inline fast-path**: within the same HTTP request, it plans the
run's tasks and pushes them straight into the worker pool's dispatch
channel, instead of waiting for the scheduler's next ~30s poll.

**Edge cases:**
- **Idempotent by construction:** `run_id = {dag_id}_manual__{execution_date_iso8601}`
  — re-POSTing the same `execution_date` returns the existing run with
  `200 OK` and `"message": "DAG run already exists for this execution_date"`
  instead of creating a duplicate, rather than the fresh `201 Created` a new
  trigger gets.
- **Verified: the inline dispatch is best-effort and fails completely
  silently from the caller's perspective.** If planning, the
  `tasks_created` flag update, the state transition, the transaction
  commit, or pushing into the dispatch channel fails for any reason
  (channel full, a DB hiccup, or the dispatch channel simply not being
  wired in a scheduler-only build), the handler just logs a warning and
  returns — the HTTP response is completely unaffected and still reports
  success. The run just falls back to the scheduler's normal ~30s poll
  instead of starting near-instantly; nothing in the response tells you
  which path actually happened.
- **Verified: the JSON response's `state` field is hardcoded to
  `"queued"`, even when inline dispatch already succeeded and flipped the
  DB row to `"running"` moments earlier in the same request.** Don't trust
  the trigger response's `state` as current truth — re-fetch via feature
  72's endpoint immediately after if you need the real state.
- `execution_date` accepts exactly three formats — full RFC3339
  (`2026-01-29T00:00:00Z`), a bare `Z`-suffixed variant, or a plain date
  (`2026-01-29`) — anything else 400s as `INVALID_EXECUTION_DATE` before
  `params`/`conf` are even looked at.
- Same params-vs-conf precedence and typed-validation gap as feature 24
  (`number`/`array`/`object`/`date`/`datetime` params fail validation if
  submitted via `params`; use `conf` to bypass).

**Code snippet:**
```
POST /api/dags/etl_orders/trigger
Content-Type: application/json

{
  "execution_date": "2026-07-14T00:00:00Z",
  "params": {"run_date": "2026-07-14", "customer_id": 42, "mode": "full"}
}

# -> 201 Created (new run; may already be "running" in the DB via inline
#    dispatch, even though this response always says "state": "queued")
{
  "dag_id": "etl_orders",
  "run_id": "etl_orders_manual__2026-07-14T00:00:00Z",
  "run_type": "manual",
  "state": "queued",
  "execution_date": "2026-07-14T00:00:00Z"
}

# Re-POSTing the identical execution_date returns the SAME run, 200 not 201:
# -> 200 {"...", "message": "DAG run already exists for this execution_date"}
```

---

### 70. Backfill range

**Description:** `POST /api/dags/{dag_id}/backfill`
(`{start_date, end_date, reset_existing, conf}`) is the UI's backfill-range
action — the HTTP entry point behind feature 25. It re-derives every
scheduled `execution_date` in range from the DAG's own cron string via
`CalculateAllMissedRuns`, then bulk-inserts `run_type='backfill'` rows.

**Edge cases:**
- **`ErrNoSchedule` fires on exactly three `schedule_interval` values: the
  empty string, the literal string `"None"`, and the literal string
  `"@once"`** — confirming feature 25's claim that one-shot and (by
  extension, since they carry no cron string at all) timetable-scheduled
  DAGs can't use this endpoint; the check is a plain string comparison, not
  a semantic "is this DAG timetable-scheduled" check.
- **Verified: `reset_existing=true`'s delete and the subsequent bulk-insert
  are two separate, non-transactional calls.** If `DeleteDAGRunsInRange`
  succeeds but `BulkInsertBackfillRuns` then fails for any reason (a DB
  error, a constraint violation), the date range has already been wiped
  with nothing put back — there is no wrapping transaction to roll the
  delete back. This is the same "delete now, hope the reload works"
  pattern as feature 59's `truncate=True` COPY.
- **Two separate safety caps exist, but only one is actually reachable
  from the API.** Date calculation itself is capped at 1000 results
  internally (`CalculateAllMissedRuns(..., maxRuns=1000)`), but the
  endpoint rejects anything **over 500** before ever reaching the insert
  step — so in practice the 500-run rejection (feature 25) always fires
  first; the inner 1000 cap only matters if that limit is changed in code.
- `start_date` is shifted back by exactly one second
  (`startDate.Add(-time.Second)`) before being used as the cron walk's base
  time, specifically so a cron tick landing exactly on `start_date` itself
  is included rather than skipped as "before the window."
- The response's `count` reflects **rows actually inserted**
  (`ON CONFLICT DO NOTHING`), which can be lower than the number of dates
  computed if some already existed in the range — re-running the same
  backfill request (without `reset_existing`) is safe and just reports a
  smaller `count` the second time.
- Same date formats as trigger (RFC3339, plain date, or a bare
  no-offset datetime) are accepted for `start_date`/`end_date` — no
  timezone conversion beyond straight parsing; a naive date like
  `"2026-01-01"` is treated as UTC midnight regardless of the DAG's own
  `timezone` (feature 3).

**Code snippet:**
```
POST /api/dags/daily_aggregation/backfill
Content-Type: application/json

{
  "start_date": "2026-01-01",
  "end_date": "2026-01-31",
  "reset_existing": false
}

# -> 201 Created
{
  "dag_id": "daily_aggregation",
  "runs": ["daily_aggregation_backfill__2026-01-01T00:00:00Z", "..."],
  "count": 31
}

# A DAG with schedule="@once", schedule=None, or a named timetable=:
# -> 400 {"error": "DAG has no schedule", "code": "NO_SCHEDULE"}
```

---

### 71. Delete run

**Description:** `DELETE /api/dags/{dag_id}/runs/{run_id}` removes a single
`dag_run` row outright — a plain
`DELETE FROM metadata.dag_run WHERE dag_id=$1 AND run_id=$2`. Because every
dependent table (`task_instance`, and transitively `xcom`,
`trigger_instance`) declares `ON DELETE CASCADE` back to `dag_run`
(doc 06), this one row delete cascades to wipe the run's entire task
history in the same statement.

**Edge cases:**
- **Verified, no state guard at all: nothing stops you from deleting a
  `running`/`queued` run.** The repository issues the `DELETE`
  unconditionally — it never checks `state` first. Deleting an in-flight
  run does **not** cancel the worker goroutines/subprocesses currently
  executing its tasks; they keep running to completion (or their own
  timeout) with their `task_instance` row already gone. Any state-flush
  `UPDATE` those in-flight tasks attempt afterward simply matches zero
  rows and is silently a no-op — no error surfaces anywhere, but the run's
  history is already gone, and any XCom those orphaned tasks try to
  push/pull fails since its `task_instance` FK target no longer exists.
- **This is a genuine, irreversible delete** — unlike a "clear" action
  (which resets state so tasks re-run under the same `run_id`), there is no
  soft-delete/undo; the `dag_run` row and everything cascaded from it is
  gone permanently.
- **Not-found is a real `404`, distinguished from a generic failure:** the
  repository returns the sentinel `sql.ErrNoRows` specifically when zero
  rows were affected, which the handler checks with `errors.Is` to return
  `404 "DAG run not found"` rather than a `500`.
- `run_id` is URL-decoded with `url.PathUnescape` before use (same
  reasoning as feature 72) — required because manual/backfill run_ids
  embed a colon- and, for some timezone offsets, `+`-bearing ISO8601
  timestamp that must survive being placed in a URL path segment.
- **There is no bulk/range delete at this endpoint** — contrast with
  backfill's `reset_existing=true` (feature 70), which deletes a whole
  date range in one call; this endpoint only ever removes exactly one
  `(dag_id, run_id)` pair per request.

**Code snippet:**
```
DELETE /api/dags/sales_daily_report/runs/sales_daily_report_manual__2026-07-14T00%3A00%3A00Z

# -> 200 {"message": "DAG run deleted", "dag_id": "sales_daily_report", "run_id": "..."}
# -> 404 {"error": "DAG run not found"}   if the run doesn't exist

# NOTE (verified): nothing stops you from deleting a run that is still
# `running` — its in-flight tasks are NOT cancelled, they just lose their
# task_instance row (cascaded away) mid-execution.
```

---

### 72. View run detail

**Description:** `GET /api/dags/{dag_id}/runs/{run_id}` returns run-level
metadata (`state`, `execution_date`, `start_time`, `end_time`, a
server-computed `duration_seconds`) for the run detail page;
`GET .../runs/{run_id}/tasks` returns the per-task state list used to
overlay onto the task graph (feature 66) — each task's **latest try only**.

**Edge cases:**
- **`run_id` is URL-decoded with `url.PathUnescape`, deliberately not
  `QueryUnescape`.** A manual/backfill `run_id` can embed a `+` (from a
  timezone-offset-bearing timestamp); `QueryUnescape` would turn that `+`
  into a literal space, corrupting the lookup. If you're building your own
  client against this API, decode/encode the same way — a naive
  URL-decode step can silently 404 a run that actually exists.
- **`duration_seconds` for a still-`running` run is computed live as
  `NOW() - start_date` at the moment of the request, not a ticking
  value.** It's a snapshot; polling (or the WebSocket state-push, doc 04)
  is what makes it feel "live" in the UI — the field itself doesn't update
  on its own between requests.
- **The `/tasks` (and legacy `/task-instances`) view collapses retries:
  for any task with `try_number > 1`, only the highest `try_number` row is
  returned** (`ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY try_number
  DESC)`), so earlier failed attempts are invisible from this endpoint —
  you'd need the task's own log files/task-detail view (doc 07's "Task
  detail" feature) to see prior-try history, not this run-level task list.
- **The two "not found" checks aren't symmetric across routes under the
  same `/runs/:run_id` group.** `GetDagRunDetail` (the primary detail
  route) checks DAG existence first and 404s as `"DAG not found"`
  separately from a `"DAG run not found"` when the DAG exists but the run
  doesn't — but the `/tasks` route only checks whether the **run** exists,
  skipping the DAG-existence check entirely.
- `conf` in the response is omitted (`null`/absent) whenever it's exactly
  the literal empty object `"{}"` — you can't distinguish "the trigger
  request explicitly sent `conf: {}`" from "no conf was ever set" by
  inspecting this field.

**Code snippet:**
```
GET /api/dags/sales_daily_report/runs/sales_daily_report_manual__2026-07-14T00%3A00%3A00Z
-> 200
{
  "dag_id": "sales_daily_report",
  "run_id": "sales_daily_report_manual__2026-07-14T00:00:00Z",
  "run_type": "manual",
  "state": "running",
  "execution_date": "2026-07-14T00:00:00Z",
  "start_time": "2026-07-14T00:00:01Z",
  "end_time": null,
  "duration_seconds": 381
}

GET /api/dags/sales_daily_report/runs/sales_daily_report_manual__2026-07-14T00%3A00%3A00Z/tasks
-> 200
[
  {"task_id": "build_report", "operator": "PythonOperator", "state": "success",
   "try_number": 1, "start_time": "...", "end_time": "..."},
  {"task_id": "email_report", "operator": "EmailOperator", "state": "up_for_retry",
   "try_number": 2, "start_time": "...", "end_time": null}
]
```

---

## Category: Config resources

### 73. Connections

**Description:** `/api/connections` CRUD + `/api/connections/test` (validate
and probe without saving) + `/api/connections/export|import` (bulk JSON,
admin-gated) — backed by a `Registry` of exactly **10** `Connector`
implementations (snowflake, databricks, mysql, postgres, redshift, s3, gcs,
bigquery, slack, ssh — matching doc 04's list; still no PagerDuty
connector, per feature 45). Each connector supplies `Meta()` (dynamic form
fields for the UI), `Validate`, `Normalize`, a real `TestConnection` probe,
and `SecretFields()` (which `extra` JSONB keys get AES-256-GCM encrypted
alongside the main `password` column).

**Edge cases:**
- **Verified, critical bug: editing a connection through a form pre-filled
  from a fetched masked password silently corrupts it.**
  `GetConnectionByID`/`GetAllConnections` always return
  `password: "********"` (a fixed literal placeholder). `UpdateConnection`
  has zero special-case for that placeholder — it only skips re-encrypting
  a value that already carries the `enc:v1:` prefix. Submitting an edit
  where the password field still literally contains `"********"` (i.e. a
  UI round-tripped the masked GET response back into the update PUT
  without the user retyping the secret) gets AES-encrypted **as the new
  real password**, permanently overwriting the actual credential with the
  8-character string `"********"`. Always re-enter the real secret when
  editing rather than resubmitting whatever a GET returned.
- **`TestConnection` never reads a saved connection by ID — it only
  validates whatever raw body you POST.** There's no "test the connection I
  already saved" by `connection_id`; the full request (including a real,
  unmasked password) must be resent every time you click Test, even for an
  already-existing connection.
- `Extra` secret fields (e.g. a nested API token) are only encrypted for
  the specific keys each connector declares via `SecretFields()` — an
  ad-hoc key you add to `extra` yourself that isn't in that list is stored
  and returned in **plaintext**, even sitting right next to genuinely
  encrypted sibling keys in the same JSONB blob.
- `GET /api/connections/export` requires the **Admin** role specifically
  (checked via a dedicated `IsAdmin` helper), independent of the normal
  RBAC permission system used everywhere else in this API — a custom role
  granted `connection:view` via `piflow_permission` but not literally
  `Admin` still gets `403` from export, even though it can read
  connections individually via `GET /api/connections/:id`.
- `mask_passwords` on export (default `true`) only blanks the top-level
  `password` field — it has no effect on `extra`'s secret fields, which are
  always masked separately and unconditionally by a different code path;
  the two masking mechanisms don't share a toggle.
- Bulk `import` (`mode=skip|overwrite|fail`, default `skip`) processes the
  array **sequentially, per-item**, collecting failures into an `errors`
  array rather than failing the whole batch atomically — a malformed
  connection #3 of 10 doesn't roll back #1–2 that already succeeded; check
  the response's `created`/`updated`/`skipped`/`errors` counts, don't
  assume all-or-nothing.

**Code snippet:**
```
GET /api/connections/types
-> 200 [{"type": "snowflake", "label": "Snowflake", "fields": [...]}, ...]  # exactly 10 types

POST /api/connections
{"connection_id": "snowflake_prod", "connection_type": "snowflake",
 "username": "svc_user", "password": "real-secret-value",
 "extra": {"account": "xy12345", "warehouse": "COMPUTE_WH"}}
# -> 201, password stored AES-256-GCM encrypted

POST /api/connections/test
{"connection_id": "snowflake_prod", "connection_type": "snowflake",
 "username": "svc_user", "password": "real-secret-value", "extra": {"account": "xy12345"}}
# -> 200 {"status": "success", "message": "Connection successful"}  (a real live probe)

# DANGER (verified): re-saving a fetched connection without retyping the password
GET /api/connections/snowflake_prod   -> {"password": "********", ...}
PUT /api/connections/snowflake_prod   {"password": "********", ...}  # <-- corrupts the real secret
```

---

### 74. Variables

**Description:** `/api/variables` CRUD, backed by the flat `variable` table
(key → string value, `is_encrypted` flag). Keys must match
`^[a-zA-Z][a-zA-Z0-9_.\-]{0,254}$`. When `is_encrypted=true`,
create/update AES-encrypt the value before writing it, and the single-key
GET transparently decrypts it back on read.

**Edge cases:**
- **The list view masks encrypted values as `"***"`, but the single-key GET
  does not mask at all — it returns the real, decrypted plaintext.**
  `GET /api/variables` shows `"***"` for any `is_encrypted=true` row;
  `GET /api/variables/:key` for that same row returns the actual secret in
  the clear to anyone with view permission. Encryption here (like the
  DAG-runtime Variables in feature 41) only protects the value at rest in
  Postgres and in the list view — not from any caller who can hit the
  single-key endpoint.
- **Same structural risk as the Connections bug (feature 73) is present in
  the code, though not confirmed against any specific frontend:** `Update`
  has no special-casing for a placeholder value either — if a client
  pre-fills an edit form from the masked `"***"` list view (rather than a
  fresh single-key GET) and saves without changing the value, `"***"`
  itself gets encrypted and stored as the new "real" value, silently
  destroying the original secret. Always fetch via the single-key endpoint
  (which returns the true value) before editing an encrypted variable,
  never from the list.
- **`is_encrypted` is just another field in the update payload, not a
  one-way flag** — flipping it `true→false` on update stores the (already
  decrypted-by-then, since the request must supply the real value)
  plaintext going forward; flipping `false→true` encrypts whatever
  plaintext you send. There's no server-side guard against accidentally
  toggling this.
- Key format validation only runs on **create** — update/delete operate on
  whatever `key` is already in the path param and never re-validate the
  pattern (moot in practice, since an invalid key couldn't have been
  created in the first place).
- **The worker's own internal variable loader silently skips any row whose
  decryption fails**, rather than failing the whole variable-load for every
  task — a corrupted encrypted value (e.g. from the placeholder-overwrite
  risk above, once the AES key or ciphertext no longer matches) just
  vanishes from every task's `{{ .Var.x }}`/`var.value.x` context with no
  error surfaced anywhere.

**Code snippet:**
```
POST /api/variables
{"key": "extract_api_key", "value": "sk-real-secret", "is_encrypted": true}
# -> 201 {"message": "variable created", "key": "extract_api_key"}

GET /api/variables                  -> [{"key": "extract_api_key", "value": "***", "is_encrypted": true}]
GET /api/variables/extract_api_key  -> {"key": "extract_api_key", "value": "sk-real-secret", "is_encrypted": true}
#                                       ^ real plaintext returned here — no masking on single-key GET
```

---

### 75. Pools

**Description:** Per doc 06/08, `metadata.pool` (`pool_id` PK, `slots`,
`description`) is the concurrency-pool table a task's `pool=` param
(feature 18) draws from, and the RBAC seed matrix even grants
`pool:view`/`pool:edit` permissions to the Admin/Op/Editor roles.

**Edge cases:**
- **Verified: there is no REST API for Pools at all.** An exhaustive
  search of the entire webserver package (handlers, services,
  repositories, routes) turns up zero pool-related endpoints — the only
  code that reads `metadata.pool` is the scheduler-side
  `pool_repository.go`'s `GetAllPools()`, called exclusively at dispatch
  time to enforce slot limits (doc 02/03). The `pool:view`/`pool:edit` RBAC
  permissions are seeded and ready, but there is nothing behind them to
  authorize — "View/edit concurrency slots" (doc 07/08) has no UI or API
  surface to exercise today.
- **Practical consequence for feature 18:** creating a pool with a real
  slot cap (rather than the effectively-unlimited fallback that applies
  when a `pool=` name has no matching row) requires inserting directly
  into `metadata.pool` via a SQL client against the Postgres sidecar —
  there is no `POST /api/pools`-style call to do it through the product.
- Only the seeded `default` pool (128 slots) exists out of the box
  (`pi_schema.sql`'s idempotent seed) — any other pool name referenced
  from a DAG file exists only as a string on the `task` row until someone
  manually inserts a corresponding `metadata.pool` row.
- Because enforcement is scheduler-side only (a SQL subquery against
  `metadata.pool.slots` at dispatch time), even a manually-inserted pool
  row takes effect on the **very next scheduler iteration** — no restart
  or cache invalidation needed, once you have a way to create the row.

**Code snippet:**
```
-- No REST endpoint exists — the only way to create a real pool cap today
-- is a direct SQL statement against the Postgres sidecar:
INSERT INTO metadata.pool (pool_id, slots, description)
VALUES ('etl_heavy', 10, 'Heavy ETL tasks - capped at 10 concurrent')
ON CONFLICT (pool_id) DO UPDATE SET slots = EXCLUDED.slots;

-- Referencing it from a DAG file (feature 18) works as soon as the row exists:
heavy_task = PythonOperator(task_id="heavy_transform", python_callable=lambda: None, pool="etl_heavy")
```

---

### 76. Event listeners

**Description:** `/api/event-listeners` CRUD (admin-only, `system/admin`
RBAC) registers webhook subscriptions on the in-process event bus for
lifecycle events (`GET /api/events/types` enumerates all 15 — task state
changes, DAG run created, SLA miss, dataset triggered, backpressure,
callback fired, etc.). `GET /api/events/recent` serves a ring-buffer of
recently published events for debugging, filterable by `dag_id`/`type`.

**Edge cases:**
- **Verified, significant bug: deleting a listener never unsubscribes it
  from the live event bus.** `DeleteListener` only removes the DB row —
  the event bus has no `Unsubscribe` method at all, and nothing in the
  delete path removes the in-memory webhook subscription that was
  registered when the listener was created (or loaded at boot). A
  "deleted" listener keeps firing its webhook for every matching event
  until the process restarts, even though it no longer appears in
  `GET /api/event-listeners`.
- **Verified, compounding bug: updating a listener re-subscribes instead of
  replacing.** `UpdateListener` re-registers a fresh webhook subscription
  with the new config but never removes the old one first (same missing-
  unsubscribe root cause) — after editing a listener once, the same event
  fires the webhook **twice** (old config and new config both still live);
  editing it N times compounds to N+1 deliveries per event, all within the
  same running process's lifetime.
- **Both bugs are process-lifetime, not persistent.** A full restart
  re-bootstraps subscriptions cleanly from scratch (only `enabled=true,
  listener_type='webhook'` rows are loaded fresh at boot), so the
  duplicate/zombie-listener state resets on redeploy but silently
  accumulates between deploys as listeners are edited/deleted through the
  UI in the meantime.
- **`listener_type` isn't validated against a fixed set at all.** Only the
  literal string `"webhook"` is ever actually wired to the event bus; any
  other value (a typo, or a placeholder for a not-yet-implemented type) is
  accepted and persisted without error, but silently never fires anything.
- `event_types` is stored and used for filtering but isn't cross-checked
  against the known event-type list either — a typo'd event type string is
  accepted at creation time and will simply never match any real published
  event, with no validation error to catch the mistake.
- `GET /api/events/recent`'s `limit` silently clamps to `100` for any value
  `<=0` or `>500` (including non-numeric strings, which parse to `0`) —
  passing `limit=99999` or `limit=abc` both quietly return the default
  100, not an error or the max 500.

**Code snippet:**
```
POST /api/event-listeners
{"name": "sla_alerts", "event_types": ["sla_miss"], "listener_type": "webhook",
 "config": {"url": "https://ops.company.com/hooks/piflow-sla", "headers": {}}}
# -> 201, immediately subscribed to the live event bus

# Editing it once already double-fires (verified bug):
PUT /api/event-listeners/1
{"name": "sla_alerts", "event_types": ["sla_miss"],
 "config": {"url": "https://ops.company.com/hooks/piflow-sla-v2"}}
# -> 200, but BOTH the old and new webhook URLs now receive every sla_miss
#    event until the server process restarts

DELETE /api/event-listeners/1
# -> 200 {"deleted": true} — but the in-memory subscription(s) keep firing regardless
```

---

### 77. Environments

**Description:** `POST /api/admin/envs` (`{"requirements": [...]}`)
submits an async, admin-only build of a managed Python virtualenv for
`PythonVirtualenvOperator` (feature 22) — `GET /api/admin/env-builds/:id`
polls build state (`pending`→`building`→`ready`/`failed`);
`GET /api/admin/envs` / `GET /api/admin/envs/:name` list/describe
already-built envs from the read-only stage mount;
`DELETE /api/admin/envs/:name` removes one.

**Edge cases:**
- **The env name is fully deterministic, not chosen by the caller.** It's
  `auto_<first-16-hex-of-sha256>` of the requirements list after
  trim/blank-line/comment stripping and **sorting** — the same set of
  packages in any order, or with extra blank lines/comments, always
  derives the identical name, matching the Python SDK's own env-naming
  logic (verified against a shared test vector) so a DAG's
  `requirements=[...]` (feature 22) and an admin's manual build request
  agree on the name without either side communicating it explicitly.
- **Submitting an identical requirements set while a build is already
  `pending`/`building`/`ready` returns that SAME build object idempotently**
  — it does not start a second build or error; only once the prior build
  for that exact name ended `failed` (or was deleted) does resubmitting
  start a fresh one.
- **Verified: only ONE environment build runs at a time, system-wide,
  regardless of how many distinct requirement sets are submitted
  concurrently.** The build step is guarded by a single package-level
  mutex, not one per env name — submitting builds for `pandas` and,
  separately, `numpy` back-to-back serializes the second fully behind the
  first's complete `venv` + `pip install` + stage-copy cycle, even though
  they share nothing.
- **Installs are wheelhouse-only, never live PyPI** (`pip install
  --no-index --find-links <wheelhouse> ...`, doc 05) — a requirement not
  already staged in the wheelhouse fails the build with pip's own "no
  matching distribution" error; there's no fallback to the internet even
  though the app service otherwise has egress.
- **`jinja2` and `markupsafe` are force-appended to every build's pip
  install command**, regardless of whether the DAG author's
  `requirements=[...]` mentions them — needed because Python-operator
  template rendering (feature 40) depends on Jinja2 being present in
  whatever interpreter runs the task; if your own requirements pin a
  conflicting `jinja2` version, both constraints go into the same
  `pip install` call and pip's normal resolver behavior (not anything
  PI-Flow controls) decides what actually lands.
- **Verified (explicit code comment): `DeleteEnv` only removes the
  writable-stage copy — it does not evict any worker's already-materialized
  local cache.** Per doc 03's venv-resolver behavior, a worker that already
  pulled this env down keeps using its local content-hash-keyed copy
  "until that worker recycles" — deleting an in-use env doesn't break
  already-warm workers immediately, but any worker that hasn't cached it
  yet (or a freshly scaled-out instance) fails to resolve it right away.
  There is also no "is this env currently referenced by any DAG" check
  before deletion.
- **Build status deliberately lives at a separate path,
  `/api/admin/env-builds/:id`, rather than `/api/admin/envs/:id`** — a
  code comment notes this specifically avoids a Gin routing conflict
  between the static build-status lookup and the wildcard
  `/envs/:name` describe/delete route; don't guess the URL by analogy with
  the other resource's `:id`-style routes.

**Code snippet:**
```
POST /api/admin/envs
{"requirements": ["pandas==2.2.0", "requests==2.31.0"]}
# -> 202 {"build_id": "bld_...", "name": "auto_<16hex>", "state": "pending"}

GET /api/admin/env-builds/bld_...
# -> 200 {"build_id": "...", "name": "auto_...", "state": "ready", "content_hash": "..."}

GET /api/admin/envs
# -> 200 {"envs": [{"name": "auto_...", "python_version": "3.12.x", ...}], "count": 1}

DELETE /api/admin/envs/auto_...
# removes the stage copy only — warm workers keep using their locally cached copy
```

---

## Category: Access & account

### 78. Login/session

**Description:** `/api/auth/login` (session cookie + JWT pair),
`/api/auth/token` (JWT-only, no cookie), `/api/auth/logout`,
`/api/auth/refresh`, `/api/auth/me`, and `/api/auth/password` (change own
password) make up the self-service auth surface. A single `Login` call
establishes **two independent, differently-lived credentials at once**: an
HttpOnly session cookie (DB-tracked, revocable) and a JWT access+refresh
pair (stateless, never persisted anywhere server-side).

**Edge cases:**
- **Verified: logging out only revokes the session-cookie path — any JWTs
  issued by that same login call keep working until they naturally
  expire.** `Logout` deletes the session row and clears the cookie, but
  JWTs are never tracked in any table (no blacklist/revocation list) — an
  access token (15 min default) or refresh token (7 days default) grabbed
  from a login response before logging out remains fully valid for the
  rest of its natural lifetime, Bearer-header auth included, even after
  "logging out."
- **Verified: `/api/auth/refresh` doesn't invalidate the refresh token it
  was just given.** It mints a brand-new access+refresh pair from a
  presented refresh token, but the old one isn't blacklisted anywhere —
  both the old and the newly-issued refresh token remain independently
  valid (each until its own expiry), not rotate-and-invalidate.
- **Role changes lag behind already-issued tokens.** `Refresh`/`Login`
  always fetch fresh role names at mint time, so a freshly-minted token
  reflects a user's *current* roles — but a token minted **before** a role
  change keeps its old roles baked into its claims and stays valid under
  them for up to its full 15-minute life. Only the session-cookie path
  re-checks `is_active` live on every request; the JWT path has no
  per-request revalidation at all.
- **`/api/auth/token` and `/api/auth/refresh` have none of `/api/auth/login`'s
  audit logging.** `Login` calls the audit logger on both success and
  `login_failed`; `TokenLogin` and `Refresh` call it on neither —
  JWT-only logins and every token refresh leave no trace in `audit_log`,
  only cookie-based UI logins do.
- **`ChangePassword` doesn't revoke anything else either.** Changing your
  own password re-hashes and stores it, but does not delete your other
  active sessions or invalidate any already-issued JWTs — a stolen
  session/token from before the password change keeps working exactly as
  before until it separately expires or is manually revoked (feature 80).

**Code snippet:**
```
POST /api/auth/login
{"username": "alice", "password": "correct-horse-battery-staple"}
# -> 200 {"access_token": "...", "refresh_token": "...", "expires_at": "...",
#         "user": {...}, "roles": ["Editor"], "force_password_change": false}
# Also sets an HttpOnly session cookie — TWO independent credentials from one call

GET /api/auth/me        (Authorization: Bearer <access_token>, OR the session cookie)
-> 200 {"user": {...}, "roles": [...], "permissions": [...]}

POST /api/auth/refresh
{"refresh_token": "<old_refresh_token>"}
# -> 200 new access_token + refresh_token
# NOTE: the OLD refresh_token you just sent is still valid too — not rotated out

POST /api/auth/logout
# -> 200 {"message": "logged out"} — kills the session cookie/row ONLY;
#    any JWTs from the same login keep working until they expire naturally
```

---

### 79. API keys

**Description:** `/api/auth/api-keys` — Create/List/Revoke long-lived
personal API keys (`X-API-Key` header auth, doc 04). A key is generated as
`pf_<64 hex chars>`, shown once in the Create response, and stored only as
a SHA-256 hash plus a display `key_prefix` (`pf_xxxxxxxx...`) — the
plaintext is never recoverable after creation.

**Edge cases:**
- **The plaintext key is shown exactly once, at creation** — losing it
  means generating a brand-new key (and separately revoking the old one if
  it needs to be retired); there is no "show key again" or
  reset-without-rotating endpoint.
- **`APIKeyMaxPerUser` (default 5, doc 05) only counts currently
  `is_active=true` keys.** Revoking a key immediately frees a slot — you
  can create and revoke keys indefinitely as long as you never have more
  than the max simultaneously active; revoked keys aren't deleted, just
  flagged inactive, so lifetime key count isn't capped.
- **Revocation is checked live on every single request, not cached** —
  `ValidateAPIKey`'s query filters `is_active=TRUE AND u.is_active=TRUE AND
  (expires_at IS NULL OR expires_at > NOW())` fresh each time, so
  `DELETE /api/auth/api-keys/:id` takes effect on the very next API call
  made with that key — unlike the JWT path (feature 78), API-key auth has
  real, immediate revocation.
- **An admin can revoke ANY user's key by ID, but cannot list other users'
  keys through this API.** `RevokeKey` accepts an admin bypass on the
  ownership check, but `ListKeys` is hard-scoped to the caller's own
  `user_id` with no admin "list all keys" or "list this user's keys"
  variant — an admin can only act on a key ID they already know (e.g. from
  the audit log), not discover it by browsing.
- **There is no rename/edit endpoint** — a key's `name` is fixed at
  creation; renaming means revoking and creating a new key under the
  desired name (which also means rotating the secret, since there's no way
  to relabel without regenerating).
- Every successful create/revoke is audit-logged (`create_api_key`/
  `revoke_api_key`) — unlike login via `/api/auth/token` (feature 78),
  this surface's mutations are consistently traceable in `audit_log`.

**Code snippet:**
```
POST /api/auth/api-keys
{"name": "ci-pipeline-key"}
# -> 201 {"key": "pf_9f3a...<64 hex chars>...", "api_key": {"id": 7, "key_prefix": "pf_9f3a3c1e...", ...},
#         "message": "Store this key securely — it will not be shown again"}

GET /api/auth/api-keys
# -> 200 {"api_keys": [{"id": 7, "name": "ci-pipeline-key", "key_prefix": "pf_9f3a3c1e...",
#                        "is_active": true, "last_used_at": null, ...}]}
# (the raw key itself is never returned again, only the masked prefix)

DELETE /api/auth/api-keys/7
# -> 200 {"message": "API key revoked"} — effective immediately on the next request using it
```

---

### 80. Sessions

**Description:** `/api/admin/sessions` (list all active browser sessions,
admin-only) + `/api/admin/sessions/:id` (force-terminate one) +
`/api/admin/users/:id/sessions` (terminate ALL sessions for a user) — the
admin-facing counterpart to the per-user session created at login
(feature 78).

**Edge cases:**
- **"Instant revocation" is real here, structurally, unlike JWTs.**
  `ValidateSession` is a live `SELECT ... WHERE s.id=$1 AND s.expires_at >
  NOW() AND u.is_active=TRUE` executed on every request authenticated via
  the session cookie — there's no in-memory session cache to invalidate.
  Terminating a session (or a user's `is_active` flag flipping to false)
  takes effect on that user's **very next request**, not after some cache
  TTL.
- **This only ever touches session-cookie auth — it has zero effect on
  that same user's JWTs or API keys.** Terminating every session for a
  user does not revoke any access/refresh token they're also holding
  (feature 78) nor any API key (feature 79) — a user logged in via a
  mobile client using only a Bearer token continues working uninterrupted
  even after an admin "terminates all their sessions" from the UI, which
  can give a false sense of having fully locked someone out.
- **`ListActiveSessions` only ever shows non-expired rows**
  (`WHERE expires_at > NOW()`) — an admin reviewing "who's logged in"
  never sees a session that's already timed out, even if it was active
  moments ago; there's no historical view of past sessions here (that's
  what `audit_log`'s `login`/`logout` entries are for, separately).
- **No record of *why* a session was terminated is kept on the session
  side** — `TerminateSession`/`TerminateUserSessions` just `DELETE` the
  row(s) outright; any audit trail of an admin forcibly logging someone
  out has to come from `audit_log` (if that admin action is itself
  logged elsewhere), not from anything the session table retains.
- Sessions carry `ip_address`/`user_agent` captured **at login time only**
  — if a session is later reused from a different network/browser, neither
  field updates to reflect that; only `last_active_at` moves (touched on
  each authenticated request), so the admin list can't distinguish "same
  browser, different location" without cross-referencing IP history
  elsewhere.

**Code snippet:**
```
GET /api/admin/sessions
# -> 200 {"sessions": [{"id": "a1b2...", "user_id": 3, "username": "alice",
#                        "ip_address": "10.0.0.5", "user_agent": "Mozilla/5.0...",
#                        "expires_at": "...", "last_active_at": "..."}]}

DELETE /api/admin/sessions/a1b2c3...
# -> 200 {"message": "session terminated"} — effective on alice's very next request

DELETE /api/admin/users/3/sessions
# -> 200 {"message": "all sessions terminated for user"}
# NOTE: alice's JWTs and API keys (features 78/79) are UNAFFECTED by this call
```

---

### 81. RBAC (admin)

**Description:** `/api/admin/users`, `/api/admin/roles`,
`/api/admin/roles/:id/permissions`, and `/api/dags/:dag_id/permissions`
provide full user/role/permission/per-DAG-ACL management. The 5 seeded
roles (Admin/Op/Editor/Viewer/Public, doc 06) are structurally protected
from modification; only custom roles (created via `POST /api/admin/roles`)
can have their permissions edited.

**Edge cases:**
- **The 5 seeded roles are fully immutable through this API — name,
  description, permissions, and existence.** `UpdateRole`, `DeleteRole`,
  and `SetRolePermissions` each independently check for a default role and
  reject with `403` if so. There's no way to, say, add one extra
  permission to the built-in `Viewer` role directly — you'd need to create
  a new custom role with `Viewer`'s permissions plus the extra one, and
  assign that instead.
- **Verified, real TOCTOU race: "last admin" protection is read-then-write,
  not transactional.** Disabling a user (`is_active=false`), deleting a
  user, and removing the `Admin` role via role reassignment each
  independently count users with the `Admin` role and only reject if that
  count is `<= 1` *before* proceeding — there's no locking between the
  check and the mutation. Two concurrent requests each demoting a
  **different** one of exactly two remaining admins can both read
  `adminCount=2`, both pass the check, and both succeed — leaving zero
  admins, the exact outcome each individual request believed it was
  preventing.
- **`PUT /api/admin/users/:id/roles` fully replaces a user's role set — it's
  not additive.** Submitting a `role_ids` array that omits a role the user
  currently has silently removes it; there's no separate "grant one more
  role" call, only wholesale replace.
- **Verified, significant bug: `GET /api/dags/{dag_id}/permissions`
  hardcodes every entry's `source` field to the literal string `"admin"`,
  regardless of the row's actual `source` column.** The underlying query
  doesn't even select `dp.source`, and the handler's flattening logic
  writes `"source": "admin"` unconditionally for every permission it
  emits. A permission actually synced from a DAG file's `access_control=`
  (feature 10, `source='dag_file'`) is indistinguishable from an
  admin-set one in this view — an operator editing per-DAG permissions
  through this endpoint has no way to tell, from the API response, that a
  grant they're about to toggle will simply be overwritten again on the
  next Git ingestion pass. (The underlying write path is unaffected by
  this — the real `source` column is preserved correctly on disk; it's
  only this read endpoint that's lossy.)
- **`POST`/`DELETE /api/dags/{dag_id}/permissions` toggle exactly ONE
  action per call via a read-then-flip-then-upsert cycle** — granting a
  role `view`+`trigger`+`edit` on a DAG takes three separate sequential
  API calls, not one batch call with an actions array.
- **The permission row is deleted entirely once all five boolean columns
  are false**, rather than left as an all-`false` row — functionally
  equivalent for permission checks, but means re-querying immediately
  after removing a role's last permission on a DAG returns no row for that
  role at all, not a row with everything `false`.

**Code snippet:**
```
POST /api/admin/roles
{"name": "DataEngineer", "description": "Custom role for the data platform team"}
# -> 201 (a NEW custom role — editable/deletable, unlike the 5 seeded roles)

PUT /api/admin/roles/12/permissions
{"permissions": [{"resource_type": "dag", "resource_id": "*", "action": "trigger"},
                  {"resource_type": "connection", "resource_id": "*", "action": "view"}]}
# -> 200 {"message": "permissions updated"}  (would 403 if role 12 were e.g. "Editor")

PUT /api/admin/users/5/roles
{"role_ids": [12]}
# -> 200 — REPLACES all of user 5's roles with just role 12, not additive

POST /api/dags/finance_close/permissions
{"role_name": "Op", "action": "trigger"}
# -> 200 {"role_name": "Op", "action": "trigger", "source": "admin"}
# NOTE (verified): "source" is ALWAYS "admin" in every response from this endpoint,
# even for a permission actually synced from the DAG file's access_control= (feature 10)
```
