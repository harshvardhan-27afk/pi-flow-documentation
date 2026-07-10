# PI-Flow User Guide

This guide is written for people **authoring DAG files**. For every feature you get:
a short description, edge cases/gotchas to know before you rely on it, and a code
snippet showing the exact syntax to use in a Python DAG file.

All snippets use the Airflow-2.x-style API that PI-Flow's dynamic parser
(`dag_parser/dynamic/dag_context.py`) understands.

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
> not the `DAG` object itself — they're listed here because they were requested
> alongside the DAG-level batch, but syntactically they go on the operator call
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

This closes out the **Per-task customization** category.

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

