# PI-Flow DAG Authoring Guide

This guide teaches you how to **author DAGs** (workflows) for PI-Flow — the Python
syntax, the parameters each feature accepts, and how each feature actually behaves
once your DAG is running. 



---

## Part 0 — Foundations

### What is a DAG?

A **DAG** (Directed Acyclic Graph) is just PI-Flow's word for "a workflow." You
write it as a plain Python file. It describes:

- **What work needs to happen** — a set of individual steps, called **tasks**.
- **In what order** — which tasks must finish before others can start.
- **When it should run** — on a schedule, on demand, or in response to an event.

"Directed" means dependencies point one way (task A leads to task B, not the
reverse). "Acyclic" means there are no loops — a task can never depend on itself,
even indirectly through other tasks.

### The smallest possible DAG

```python
from datetime import datetime
from dag_parser.dynamic.dag_context import DAG
from dag_parser.dynamic.operators import PythonOperator

with DAG(
    dag_id="hello_world",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
) as dag:

    def say_hello():
        print("Hello from PI-Flow!")

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )
```

Save this as a `.py` file in the `dags/` folder of your team's Git repo. PI-Flow
periodically pulls that repo, notices the new/changed file, and parses it — no
manual upload step. From that point on, `hello_world` shows up in the UI and starts
producing scheduled runs.

### The building blocks, in plain terms

| Term | What it means |
|---|---|
| **DAG** | The whole workflow definition — one Python file (or one `with DAG(...)` block) usually equals one DAG. |
| **Task** | One unit of work inside a DAG — "run this Python function," "run this SQL," "call this API." |
| **Operator** | The *kind* of task — `PythonOperator`, `BashOperator`, `SnowflakeOperator`, etc. Each operator knows how to execute one kind of work. |
| **Dependency** | An ordering rule between two tasks, written with `>>` — "B only starts after A." |
| **DAG Run** | One *execution* of the whole DAG — e.g. "today's 2 AM run." A DAG can have many runs over time, one per schedule tick (or per manual trigger). |
| **Task Instance** | One task, inside one specific DAG run — e.g. "the `say_hello` task, from today's 2 AM run." This is the thing that actually has a state (`running`, `success`, `failed`, ...). |

A useful mental model: **the DAG is the blueprint; a DAG Run is one building built
from that blueprint; a Task Instance is one room in that building.**

### The task state lifecycle

Every task instance moves through states as its run progresses. You'll see these
constantly in the UI and in this guide:

```
none → scheduled → queued → running → success
                                    ↘ failed
                                    ↘ up_for_retry → (back to scheduled)
                                    ↘ skipped
                                    ↘ deferred → (back to scheduled, once its wait condition is met)
                                    ↘ up_for_reschedule → (back to scheduled, for reschedule-mode sensors)
```

- `none` — not ready yet; still waiting on upstream tasks or a gating condition.
- `scheduled` / `queued` — ready to run; waiting for a worker to pick it up.
- `running` — actively executing.
- `success` / `failed` / `skipped` — a terminal outcome for this attempt.
- `up_for_retry` — failed, but will automatically try again.
- `deferred` — waiting on an async condition (a timer, an HTTP check) without holding a worker slot.
- `up_for_reschedule` — a "reschedule-mode" sensor's way of waiting without holding a worker slot between checks.

### How your DAG file actually gets picked up

You don't need to know the internals to author DAGs, but the short version helps
explain some behavior later in this guide: your DAG file lives in a Git repository;
PI-Flow periodically fetches that repo, detects new/changed `.py` files, and parses
them. Once parsed, your DAG's structure (tasks, schedule, dependencies) is recorded,
and from then on PI-Flow's scheduler takes over — creating runs, planning tasks, and
dispatching them to run, all without your file being touched again until you edit
it and push a new commit.

One thing worth knowing early: **an in-flight run is never affected by a later edit
to the DAG file.** Every run is pinned to the exact version of the DAG that existed
when it started, so you can safely fix bugs or add tasks without disturbing runs
already in progress.

### How this guide is organized

1. **Declaring a DAG & Scheduling** — the basics: identity, when it runs, time window, catchup.
2. **Controlling a DAG Run's behavior** — concurrency, timeouts, SLAs, typed parameters, callbacks, access control, partitioning, templating options.
3. **Tasks — behavior & customization** — retries, trigger rules, cross-run dependencies, timeouts, task SLA, pools, priority, setup/teardown, per-task callbacks, environment control.
4. **Scheduling & triggering runs** — automatic runs, manual triggers, backfill, dataset triggers, cross-DAG triggers, deferred waits.
5. **Dependencies & flow control** — edges, labels, convergence, dynamic mapping, branching, voluntary skip.
6. **Data passing & templating** — XCom, TaskFlow, Go/Jinja templating, Variables & Connections.
7. **Alerting & notifications** — email, Slack, HTTP webhook, PagerDuty, and how event scope works.
8. **Sensors & waiting** — the four built-in sensors, tuning, poke vs. reschedule, deferrable waits.
9. **Reliability, in one place** — a short recap of how SLA/timeout/retry mechanisms fit together.
10. **Operators & integrations catalog** — the full list of what a task can actually do.

Each feature below follows the same four-part layout:

- **What it does** — plain-language explanation.
- **Parameters** — every knob, its type, its default, and its valid range/values.
- **How it works** — the behavior you should understand before relying on it (not a bug list — just how the mechanism actually operates).
- **Example** — working code.

---

## Part 1 — Declaring a DAG & Scheduling

### 1. DAG identity & docs

**What it does:** Every DAG needs a unique identifier. You can also attach a
description and tags so people (and the search box) can find it later.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `dag_id` | string | *required* | Must be unique across your entire DAG repository. |
| `description` | string | `""` | Plain text, shown in the DAG list. Not Markdown-rendered. |
| `tags` | list of strings | `[]` | Used for search/filter in the UI. |
| `start_date` | `datetime` | *required for scheduled DAGs* | See feature 3. |

**How it works:**
- Because `dag_id` is the primary key PI-Flow uses to store your DAG, if two files
  in the repo declare the same `dag_id`, whichever one is ingested last "wins" and
  overwrites the other in storage — keep `dag_id`s unique per file.
- Keep `description` short — it's meant for a list view, not a full README.
- `owners` is a common Airflow convention but is not part of PI-Flow's DAG-level
  metadata today — if you want to record an owning team, put it in `description`
  or `tags` as a convention.

**Example:**
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

**What it does:** Decides when PI-Flow automatically creates a new run of your
DAG. There are three ways to schedule a DAG: a cron expression, a named
"timetable," or a dataset (run when upstream data changes rather than on a
clock).

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `schedule` (or `schedule_interval`) | string / list / dict / `None` | `None` | Cron string, `@daily`-style descriptor, a list of `Dataset(...)`, or a dict form (see below). `None` means manual-trigger-only. |
| `timetable` | string | `""` | One of `"last_day_of_month"` or `"business_days"` (Mon–Fri). Mutually exclusive with `schedule`. |

**How it works:**
- `schedule` and `schedule_interval` mean the same thing — just pick one name and
  use it consistently in a given DAG.
- Setting `timetable` overrides `schedule` entirely — a DAG uses either a cron
  schedule or a named timetable, never both at once.
- Passing a list of `Dataset(...)` objects (instead of a cron string) makes the DAG
  dataset-driven: it runs when the datasets it consumes get new data, not on a
  clock. See feature 26 for the full dataset-trigger walkthrough.
- The dataset dict form's `trigger_type` accepts exactly `"any"` (run once **any**
  listed dataset updates) or `"all"` (run only once **every** listed dataset has
  updated since the last successful run) — default is `"all"`.
- Double-check your cron syntax — an invalid cron expression is only caught when
  the scheduler tries to evaluate it, not when the DAG is first ingested.

**Example — cron:**
```python
with DAG(
    dag_id="hourly_sync",
    schedule="0 * * * *",   # every hour on the hour
    start_date=datetime(2026, 1, 1),
) as dag:
    ...
```

**Example — cron descriptor:**
```python
with DAG(
    dag_id="daily_job",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
) as dag:
    ...
```

**Example — named timetable:**
```python
with DAG(
    dag_id="month_end_close",
    timetable="last_day_of_month",
    start_date=datetime(2026, 1, 1),
) as dag:
    ...
```

**Example — dataset-driven:**
```python
from dag_parser.dynamic.dag_context import Dataset

with DAG(
    dag_id="consume_sales_table",
    schedule=[Dataset("s3://bucket/sales_table")],
    start_date=datetime(2026, 1, 1),
) as dag:
    ...

# Explicit form, triggered when ANY of the listed datasets updates:
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

### 3. Time window & timezone

**What it does:** Bounds *when* a DAG is allowed to schedule runs, and in what
timezone its cron expression should be interpreted.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `start_date` | `datetime` | *required for automatic scheduling* | The earliest point a scheduled run can be created from. |
| `end_date` | `datetime` | `None` | Stops future scheduling after this point. |
| `timezone` | string (IANA name, e.g. `"America/New_York"`) | `"UTC"` | Timezone used to evaluate the cron expression's wall-clock time. |

**How it works:**
- `start_date` is the anchor PI-Flow uses to calculate the first (and, with
  catchup, every subsequent) scheduled run — without it, there's no reference
  point.
- `timezone` affects the cron's wall-clock meaning: `"10 11 * * *"` means 11:10 in
  *that* timezone, not UTC. If your DAG seems to run at the "wrong" hour, this is
  usually why.
- `end_date` only stops *future* scheduling — it does not cancel a run that's
  already in progress or queued.
- If you leave `timezone` unset, it defaults to UTC — be explicit if your team
  thinks in local time.

**Example:**
```python
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

### 4. Catchup & backfill toggle

**What it does:** Controls whether PI-Flow fills in every schedule interval that
was "missed" between `start_date` and now, or only starts scheduling from the most
recent interval going forward.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `catchup` | boolean | `True` | `True` = backfill every missed interval; `False` = only run going forward. |

**How it works:**
- With `catchup=True` (the default) and a `start_date` set far in the past on a
  frequent schedule, PI-Flow will gradually create every missed run — a handful per
  scheduler cycle, not all at once — until it's caught up to "now."
- For most "just run going forward" DAGs, set `catchup=False` explicitly so
  ingesting the DAG for the first time doesn't produce a flood of historical runs.
- `catchup=False` still respects `max_active_runs` and every other run-level
  guardrail — it only changes *where* the schedule starts counting from.
- Catchup-created runs are still capped by `max_active_runs` (feature 5) — if the
  cap is already reached, the remaining catchup runs simply wait for a slot to free
  up.

**Example:**
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

## Part 2 — Controlling a DAG Run's Behavior

### 5. Run concurrency limits

**What it does:** Two independent caps: how many *runs* of this DAG can be active
at once, and how many *tasks* (across all its active runs, combined) can be
running at once.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `max_active_runs` | integer | `16` | Cap on concurrent `dag_run`s for this DAG. |
| `max_active_tasks` | integer | `None` (no DAG-level cap) | Cap on concurrent task instances across **all** of this DAG's active runs combined. |

**How it works:**
- If `max_active_runs` is reached, PI-Flow simply doesn't create the next
  scheduled run yet — it waits for a slot to free up rather than skipping or
  queuing indefinitely.
- `max_active_tasks` applies across **all runs of the DAG combined**, not
  per-run — with several concurrent runs and a low cap, tasks from *different*
  runs compete for the same budget.
- Set these thoughtfully on a wide, fan-out-heavy DAG — a low `max_active_tasks`
  throttles a single run's own internal parallelism, not just cross-run
  parallelism.

**Example:**
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

**What it does:** Force-fails an entire DAG run if it's still going after a set
number of seconds — a hard ceiling on total run duration.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `dagrun_timeout_seconds` | integer (seconds) | `None` | If unset, falls back to a global 24h abandoned-run safety net (see below). |

**How it works:**
- This is a strict wall-clock timeout — it fires even if every task inside the run
  is healthy and actively progressing. Set it generously if your DAG legitimately
  runs long.
- If you don't set it, the run still isn't unbounded — there's a global 24-hour
  fallback, but that one **only** fires if the run has genuinely gone quiet (no
  task actively heartbeating) — a slow-but-healthy run is never killed by the
  fallback alone.
- On timeout, the run and any still-active child tasks are force-failed — there's
  no "soft warning" state, it's a hard stop.
- This is separate from a task's own `execution_timeout` (feature 16) — set both
  if you want protection at both the run level and the individual task level.

**Example:**
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

**What it does:** Flags (but never fails) a run that takes longer than expected —
a monitoring signal, not an enforcement mechanism.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `expected_duration_seconds` | integer (seconds) | `None` | Threshold above which the run is flagged as an SLA miss. |
| `on_sla_miss_callback` | `SmtpNotifier(...)` (or a dict via the task-level `_callbacks` pattern — see feature 21) | `None` | What to do when the SLA is missed. |

**How it works:**
- An SLA miss is detection-only — it never changes the run's state or stops it. If
  you want to actually kill a long-running run, use `dagrun_timeout_seconds`
  (feature 6) instead.
- It fires once per run, not repeatedly on every subsequent scheduler tick.
- This measures the whole run's elapsed time; there's a separate, independent
  task-level SLA (feature 17) for flagging individual slow tasks.
- For reliable delivery today, use `SmtpNotifier(...)` (or the task-level dict
  form) for your callback — see feature 9 for the full picture on DAG-level
  callbacks.

**Example:**
```python
from dag_parser.dynamic.dag_context import SmtpNotifier

with DAG(
    dag_id="revenue_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    expected_duration_seconds=600,   # flag as an SLA miss if a run takes > 10 minutes
    on_sla_miss_callback=SmtpNotifier(
        to=["team@company.com"],
        subject="SLA miss: revenue_pipeline",
        html_content="<p>This run exceeded its expected duration.</p>",
    ),
) as dag:
    ...
```

---

### 8. Typed run parameters

**What it does:** Declares the shape of the `conf` a user must supply when
manually triggering the DAG — types, defaults, enums, ranges, and which fields are
required. The UI builds a form straight from this schema.

**Parameters (fields of `Param(...)`):**

| Field | Type | Default | Notes |
|---|---|---|---|
| `type` | string | *required* | One of `string`, `integer`, `number`, `boolean`, `array`, `object`, `date` (`YYYY-MM-DD`), `datetime` (ISO string). Any other value breaks ingestion. |
| `default` | matches `type` | none | If set, the param is treated as optional. |
| `required` | boolean | inferred | No `default` → required. Any `default` (even `0`/`False`/`""`) → optional, unless you set `required=False`/`True` explicitly. |
| `enum` | list | none | Restricts allowed values. |
| `minimum` / `maximum` | number | none | For `integer`/`number`. |
| `min_length` / `max_length` | integer | none | For `string`. |
| `pattern` | regex string | none | For `string`. |
| `description` | string | `""` | Shown in the UI form. |

**How it works:**
- `enum`/range/pattern rules are checked when someone actually triggers a run, not
  when the DAG file is parsed — a bad `conf` is rejected at trigger time.
- Booleans are checked strictly — `True`/`False` will not be silently accepted
  where an `integer`/`number` is expected, and vice versa.
- Inside your tasks, you read the *values* supplied at trigger time through
  templating (`{{ .Params.key }}` in Go-templated fields, or `params.key` in
  Jinja) — the `Param(...)` object itself only defines the schema, not the runtime
  value.
- All eight declared types (`string`, `integer`, `number`, `boolean`, `array`,
  `object`, `date`, `datetime`) are fully validated against your schema whenever
  a value is supplied through the typed `params` field at trigger time.

**Example:**
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

**What it does:** Attaches an alert to a DAG **run** reaching a terminal outcome —
`on_success_callback`, `on_failure_callback`, and `on_sla_miss_callback` (feature 7).

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `on_success_callback` | callable, or `SmtpNotifier(...)` | `None` | Fires when a run finishes successfully. |
| `on_failure_callback` | callable, or `SmtpNotifier(...)` | `None` | Fires when a run ends in failure. |
| `on_sla_miss_callback` | callable, or `SmtpNotifier(...)` | `None` | Fires on an SLA miss (feature 7). |

**How it works:**
- Pass either a plain Python function (it receives a `context` dict describing
  the run) for custom logic, or a `SmtpNotifier(to=[...], subject=...,
  html_content=...)` for a ready-made email alert — both dispatch correctly.
- If you need a DAG-level outcome to trigger a **Slack**, **HTTP webhook**, or
  **PagerDuty** alert declaratively rather than writing your own function, the
  task-level callback pattern in feature 21 supports those channels directly —
  or add a dedicated notification *task* at the end of the DAG (e.g.
  `SlackAPIPostOperator` with `trigger_rule="all_done"` or `"one_failed"`).
- For per-task alerting (a specific task's own success/failure/retry/skip), use
  the task-level callback pattern in feature 21 instead.

**Example:**
```python
from dag_parser.dynamic.dag_context import SmtpNotifier

def notify_failure(context):
    print(f"DAG failed: {context['run_id']}")

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

**What it does:** Declares per-role permissions for this specific DAG directly in
code, layered on top of (and separate from) permissions set manually in the Admin
UI.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `access_control` | dict `{role_name: [permissions]}` | `{}` | Permissions: `can_read`, `can_trigger`, `can_edit`, `can_delete`, `can_clear`. |

**How it works:**
- `role_name` must match an existing role — one of the built-in `Admin`, `Op`,
  `Editor`, `Viewer`, `Public`, or a custom role created via the Admin UI.
- Re-ingesting the DAG file **replaces** the set of permissions it manages each
  time — if you remove `access_control` from the file, those grants are removed on
  the next ingestion pass.
- Permissions set manually through the Admin UI are never touched by this — the
  two layers coexist per role/DAG.
- This only scopes access to *this* DAG — broader, cross-DAG role permissions are
  a separate, global RBAC concern.

**Example:**
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

**What it does:** Scopes a DAG's runs to a partition key (a date bucket, a region,
a customer segment, etc.), so each run's data footprint is tracked independently
of plain run history.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `partitions` | one of `DailyPartition()`, `HourlyPartition()`, `WeeklyPartition()`, `MonthlyPartition()`, `StaticPartition([...])` | `None` | See below for how the key is computed for each. |

**How it works:**
- For the time-based types (`Daily`/`Hourly`/`Weekly`/`Monthly`), the partition
  key is derived automatically from the run's execution date — e.g.
  `DailyPartition()` produces `"2026-07-09"`.
- `StaticPartition([...])` is different: it does *not* derive a key from the
  execution date at all — you must supply `partition_key` explicitly when
  triggering a run for a statically-partitioned DAG. Pass a non-empty list of
  keys.
- Partitioning is orthogonal to scheduling — you still need a `schedule`/
  `timetable`/dataset trigger (or manual triggers) to actually create runs;
  `partitions` only changes how those runs are tagged and tracked.

**Example:**
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

**What it does:** Four DAG-level knobs that change how Python-family operators
render their fields through Jinja2 at execution time.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `render_template_as_native_obj` | boolean | `False` | If `True`, a template resolving to a single value returns a real Python object (`int`, `list`, ...) instead of always a string. |
| `user_defined_macros` | dict `{name: callable}` | `{}` | Extra names usable inside `{{ }}` expressions. |
| `user_defined_filters` | dict `{name: callable}` | `{}` | Extra `\|filter` functions. |
| `template_undefined` | a Jinja2 `Undefined` subclass (e.g. `jinja2.StrictUndefined`) | permissive (renders empty) | Pass the class itself, not an instance. |

**How it works:**
- These four settings apply only to Python-family operators
  (`PythonOperator`/`ExternalPythonOperator`/`PythonVirtualenvOperator`) —
  specifically their `op_args`/`op_kwargs`/`templates_dict`. Non-Python operators
  (Bash/SQL/HTTP/etc.) render through a separate Go-templating mechanism (feature
  39) that doesn't read these settings at all.
- `render_template_as_native_obj=True` only changes behavior for a template that's
  *just* one expression (e.g. `"{{ params.count }}"`). A mixed template like
  `"count={{ params.count }}"` always renders as a string, flag or not.
- Because these attributes are read by re-importing the DAG file at execution
  time, your DAG file must remain importable on the worker at that moment — keep
  it valid even after you've edited other parts of it.

**Example:**
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

## Part 3 — Tasks: Behavior & Customization

### 13. Retry policy

**What it does:** Configures how many times a failed task automatically retries,
and how long it waits between attempts.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `retries` | integer | `0` | Max retry attempts (in addition to the first try). |
| `retry_delay_seconds` | integer | `0` | Fixed delay between attempts. |
| `retry_exponential_backoff` | boolean | `False` | Switch to exponential delay growth. |
| `max_retry_delay_seconds` | integer | `None` (no ceiling) | Cap on how large the exponential delay can grow. |

**How it works:**
- `retries=2` means **3 total attempts** — the first try plus 2 retries.
- With `retry_exponential_backoff=True`: `delay = min(retry_delay_seconds *
  2^(try_number-1), max_retry_delay_seconds)`, plus a random ±10% jitter to avoid
  many tasks retrying at exactly the same moment. If `retry_delay_seconds` is `0`
  or unset, it's floored to `1` second before the exponential math runs.
- Without `max_retry_delay_seconds`, exponential backoff has no ceiling — a task
  with many retries can end up waiting a long time between later attempts.
- Set retry defaults once for the whole DAG via `default_args=`, and override per
  task only where needed — a task-level value always wins over the DAG default.
- A task that's voluntarily skipped (feature 34) does not consume a retry
  attempt — only genuine failures do.

**Example:**
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

### 14. Trigger rules

**What it does:** Decides when a task becomes eligible to run, based on the
states of its upstream tasks — instead of the default "every upstream must
succeed."

**Parameters:**

| Param | Type | Default | Valid values |
|---|---|---|---|
| `trigger_rule` | string | `"all_success"` | `all_success`, `all_failed`, `all_done`, `all_skipped`, `all_done_setup_success`, `one_success`, `one_failed`, `one_done`, `none_failed`, `none_failed_min_one_success`, `none_skipped`, `always` |

**How it works:**
- A task with **no** upstream dependencies is always immediately ready, whatever
  `trigger_rule` you set — the rule only matters once there's at least one
  upstream edge.
- `always` runs a task unconditionally — including if every upstream failed or was
  skipped. Handy for cleanup/notification steps.
- `none_failed_min_one_success` is stricter than `none_failed`: it additionally
  requires at least one upstream to have actually succeeded, not just "no
  failures." A branch where every upstream was skipped satisfies `none_failed` but
  not `none_failed_min_one_success`.
- `all_done_setup_success` is `all_done` plus requiring any upstream `is_setup`
  tasks to have succeeded (feature 20).
- Type your `trigger_rule` string carefully — an unrecognized value quietly falls
  back to `all_success` semantics rather than raising an error, which can look
  like a logic bug in your DAG rather than a typo.

**Example:**
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

**What it does:** Gates a task on the state of the *same* task (or its
downstream) from the DAG's previous run — useful for keeping sequential runs from
racing ahead of unfinished prior work.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `depends_on_past` | boolean | `False` | Blocks until the same task in the previous run succeeded. |
| `wait_for_downstream` | boolean | `False` | Blocks until every downstream task from the previous run reached a terminal state. |

**How it works:**
- Both flags compare against the *immediately preceding* run of the same DAG, not
  "the last successful run." If there is no previous run to compare against (e.g.
  this is the DAG's first run), the check is effectively a no-op.
- `wait_for_downstream=True` on a task with no downstream tasks has nothing to
  wait on — it behaves as if it were `False`.
- Both can be set once via `default_args` and overridden per task, using the same
  inheritance pattern as retries (feature 13).
- Combining `depends_on_past=True` with `max_active_runs=1` effectively forces
  fully serial execution of that task's lineage across runs.

**Example:**
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

**What it does:** Kills a single task attempt if it runs past a set number of
seconds.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `execution_timeout` | integer (seconds) | falls back to the deployment's global default task timeout | Note the parameter name — no `_seconds` suffix, even though it maps internally to a `_seconds` column. |

**How it works:**
- If unset, a task isn't unbounded — it falls back to the deployment's globally
  configured default task timeout.
- On expiry, the task is hard-killed (not a cooperative cancellation) — anything
  it was mid-write on may be left partially done if it wasn't written atomically.
- A timeout counts as a normal failure for retry purposes — set `retries` if you
  want a timed-out attempt to get another chance.
- This is a task-level timeout, independent of the DAG-level
  `dagrun_timeout_seconds` (feature 6) — a task can time out well before the
  whole run's timeout is reached.

**Example:**
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

**What it does:** Flags an SLA miss if a task hasn't reached a terminal state
within a set number of seconds of the DAG run's logical date. Detection only —
never kills or fails the task.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `sla` | integer (seconds) | `None` | Note the parameter name — maps internally to `sla_seconds`. |

**How it works:**
- The clock starts at the DAG run's **logical date**, not the task's own start
  time — a task scheduled later in a chain effectively has less of its own SLA
  budget left, since earlier tasks already consumed part of the same window.
- You get at most one SLA-miss record per task instance, no matter how many times
  it's re-evaluated on later scheduler ticks.
- There's no separate per-task SLA-miss callback — a miss reuses the **DAG's**
  `on_sla_miss_callback` (feature 7), falling back to the DAG's
  `on_failure_callback` if no SLA callback is set.

**Example:**
```python
critical_step = PythonOperator(
    task_id="critical_step",
    python_callable=lambda: None,
    sla=120,   # flag an SLA miss if not done within 2 minutes of the run's logical date
)
```

---

### 18. Concurrency & pools

**What it does:** `pool` assigns a task to a named, shared concurrency budget;
`task_concurrency` caps how many instances of that specific task can run at once
across all active runs of its DAG.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `pool` | string | `"default"` | Must reference an existing pool (created by an admin) to have a real effect. |
| `task_concurrency` | integer | `None` (no cap) | Scoped to one `(dag_id, task_id)` pair, across all its runs. |

**How it works:**
- The `"default"` pool is seeded with 128 slots system-wide — every task that
  doesn't opt into a different pool shares that same budget with everything else
  in the deployment.
- Assigning a `pool` name that doesn't exist yet doesn't create it — ask an admin
  to create the pool first if you need a real, smaller concurrency ceiling.
- Pool slots are shared across **all** DAGs referencing the same pool name — a
  very active DAG can crowd out a lower-volume DAG sharing the same pool.
- `task_concurrency` is independent of, and applies simultaneously with,
  `max_active_tasks` (feature 5) — a task needs a free slot under both (plus its
  pool) to be dispatched.
- Hitting a limit never fails or skips a task — it just waits, still `scheduled`,
  for a slot to open up.

**Example:**
```python
heavy_task = PythonOperator(
    task_id="heavy_transform",
    python_callable=lambda: None,
    pool="etl_heavy",        # ask an admin to create this pool for a real cap
    task_concurrency=2,      # never more than 2 concurrent instances of THIS task_id
)
```

---

### 19. Prioritization

**What it does:** Ranks tasks against each other when there's contention for
dispatch — under normal, uncongested conditions it has no visible effect.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `priority_weight` | integer | `0` | Higher dispatches first under contention. Can be negative. |
| `weight_rule` | string | `"absolute"` | `absolute`, `upstream`, or `downstream`. |

**How it works:**
- Dispatch order under load is highest `priority_weight` first. Under system
  load-shedding, low or negative weights get dropped before high ones.
- `weight_rule="upstream"`: effective weight = this task's own weight **plus the
  sum of every ancestor's own weight**, computed recursively — a task deep in a
  long chain automatically inherits a compounding boost.
- `weight_rule="downstream"`: mirror image — effective weight = own weight plus
  the sum of every descendant's weight — a task gating a large fan-out
  automatically gets a bigger boost.
- This computation is a real dependency-graph walk performed on every planning
  pass for any DAG using a non-`"absolute"` rule — cheap for normal-sized DAGs,
  but genuine work, not a cached one-time value.

**Example:**
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

### 20. Lifecycle role: setup & teardown

**What it does:** Marks a task as preparing resources (`is_setup`) or cleaning
them up (`is_teardown`). Teardown tasks get special treatment — they're triggered
once every "real work" task in the run is done, regardless of how those tasks
turned out.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `is_setup` | boolean | `False` | Enables the `all_done_setup_success` trigger rule for downstream tasks. |
| `is_teardown` | boolean | `False` | Excludes the task from normal dependency-based planning; triggered in bulk once work tasks finish. |

**How it works:**
- Teardown tasks are never scheduled through the normal dependency-graph pass,
  even once their upstreams are satisfied — they're only triggered later, in bulk,
  once every non-teardown task has reached a terminal state. In practice this
  means a teardown task runs regardless of whether upstream tasks succeeded,
  failed, or were skipped.
- Because of that, it's common (though not required) to declare
  `trigger_rule="all_done"` on a teardown task, just to document the intent
  clearly in code.
- `is_setup=True` doesn't change scheduling on its own — a setup task follows the
  normal graph and its own `trigger_rule` like any other task. Its only special
  effect is enabling `all_done_setup_success` for tasks downstream of it.
- Setting both `is_setup` and `is_teardown` on the same task isn't meaningful —
  the teardown behavior takes over.

**Example:**
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
        trigger_rule="all_done",   # documents intent
    )

    spin_up >> process >> tear_down
```

---

### 21. Per-task callbacks

**What it does:** Fires an alert per individual task instance's own outcome —
success, failure, retry, or skip — independent of the DAG-level callbacks in
feature 9.

**Parameters:**

You can configure a task-level callback two ways:

1. **Directly on the operator**: `on_success_callback` / `on_failure_callback` /
   `on_retry_callback` / `on_skipped_callback`, each a plain Python function
   receiving a `context` dict — use this for custom logic.
2. **Declaratively**, via `params={"_callbacks": {event: config}}`, where
   `event` is one of `on_success`, `on_failure`, `on_retry`, `on_skipped`, and
   `config` is a dict describing an email/Slack/HTTP webhook/PagerDuty alert —
   use this when you want a built-in channel without writing any Python (see
   Part 7 for the exact shape of each channel's config).

| Event | Fires when |
|---|---|
| `on_success` | The task instance succeeds. |
| `on_failure` | The task instance ends in terminal failure (fires once, on the final attempt — not on every retry). |
| `on_retry` | A failed attempt is being retried. |
| `on_skipped` | The task itself raises a voluntary skip (feature 34) or a soft-fail sensor times out. |

**How it works:**
- Use the direct `on_X_callback=my_function` kwargs for custom Python logic
  (log something, call an internal API, write a task note, etc.).
- Use the `params={"_callbacks": {...}}` dict form for a built-in
  email/Slack/HTTP webhook/PagerDuty alert with no custom code.
- If you don't set a given event's callback, nothing is added for that event.
- These stack with DAG-wide `default_args` the same way retries do (feature 13) —
  a task-level callback wins; otherwise the DAG's default (if any) is inherited.
- `on_skipped` only fires for the task's *own* terminal skip — it does not fire
  for a task that was skipped by branch evaluation (feature 33) or by
  trigger-rule skip-propagation (feature 14) — those are considered "never
  actually executed," so there's no outcome to alert on.
- `on_execute_callback` (fires when a task starts) is accepted syntactically but
  has no observable effect today — it isn't part of this event set.
- If you need a DAG-wide "final outcome, whatever it was" alert, add a dedicated
  notifier task with `trigger_rule="all_done"` (feature 46) rather than relying on
  a callback.

**Example:**
```python
def log_retry(context):
    print(f"retrying: {context['task_id']}")

flaky_call = PythonOperator(
    task_id="flaky_call",
    python_callable=lambda: None,
    retries=2,
    on_retry_callback=log_retry,
    params={
        "_callbacks": {
            "on_failure": {"type": "slack", "connection_id": "slack_alerts_webhook"},
        }
    },
)
```

---

### 22. Environment control

**What it does:** Controls what a Python/Bash task's subprocess actually sees:
extra environment variables, which interpreter runs it, and (for
`PythonVirtualenvOperator`) which managed virtualenv it runs under.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `env` | dict `{name: value}` | `None` | Extra environment variables for the task's subprocess. |
| `append_env` | boolean | `True` | `True` = layer `env` on top of the default safe allowlist. `False` = only the bare essentials plus your `env`. |
| `python` (`ExternalPythonOperator`) | string (absolute path) | none | Run under a specific pre-provisioned interpreter. |
| `venv` (`PythonVirtualenvOperator`) | string, matches `[A-Za-z0-9_-]+` | none | Name of an already-built managed environment. |
| `requirements` (`PythonVirtualenvOperator`) | list of strings | none | Airflow-style pip requirement list; derives a deterministic environment name. Exactly one of `venv`/`requirements` must be set. |

**How it works:**
- By design, tasks never inherit the orchestrator's own environment — you get a
  small fixed allowlist (`PATH`, `HOME`, `TZ`, `LANG`, `LC_ALL`,
  `DAGS_REPO_PATH`, `PYTHONPATH`) by default. Orchestrator secrets are never part
  of that list, so there's no way to leak them through `env=`, even by naming them
  explicitly.
- `append_env=False` gives a "pristine" subprocess: only the bare essentials
  (`PATH`, `HOME`, `DAGS_REPO_PATH`, `PYTHONPATH`) plus whatever you put in `env=`
  — note that even in pristine mode, `TZ`/`LANG`/`LC_ALL` are dropped unless you
  set them yourself.
- `python=`/`venv=` re-import your DAG module fresh inside the target
  interpreter (no pickling of the callable) — the target interpreter must be able
  to `import dag_parser` and any third-party packages your callable needs.
- A named `venv=` environment must already be built by an admin before any task
  can use it — a DAG referencing one that was never built will fail when the task
  actually runs, not at ingestion time.

**Example:**
```python
# env + append_env — overlay mode (default): allowlist + your vars
BashOperator(
    task_id="with_extra_env",
    bash_command="echo $STAGE $PATH",
    env={"STAGE": "staging"},
    append_env=True,   # default; PATH/HOME/TZ/... still present alongside STAGE
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
    venv="pandas_env",   # must already be built by an admin
)

# PythonVirtualenvOperator — Airflow-style requirements (auto-derived env name)
PythonVirtualenvOperator(
    task_id="run_with_requirements",
    python_callable=lambda: None,
    requirements=["pandas==2.2.0", "requests==2.31.0"],
)
```

---

## Part 4 — Scheduling & Triggering Runs

### 23. Automatic runs

**What it does:** How PI-Flow actually turns your `schedule`/`timetable` (Part 1)
into real `dag_run` rows, with no action needed on your part.

**How it works:**
- `schedule="@once"` is special — it creates exactly one run, at `start_date`, the
  very first time the DAG is seen with no existing runs, and never again after
  that.
- `{{ data_interval_start }}` / `{{ data_interval_end }}` are only populated for
  real cron/descriptor schedules (and `@once`). A DAG using a named `timetable=`
  or with no schedule at all gets `NULL` there — avoid templating those fields on
  a timetable-scheduled DAG.
- `max_active_runs` (feature 5) is enforced at the moment a new automatic run
  would be created — if the DAG is already at its cap, the scheduler just tries
  again on the next poll rather than queuing up a backlog of "missed creations."

**Example:**
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

**What it does:** How a DAG gets triggered on demand (via the UI or API) with a
custom `conf`, and how that interacts with your declared `params` schema (feature
8). This section covers what to know as a DAG author; the actual API call is
documented separately.

**How it works:**
- A paused DAG cannot be manually triggered — it must be unpaused first.
- If you supply values through the typed `params` field, they're validated
  against your declared schema (required/type/enum/range). If you supply raw
  `conf` instead, it's accepted with no schema validation — useful as a
  workaround for the `number`/`array`/`object`/`date`/`datetime` type-validation
  gap noted in feature 8.
- Triggering is idempotent by execution date — firing the same manual trigger
  twice for the same resolved execution date returns the *same* run rather than
  creating a duplicate.
- A manually-triggered run is pinned to the DAG's latest version at the moment of
  triggering (the same version-pinning behavior mentioned in Part 0) — it won't
  pick up a newer version if the DAG file changes again before the run starts.

**Example:**
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

**What it does:** Creates runs for every scheduled interval in a historical date
range in one request, using the DAG's own cron schedule to compute each
execution date.

**How it works:**
- Requires a real cron/descriptor `schedule` — DAGs with no schedule, `"@once"`,
  or a named `timetable=` can't be backfilled through this mechanism, even though
  they schedule automatic runs fine.
- There's a hard cap on how many runs a single backfill request can create — a
  very wide date range on a frequent schedule should be split into smaller
  requests.
- An optional "reset existing" mode deletes runs already in the requested range
  before recreating them — this is a real, irreversible delete of run history for
  that range, so use it deliberately.
- Backfill-created runs use their own `run_type` and a deterministic run ID, so
  re-running the same backfill request without resetting is safe — runs that
  already exist in the range are simply left alone.
- Backfill run creation respects the DAG's `max_active_runs` cap (feature 5) the
  same way ordinary scheduled runs do — if creating the next computed run would
  exceed the cap, it's held back and created once a slot frees up.

**Example (conceptual — issued via API/UI, not DAG-file syntax):**
```python
# The DAG itself needs nothing special beyond a real cron schedule:
with DAG(
    dag_id="daily_aggregation",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,   # catchup only affects auto-scheduling, not backfill
) as dag:
    ...
```

---

### 26. Dataset-driven trigger

**What it does:** A DAG scheduled with `schedule=[Dataset(...)]` (feature 2) runs
automatically when its upstream dataset(s) receive new data.

**How it works:**
- Only one dataset-triggered run is ever in flight at a time — while a run is in
  progress, further dataset updates don't queue up additional runs; they're
  coalesced into whatever the next evaluation picks up after the current run
  finishes.
- The "since last update" clock is anchored to the last **successful** run — if
  the most recent run failed, the same dataset events that triggered it are still
  considered new on the next check, so the DAG gets retried against the same
  updates rather than waiting for fresh ones.
- `trigger_type` is matched exactly against lowercase `"any"` or `"all"` — double
  check spelling/case if a dataset-driven DAG never seems to fire.
- A task produces a dataset event by declaring `outlets=[Dataset(...)]` and
  completing successfully — a failed producer task never emits an event, so a
  failure upstream naturally withholds downstream dataset-triggered DAGs.

**Example:**
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

**What it does:** `TriggerDagRunOperator` lets one task start a run of a
*different* DAG, optionally waiting for it to finish before the parent task
completes.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `trigger_dag_id` | string | *required* | Must reference an existing, unpaused DAG. |
| `conf` | dict | `{}` | Passed straight into the child run's `conf` — not validated against the child's `params` schema. |
| `wait_for_completion` | boolean | `False` | See below — does not hold a worker slot. |
| `allowed_states` | list of strings | `["success"]` | States that count as a successful wait. |
| `failed_states` | list of strings | `["failed"]` | States that end the wait as a failure. |
| `poke_interval` | integer (seconds) | `10` | How often the background reconciler checks the child run's state while `wait_for_completion=True`. |

**How it works:**
- The child DAG must exist and be unpaused, or the task fails immediately with a
  clear error.
- `conf` is **not** validated against the child DAG's `params` schema the way a
  UI/API manual trigger is (feature 24) — a typo'd key or wrong type surfaces
  later, inside the child run, rather than up front.
- `wait_for_completion=True` does not hold a worker slot — the parent task moves
  into a lightweight waiting state, and a background scheduler process checks the
  child's state and flips the parent to success/failed later. This is safe even
  on a small cluster with deeply nested trigger chains.
- If the child ends up in a state that's in neither `allowed_states` nor
  `failed_states`, the parent simply keeps waiting — bound only by the DAG-level
  `dagrun_timeout_seconds` (feature 6) if you've set one.
- The generated child run ID follows a fixed pattern derived from the parent run —
  you don't choose it; read it back via XCom if a downstream task needs it.

**Example:**
```python
from dag_parser.dynamic.dag_context import TriggerDagRunOperator

with DAG(dag_id="parent_pipeline", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="child_pipeline",
        conf={"batch_date": "{{ .DS }}"},   # not validated against child's params schema
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )
```

---

### 28. Time-based defer

**What it does:** Lets a task free its worker slot entirely while it waits for a
condition — a timer, a specific moment, or an HTTP check — instead of holding the
slot the whole time.

**Parameters (`self.defer(...)`):**

| Param | Type | Notes |
|---|---|---|
| `trigger` | `TimeDeltaTrigger(seconds)` / `DateTimeTrigger(moment)` / `HttpTrigger(endpoint, ...)` | The condition to wait on. |
| `method_name` | string | Method to call on the operator once the trigger fires (commonly `"execute_complete"`). |
| `timeout` | integer (seconds) | Optional — fail the task if the trigger never fires within this window. |

**How it works:**
- `TimeDeltaTrigger`'s elapsed time is measured from the moment `defer()` was
  called, not from the run's logical date or the task's start.
- `DateTimeTrigger(moment)` treats a timezone-naive `datetime` as UTC — pass a
  timezone-aware datetime or an explicit UTC string if you need precision.
- `HttpTrigger` treats a network error as "not fired yet," not a failure — it
  keeps polling silently. Always set a `timeout` if the endpoint might be
  unreachable indefinitely.
- A ready-made `TimeSensor` (feature 49) wraps "wait until a specific time of
  day" without you needing to write your own `defer()` logic.

**Example:**
```python
from dag_parser.dynamic.dag_context import BaseOperator, TimeDeltaTrigger

class DelayedStep(BaseOperator):
    operator_name = "DelayedStep"

    def execute(self, context):
        self.defer(
            trigger=TimeDeltaTrigger(300),
            method_name="execute_complete",
            timeout=600,   # give up (fail) if it hasn't fired within 10 minutes
        )

    def execute_complete(self, event=None):
        print("delay complete")
```

---

## Part 5 — Dependencies & Flow Control

### 29. Declaring edges

**What it does:** Wires task dependencies using `>>` (downstream), `<<`
(upstream), or the explicit `set_upstream()`/`set_downstream()` methods.

**How it works:**
- `a >> b >> c` chains left to right (`b` starts after `a`; `c` starts after
  `b`). `a << b << c` reads "a depends on b, which depends on c," producing
  `c -> b -> a`.
- Fan-out/fan-in with a list on **one** side works cleanly: `a >> [b, c]`
  (one-to-many) and `[b, c] >> d` (many-to-one) are both fully supported.
- A list on **both** sides (`[a, b] >> [c, d]`) is not supported — expand it into
  explicit pairs instead (see the example).
- These calls only build the dependency graph — the actual ready/blocked/skip
  decision for each task is governed by its `trigger_rule` (feature 14).

**Example:**
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

    # Fan-out: one task to many (list on ONE side)
    transform >> [load_a, load_b]

    # Fan-in: many tasks to one (list on ONE side)
    [load_a, load_b] >> report

    # A list on BOTH sides isn't supported. Expand it explicitly instead:
    # for src in [load_a, load_b]:
    #     for dst in [report, some_other_task]:
    #         src >> dst
```

---

### 30. Edge labels

**What it does:** Annotates an edge with a short text label for the task-graph
visualization — purely cosmetic, no effect on execution.

**Parameters:**

| Param | Type | Notes |
|---|---|---|
| `Label("text")` | string | Place it between two tasks in a `>>`/`<<` chain, or pass `label=` to `set_downstream`/`set_upstream`. |

**How it works:**
- Chain it directly into a dependency declaration:
  `task_a >> Label("on_success") >> task_b`. The explicit method form,
  `task_a.set_downstream(task_b, label="on_success")`, produces the same result
  if you prefer it.
- Labels only affect what's displayed in the task-graph view; they never
  influence trigger-rule evaluation, scheduling, or execution order.

**Example:**
```python
from dag_parser.dynamic.dag_context import Label

with DAG(dag_id="edge_labels_demo", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:
    check = PythonOperator(task_id="check", python_callable=lambda: None)
    proceed = PythonOperator(task_id="proceed", python_callable=lambda: None)

    check >> Label("on_success") >> proceed
```

---

### 31. Convergence control

**What it does:** Explains how a "join" task — one with multiple upstream
edges — decides whether it's ready, using its `trigger_rule` (feature 14).

**How it works:**
- Declaring edges (feature 29) only builds the graph — it never implies join
  semantics on its own. A task with 3 upstream edges and the default
  `trigger_rule="all_success"` requires all 3 to succeed; use `one_success` (or
  another applicable rule) if any one finishing is enough.
- The classic pattern: after a `BranchPythonOperator` skips every path but one, a
  naive `all_success` join downstream of all branches would never be satisfied
  (skipped branches aren't "success"). Use `none_failed` or
  `none_failed_min_one_success` on the join so the unchosen branches' skip state
  doesn't permanently block it.
- A converging task with an `all_success`/`all_done`-style rule effectively waits
  for every incoming edge's task to reach a terminal state, even if most of them
  finished quickly and one is slow.
- Mixing trigger rules across parallel join tasks fed by the same upstream
  fan-out is fine and common — e.g. one join using `one_success` for a "fast
  path" notification, and another using `all_success` as the real completion
  gate.

**Example:**
```python
with DAG(dag_id="convergence_demo", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    choose_path = PythonOperator(task_id="choose_path", python_callable=lambda: None)
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

### 32. Dynamic task mapping

**What it does:** Fans a single task definition out into N task instances at
*runtime*, one per element of an iterable — a literal list or an upstream task's
XCom output.

**Parameters:**

| Method | Notes |
|---|---|
| `.partial(**fixed_kwargs)` | Values shared by every expanded instance. |
| `.expand(**kwargs)` | One or more keyword arguments, each an iterable — a literal list or an `XComArg` reference to an upstream task's return value. |

**How it works:**
- Passing a single keyword argument to `.expand()` produces one mapped instance
  per element, in order.
- Passing **multiple** keyword arguments produces the cross-product of every
  combination — `.expand(a=[1, 2], b=["x", "y"])` produces 4 instances:
  `(1,"x")`, `(1,"y")`, `(2,"x")`, `(2,"y")`.
- If the resolved iterable has zero elements, the whole mapped task is skipped
  (not "zero instances silently"). Downstream joins should use
  `none_failed`/`none_failed_min_one_success` (features 14/31) so they aren't
  permanently blocked by that.
- When expanding off an upstream task's XCom (`XComArg`), the referenced
  `return_value` can be either a JSON array (one element per instance) or a
  single scalar value, which is automatically treated as a one-element list.
- `.partial(**fixed_kwargs)` values are merged into every expanded instance
  first; each expand key's value is merged on top per instance.
- Retries, timeouts, pool, and trigger rule come from the shared task definition
  — every mapped instance shares the same settings; only the expanded arguments
  differ per instance.

**Example:**
```python
from dag_parser.dynamic.dag_context import XComArg

with DAG(dag_id="fanout_files", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    def list_files():
        return ["file_a.csv", "file_b.csv", "file_c.csv"]

    list_task = PythonOperator(task_id="list_files", python_callable=list_files)

    def process_file(filename, region):
        print(f"processing {filename} in {region}")

    # Two expand keys -> cross-product of filenames x regions
    process = PythonOperator.partial(
        task_id="process_file",
        python_callable=process_file,
    ).expand(
        filename=XComArg("list_files"),   # or a literal list: filename=["a.csv", "b.csv"]
        region=["us-east", "us-west"],
    )

    list_task >> process
```

---

### 33. Branching

**What it does:** `BranchPythonOperator` runs a function whose return value
selects which of its direct downstream tasks actually run; every other direct
downstream task is marked skipped.

**Parameters:**

| Param | Type | Notes |
|---|---|---|
| `python_callable` | callable | Must **return** a `task_id` string, or a list of `task_id`s — never `None`. |

**How it works:**
- The callable must always return something — a single `task_id` or a list of
  them. Forgetting to `return` (or returning `None`) fails the task outright,
  rather than skipping everything.
- A returned `task_id` that isn't actually a direct downstream of the branch task
  is simply ignored (logged, not an error) — that path just never gets selected.
- Skip-cascading downstream of an unselected branch is trigger-rule aware — a
  join using `none_failed_min_one_success` fed by both the chosen and unchosen
  branch is correctly left alone (not skipped) as long as the chosen branch can
  still succeed. Tasks with `trigger_rule="always"` are never cascade-skipped.
- The branch decision is pushed as the task's normal `return_value` XCom — you
  can also read it yourself downstream via `xcom_pull` if useful.

**Example:**
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

**What it does:** Raising `PiFlowSkip("reason")` inside a Python callable marks
that task instance `skipped` instead of `failed` — the idiomatic way to say
"there was nothing to do this run."

**How it works:**
- No retry is consumed, and `on_failure_callback` never fires for a voluntary
  skip — it goes through the skip path, firing `on_skipped_callback` instead
  (feature 21).
- Downstream tasks see this exactly like any other `skipped` state for
  trigger-rule purposes — a default `all_success` downstream task will be
  blocked/cascade-skipped just as if a branch had explicitly skipped it; use
  `none_failed`-family rules (feature 14) on anything that should still proceed.
- `PiFlowSkip` only works from inside the callable's own exception flow — raising
  it from a separately-spawned thread, or after the callable has already returned
  normally, has no effect.
- The reason string is written to the task's log but isn't stored in a separate
  queryable field — push it via `ti.xcom_push` or a task note first if you need
  it available for later reporting.

**Example:**
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

## Part 6 — Data Passing & Templating

### 35. XCom (Python)

**What it does:** The mechanism for passing small values between tasks. Inside a
Python callable, `ti.xcom_push(key, value)` writes a value; `ti.xcom_pull(...)`
reads one back.

**Parameters (`xcom_pull`):**

| Param | Type | Default | Notes |
|---|---|---|---|
| `task_ids` | string, or list of strings | the calling task's own key | A list returns a list of values in the same order. |
| `key` | string | `"return_value"` | The auto-pushed key for whatever a callable returns. |
| `map_indexes` | int, `"all"`, or list | matches caller's own map index if mapped | `"all"` returns every mapped slice's value as a list. |

**How it works:**
- Every non-`None` return value from a callable is automatically pushed to the
  `"return_value"` key on success — you don't need to call `xcom_push` yourself
  just to pass your function's result downstream.
- Returning `None` (or nothing) from a callable pushes **nothing** — a
  downstream `xcom_pull` for that key returns `None`. Returning `""` (empty
  string) *does* get pushed, and is distinct from `None`.
- Values are always JSON-encoded — pushing the integer `5` and the string `"5"`
  are stored, and round-trip, as distinct types.
- If a mapped task instance (`map_index >= 0`) calls `xcom_pull(task_ids=X)` with
  no explicit `map_indexes`, it defaults to pulling the matching index from `X` —
  pass `map_indexes="all"` explicitly if you want every slice's value as a list.
- `do_xcom_push=False` suppresses only the automatic `return_value` push — it
  never blocks an explicit `ti.xcom_push(...)` call inside your callable.

**Example:**
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
    print(count, upstream_return, many)

extract_task = PythonOperator(task_id="extract", python_callable=extract, provide_context=True)
consume_task = PythonOperator(task_id="consume", python_callable=consume, provide_context=True)
extract_task >> consume_task
```

---

### 36. TaskFlow XCom refs

**What it does:** The `@task` decorator lets you call one task-decorated function
and pass its result directly into another — the dependency edge *and* the XCom
pull are wired automatically, no manual `xcom_pull`/`xcom_push` needed.

**How it works:**
- Only works when you pass the upstream result as a **top-level** argument — an
  `XComArg` nested inside a list or dict you construct yourself (e.g.
  `transform(data=[extract_a(), extract_b()])`) isn't detected. Pass each
  upstream result as its own top-level argument instead.
- Only `op_kwargs` are populated by `@task` calls — positional arguments to your
  decorated function are re-bound to their parameter names internally.
- Resolved values are opportunistically JSON-decoded — if the upstream task
  returned a plain string that also happens to be valid JSON (e.g. `"123"`), your
  downstream function receives the decoded type (`int 123`), not the original
  string.
- Calling the same `@task`-decorated function more than once in a DAG
  auto-suffixes the `task_id` (`_2`, `_3`, ...) so each call gets a distinct task.
- TaskFlow-inferred edges and explicit `>>` edges coexist fine — you can add
  extra manual dependencies alongside the automatic ones.

**Example:**
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

### 37. Auto XCom from operators

**What it does:** Several built-in operators automatically push a `return_value`
XCom on success, without any Python code — useful for wiring a non-Python task
straight into a downstream `ti.xcom_pull` or TaskFlow reference.

**How it works, per operator:**

| Operator | What gets pushed |
|---|---|
| `SnowflakeOperator` | The **first result row**, as a JSON object (`{"col": value, ...}`). |
| `BashOperator` | Raw **stdout**, as a plain string. |
| SSH task | The remote command's **combined stdout+stderr**, as one string. |
| S3→Redshift loader | `{"rows_loaded": N}`. |
| `SlackAPIPostOperator` | `{"ts": "<message timestamp>"}` (token-auth connections only). |

- The auto-push only happens if the task actually produced a non-empty result —
  a Bash command with genuinely empty stdout, or a query returning zero rows,
  produces **no** XCom row at all (`xcom_pull` returns `None`, not `""`).
- SQL-family operators only capture the **first** row — a query returning
  multiple rows has everything after row 1 silently left out of `return_value`.
  Use a Python task if you need more than one row passed downstream.
- Bash's `return_value` is the raw stdout string, not JSON-parsed — if your
  script prints JSON, a downstream Python task needs to `json.loads()` it itself.
- An SSH task's auto-XCom mixes stdout and stderr together into one string —
  unlike `BashOperator`, which keeps them separate.

**Example:**
```python
# SQL: first row auto-pushed as {"id": 1, "name": "acme"} etc.
get_customer = SnowflakeOperator(
    task_id="get_customer",
    connection_id="warehouse_snowflake",
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

### 38. Go templating (non-Python operators)

**What it does:** Bash/SQL/HTTP/SSH-family operators (everything that isn't a
Python-family operator) have their `params` rendered through a lightweight
templating pass before execution.

**Available tokens:**

| Token | Meaning |
|---|---|
| `{{ .DS }}` | Execution date, `YYYY-MM-DD`. |
| `{{ .TS }}` | Execution timestamp, `YYYY-MM-DDTHH:MM:SS`. |
| `{{ .DSNodash }}` / `{{ .TSNodash }}` | Same, without separators. |
| `{{ .ExecutionDate }}` / `{{ .LogicalDate }}` | Full ISO8601. |
| `{{ .DagID }}` / `{{ .TaskID }}` / `{{ .RunID }}` / `{{ .TryNumber }}` / `{{ .MapIndex }}` | Identity fields. |
| `{{ .Params.key }}` / `{{ .Conf.key }}` | Your declared params / the triggering run's `conf`. |
| `{{ .Var.key }}` | A PI-Flow Variable (feature 40). |
| `ds_add .DS <days>` | Add/subtract days from a `YYYY-MM-DD` string. |
| `ds_format .DS "<layout>"` | Reformat a `YYYY-MM-DD` string (Go date-layout syntax). |
| `ts_add .ExecutionDate "<duration>"` | Add a duration to a full timestamp. |

**How it works:**
- `.DS`/`.TS` are computed in the DAG's own `timezone` (feature 3), not UTC —
  even though storage is UTC, a DAG with `timezone="America/New_York"` sees its
  local calendar date here.
- A typo in the token structure itself (e.g. misspelling `.Params`) fails the
  task loudly. A typo in a *key* inside `.Params`/`.Var` (e.g. `.Params.typo`)
  just renders as an empty string — these look similar but behave very
  differently, so check carefully.
- The three macros (`ds_add`, `ds_format`, `ts_add`) return the original,
  unmodified string on bad input rather than erroring — double check you're
  feeding `ds_add`/`ds_format` a `YYYY-MM-DD` string and `ts_add` a full
  timestamp.
- A param with no `{{ ` anywhere in it skips rendering entirely, at no cost.

**Example:**
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

### 39. Jinja2 templating (Python operators)

**What it does:** Python-family operators (`PythonOperator`,
`ExternalPythonOperator`, `PythonVirtualenvOperator`) render their
`op_args`/`op_kwargs`/`templates_dict` with a full Jinja2 engine, giving access to
the complete Airflow-style context.

**Available context:**

| Name | What it provides |
|---|---|
| `ds` / `ts` | Execution date / timestamp strings. |
| `params` / `conf` | Your declared params / the triggering run's `conf`. |
| `var.value.x` / `var.json.x` | A Variable as a plain string / JSON-parsed. |
| `macros.*` | `ds_add`, `ds_format`, `datetime`, `timedelta`. |
| `dag` / `task` | Live mock objects (`dag.dag_id`, `task.retries`, ...). |
| `data_interval_start` / `data_interval_end` | Real `datetime` objects, when the schedule produces one (feature 23). |

**How it works:**
- Only fields the parser tracks as "template fields" get Jinja treatment — for
  Python operators that's `op_args`/`op_kwargs`/`templates_dict`. Non-Python
  operators use Go templating (feature 38) for their entire params instead.
- `var.value.x` / `var.json.x` raise a clear error on a missing key — unlike the
  Go side's silent-empty behavior (feature 38), a typo'd variable name here fails
  the task loudly.
- `var.json.x` parses the variable's stored value as JSON — use `var.value.x`
  instead for a plain string.
- `data_interval_start`/`data_interval_end` are only real `datetime` objects for
  cron/descriptor-scheduled runs (feature 23) — referencing them on a manually
  triggered, timetable-scheduled, or dataset-triggered run fails loudly rather
  than silently returning `None`.

**Example:**
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

### 40. Variables & Connections

**What it does:** **Variables** are a flat, global key-value config store,
readable from both Go templates (feature 38) and Python Jinja (feature 39).
**Connections** store reusable external credentials, referenced by name from
operators that need them.

**How it works:**
- Every task gets every Variable, with no per-DAG or per-role scoping — treat
  Variables as workspace-wide configuration, not a place to isolate team-specific
  secrets.
- Encrypted Variables are decrypted before ever reaching a task — encryption only
  protects the value at rest and in the Admin UI's list view; a task that
  references the key still sees the plaintext.
- Connections are looked up by the executor directly via `connection_id`, not
  through templating — you can't read a connection's password with
  `{{ .Var.some_conn_password }}`; you pass `connection_id` to an operator that
  knows how to resolve it (Snowflake, SQL, S3, etc.).
- A `connection_id` that doesn't exist only fails when the task actually runs,
  not when the DAG is parsed — double-check the name if a task suddenly starts
  failing with a "connection lookup failed" error.
- Both Variables and Connections are created/managed through the Admin/Config UI
  or API — there's no DAG-file syntax to *create* one, only to *reference* one by
  key/`connection_id`.

**Example:**
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
    connection_id="snowflake_prod",   # must already exist
    sql="SELECT * FROM sales.orders WHERE order_date = '{{ .DS }}'",
)
```

---

## Part 7 — Alerting & Notifications

All four channels below are configured the same way: a plain dict under
`params={"_callbacks": {event: {...}}}` on a task (feature 21) — `event` is one
of `on_success`, `on_failure`, `on_retry`, `on_skipped`.

### 41. Email alerts

**Parameters (`config` dict):**

| Field | Type | Default | Notes |
|---|---|---|---|
| `type` | string | *required* | `"email"` |
| `to` | list of strings | *required* | Recipient addresses. |
| `subject` | string | *required* | Supports 4 literal tokens (see below). |
| `html_content` | string | `""` | Supports the same 4 tokens. |

**How it works:**
- `to` and `subject` are both required — there's no default subject line.
- `subject` and `html_content` support exactly four literal placeholders:
  `{{dag_id}}`, `{{task_id}}`, `{{run_id}}`, `{{event}}` (with or without inner
  spaces) — this is a simple find-and-replace, not a full templating engine, so
  Jinja/Go-style expressions won't render here.
- Requires SMTP to be configured for the deployment — if it isn't, the alert is
  silently not sent (the callback is still marked processed).
- Fires per task instance, once per triggering event — a mapped task (feature
  32) sends its own email per `map_index` that reaches the configured event.

**Example:**
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

### 42. Slack alerts

**Parameters (`config` dict):**

| Field | Type | Default | Notes |
|---|---|---|---|
| `type` | string | *required* | `"slack"` |
| `connection_id` | string | *required* | Must be a **webhook-mode** Slack connection. |

**How it works:**
- Use a Slack connection created in **webhook** mode specifically for alerting.
  If your primary Slack connection uses a bot token (for `SlackAPIPostOperator`
  task usage, feature 60), create a separate webhook-mode connection for this
  callback channel.
- The message content isn't configurable here — it's a fixed-format summary
  (DAG/Task/Run/Event). If you need custom message text, use the HTTP webhook
  channel (feature 43) or a dedicated `SlackAPIPostOperator` task instead
  (feature 60).
- A non-200 response from Slack is treated as a failed send and logged — there's
  no automatic retry for a transient Slack outage.

**Example:**
```python
notify_failure = PythonOperator(
    task_id="risky_step",
    python_callable=lambda: None,
    params={
        "_callbacks": {
            "on_failure": {
                "type": "slack",
                "connection_id": "slack_alerts_webhook",  # must be webhook-mode
            }
        }
    },
)
```

---

### 43. HTTP webhook alerts

**Parameters (`config` dict):**

| Field | Type | Default | Notes |
|---|---|---|---|
| `type` | string | *required* | `"http_webhook"` |
| `url` | string | *required* | Supports the same 4 tokens as email. |
| `headers` | dict | `{}` | Put any auth headers here — no connection lookup for this channel. |
| `body` | string | a default JSON payload | Supports the same 4 tokens. |

**How it works:**
- Both `url` and `body` support the same 4-token replacement as email
  (`{{dag_id}}`, `{{task_id}}`, `{{run_id}}`, `{{event}}`) — you can parameterize
  the URL path itself, not just the body.
- There's no connection lookup for this channel — bake any bearer token/API key
  directly into `headers`.
- Any 2xx response counts as success; anything else is treated as a failed
  delivery.
- If you omit `body`, a default JSON payload (`dag_id`, `task_id`, `run_id`,
  `map_index`, `event`, `message`, `timestamp`) is sent instead.

**Example:**
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

### 44. PagerDuty alerts

**Parameters (`config` dict):**

| Field | Type | Default | Notes |
|---|---|---|---|
| `type` | string | *required* | `"pagerduty"` |
| `connection_id` | string | *required* | Routing key stored under `extra.routing_key`, or as the connection's `password` field. |

**How it works:**
- Severity is automatically derived from the event: `on_failure` → `critical`,
  `on_retry` → `warning`, `on_success` → `info`. This mapping isn't configurable
  per task.
- If you need custom severity logic, use the HTTP webhook channel (feature 43)
  directly against PagerDuty's Events API instead.
- Every alert opens a **new** incident (`event_action: "trigger"`) — there's no
  built-in "resolve" call to auto-close an incident when a retried task later
  succeeds; wire that up yourself as a downstream task if you need it.

**Example:**
```python
notify_failure = PythonOperator(
    task_id="risky_step",
    python_callable=lambda: None,
    params={
        "_callbacks": {
            "on_failure": {
                "type": "pagerduty",
                "connection_id": "pagerduty_prod",
            }
        }
    },
)
```

---

### 45. Event scope

**What it does:** Explains exactly when each callback event fires, at both the
task level (feature 21) and the DAG level (feature 9).

**How it works:**
- **`on_failure`** fires once, on the task's terminal (final) failure — not on
  every retry attempt.
- **`on_retry`** fires once per attempt that's about to be retried.
- **`on_skipped`** only fires for the task's own terminal skip (`PiFlowSkip`, or
  a soft-fail sensor timeout) — it does not fire for a task skipped by branch
  evaluation (feature 33) or trigger-rule skip-propagation (feature 14), since
  those tasks never actually executed.
- At the **DAG level**, only `on_success`, `on_failure`, and `on_sla_miss` exist
  — there's no DAG-level retry or skip event. If you need alerting on a
  partial-failure shape across the whole DAG, attach task-level callbacks to the
  specific tasks you care about, or add a dedicated notifier task.
- Every event fires **per task instance**, including per `map_index` for a
  mapped task — there's no built-in "fire once for the whole mapped group"
  aggregation.
- For a DAG-wide "final outcome, whatever it was" alert, add a dedicated
  notifier task rather than relying on a callback:

**Example:**
```python
with DAG(dag_id="alerting_scope_demo", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    flaky = PythonOperator(
        task_id="flaky_step",
        python_callable=lambda: None,
        retries=3,
        params={
            "_callbacks": {
                # Fires once, on the FINAL failed attempt only
                "on_failure": {"type": "slack", "connection_id": "slack_alerts_webhook"},
                # Fires only if THIS task raises PiFlowSkip itself
                "on_skipped": {"type": "email", "to": ["team@company.com"], "subject": "skipped"},
            }
        },
    )

    # DAG-wide "final outcome" alert — a dedicated task, not a callback
    notify_final = SlackAPIPostOperator(
        task_id="notify_final",
        slack_conn_id="slack_alerts_webhook",
        text="alerting_scope_demo finished: check grid view for final status",
        trigger_rule="all_done",
    )

    flaky >> notify_final
```

---

## Part 8 — Sensors & Waiting

### 46. HttpSensor

**What it does:** Polls an HTTP endpoint until it returns a 2xx status, with an
optional response-body regex check.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `endpoint` | string (URL) | *required* | |
| `connection_id` | string | none | If set, resolves auth (bearer token/basic auth) from a stored HTTP connection. |
| `headers` | dict | `{}` | Merged on top of anything resolved from `connection_id`, or used on its own. |
| `response_check_regex` | regex string | none | Uses Go's RE2 regex syntax (no lookahead/lookbehind/backreferences). |
| `poke_interval` | integer (seconds) | `60` | |
| `timeout` | integer (seconds) | `3600` | Cumulative across the whole wait (see feature 49). |
| `mode` | string | `"poke"` | See feature 50 — HttpSensor's default differs from the other three sensors. |
| `soft_fail` | boolean | `False` | Timeout becomes `skipped` instead of `failed`. |

**How it works:**
- Set `connection_id` to reuse stored credentials for the target endpoint; add
  `headers=` for anything not covered by the connection, or to override it.
- A network/DNS error is treated the same as "condition not met" — it keeps
  retrying at `poke_interval` until `timeout`, rather than failing fast.
- `response_check_regex` only gets evaluated once a 2xx response is actually
  received — a regex syntax error surfaces at that point, not before.
- Response body is capped at 1MB for the regex check.
- Defaults to `mode="poke"`, unlike the other three sensors — set
  `mode="reschedule"` explicitly if you want it to free its worker slot between
  checks.

**Example:**
```python
from dag_parser.dynamic.operators import HttpSensor

wait_for_api = HttpSensor(
    task_id="wait_for_api",
    endpoint="https://api.example.com/v1/status",
    connection_id="internal_api",   # resolves auth from the stored connection
    response_check_regex=r'"status"\s*:\s*"ready"',
    poke_interval=30,
    timeout=1800,
    mode="reschedule",   # override the poke default explicitly
    soft_fail=True,      # timeout -> skipped, not failed
)
```

---

### 47. SqlSensor

**What it does:** Runs a SQL query on every poke and succeeds once the result
has at least one row whose first column is truthy.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `sql` | string | *required* | |
| `connection_id` | string | *required* | The external database connection to run the query against. |
| `poke_interval` | integer (seconds) | `60` | |
| `timeout` | integer (seconds) | `3600` | Cumulative (feature 49). |
| `mode` | string | `"reschedule"` | |

**How it works:**
- `connection_id` determines which external database the query runs against —
  point it at any registered connection whose type supports a SQL query
  (Snowflake, Postgres, MySQL, Redshift, etc.).
- If you need to wait on another DAG/task's state specifically,
  `ExternalTaskSensor` (feature 48) is the purpose-built tool for that instead
  of querying tables directly.
- Truthiness of the first cell: `NULL`, `0`, `""`, `"0"`, and case-insensitive
  `"false"` are falsy; anything else (including a query returning zero rows,
  which behaves the same as a falsy first column) is not a match.
- A malformed SQL string is a genuine error, not "condition not met" — it's
  retried each interval until timeout, then surfaces as a failure.

**Example:**
```python
from dag_parser.dynamic.operators import SqlSensor

wait_for_flag = SqlSensor(
    task_id="wait_for_flag",
    sql="SELECT ready_flag FROM control.batch_status WHERE batch_date = '{{ ds }}'",
    connection_id="warehouse_snowflake",
    poke_interval=60,
    timeout=3600,
    mode="reschedule",
)
```

---

### 48. TimeSensor

**What it does:** Waits until the current wall-clock time reaches or passes a
target time of day.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `target_time` | string, `"HH:MM"` or `"HH:MM:SS"` | *required* | Evaluated in the DAG's own `timezone` (feature 3). |
| `poke_interval` | integer (seconds) | `60` | |
| `timeout` | integer (seconds) | `3600` | Cumulative (feature 49). |
| `mode` | string | `"reschedule"` | |

**How it works:**
- `target_time` is evaluated against the DAG's own `timezone` setting (feature
  3) — a DAG declared `timezone="America/New_York"` with
  `target_time="09:00"` waits for 09:00 US Eastern, whatever timezone the
  underlying infrastructure runs in.
- Always targets **today's** date — a backfill run representing a historical
  date still waits for `target_time` on whatever day it actually executes.
- If the condition is already true when the sensor first checks (e.g. it's
  14:00 and `target_time="09:00"`), it succeeds immediately — there's no
  "wait until tomorrow" behavior.
- Defaults to `mode="reschedule"`, which is efficient for long waits (e.g. "wait
  until market open") since it only claims a worker slot for each brief check.

**Example:**
```python
from dag_parser.dynamic.operators import TimeSensor

with DAG(
    dag_id="regional_market_open",
    schedule="@daily",
    timezone="America/New_York",
    start_date=datetime(2026, 1, 1),
) as dag:

    wait_until_market_open = TimeSensor(
        task_id="wait_until_market_open",
        target_time="09:30:00",   # 09:30 US Eastern, matching the DAG's timezone
        poke_interval=60,
        timeout=3600 * 2,
        mode="reschedule",
    )
```

---

### 49. ExternalTaskSensor

**What it does:** Waits for a task (or an entire DAG run) in another DAG to
reach one of a set of allowed states.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `external_dag_id` | string | *required* | |
| `external_task_id` | string | none | Omit to wait on the whole DAG run's own state instead of one task. |
| `allowed_states` | list of strings | `["success"]` | Case-insensitive match. |
| `failed_states` | list of strings | `["failed"]` | Case-insensitive match; checked first, ends the wait as a failure. |
| `execution_date` | string (ISO or templated) | latest run | Pin to a specific external run rather than always polling the newest one. |
| `poke_interval` / `timeout` / `mode` | — | `60` / `3600` / `"reschedule"` | Same semantics as the other sensors. |

**How it works:**
- Without `execution_date`, it always resolves to the **latest** run of the
  external DAG/task — if your DAG runs more often than the external one, every
  check ends up polling the same external run. Pass `execution_date` explicitly
  if you need a specific, matched run.
- If the external state matches neither `allowed_states` nor `failed_states`
  (e.g. it's still `running`), the sensor just keeps waiting, bound only by its
  own `timeout`.
- Omitting `external_task_id` checks the external DAG **run's** own state
  column, not a derived status of its tasks — there can be a short lag between
  "all its tasks look done" and the run itself flipping to `success`.

**Example:**
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

### 50. Sensor tuning

**What it does:** Three knobs common to every sensor.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `poke_interval` | integer (seconds) | `60` | A value of `0` or negative is treated as unset and falls back to `60`. |
| `timeout` | integer (seconds) | `3600` | Cumulative across the **entire** wait, including every reschedule cycle. |
| `soft_fail` | boolean | `False` | Timeout (and, for `ExternalTaskSensor`, the external target failing) becomes `skipped` instead of `failed`. |

**How it works:**
- `timeout` is cumulative from the sensor's very first check, across every
  `reschedule → scheduled → queued → running` cycle — it is **not** reset each
  time the sensor is re-dispatched. Design your `timeout` around the total
  acceptable wait, not a per-check budget.
- `soft_fail` only changes the *timeout* outcome (and, for `ExternalTaskSensor`,
  the external-target-failed outcome) — a genuine poke error (bad SQL, an
  uncompilable regex, a missing required param) still fails the task outright,
  regardless of `soft_fail`.
- There's no built-in minimum or backoff on `poke_interval` — a very small
  interval on a `mode="poke"` sensor polls its target at a fixed, un-backed-off
  cadence for the whole `timeout` window.

**Example:**
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

### 51. Poke vs. reschedule

**What it does:** `mode="poke"` holds the worker slot for the sensor's entire
wait. `mode="reschedule"` checks once, then releases the slot entirely until the
next check is due.

**Parameters:**

| Sensor | Default `mode` |
|---|---|
| `HttpSensor` | `"poke"` |
| `SqlSensor` | `"reschedule"` |
| `TimeSensor` | `"reschedule"` |
| `ExternalTaskSensor` | `"reschedule"` |

**How it works:**
- The four built-in sensors do **not** share a common default — `HttpSensor`
  defaults to `"poke"`, while the other three default to `"reschedule"`. Set
  `mode=` explicitly if you want consistent behavior across sensor types in your
  DAG.
- `mode="poke"` is cheap on the database (one claim, one running row, no
  re-dispatch churn) but ties up a live worker slot for the full wait — fine for
  short waits, wasteful for long ones.
- `mode="reschedule"` re-claims the task on every poke, which increments its
  `try_number` each time — this is normal and expected; it doesn't consume any of
  the task's `retries` budget, so don't read a large `try_number` on a
  reschedule-mode sensor as "it failed and retried N times."
- Both modes share the exact same cumulative `timeout` semantics (feature 50) —
  the mode only changes how worker resources are used while waiting, not how
  long the sensor is willing to wait overall.

**Example:**
```python
# poke: holds the worker slot the whole time (HttpSensor's default — fine for short waits)
quick_check = HttpSensor(task_id="quick_check", endpoint="https://api.example.com/health",
                          poke_interval=10, timeout=120)   # mode="poke" implicitly

# reschedule: releases the slot between checks (better for long waits)
long_wait = HttpSensor(task_id="long_wait", endpoint="https://api.example.com/status",
                        poke_interval=300, timeout=3600 * 6, mode="reschedule")
```

---

### 52. Deferrable/async waits

**What it does:** A third waiting strategy, alongside poke and reschedule: a
deferred task consumes **zero** worker-pool resources while waiting — see
feature 28 for the full `self.defer()` walkthrough.

**How it works:**
- None of the four built-in sensors (features 46–49) use `defer()` internally —
  there's no `deferrable=True` kwarg to get the zero-footprint behavior "for
  free" on `HttpSensor`/`SqlSensor`/`TimeSensor`/`ExternalTaskSensor`. To get
  fully deferred, zero-worker-footprint waiting for an HTTP- or time-based
  condition, write your own `BaseOperator` subclass and call `self.defer(...)`
  yourself (feature 28).
- `soft_fail` has no equivalent for a deferred wait — a trigger that times out
  (per the `timeout=` passed to `defer()`) fails the task directly; there's no
  "timeout as skip" option the way sensors get via `soft_fail`.
- `execute_complete(self, event=None)` is where you handle the fired trigger's
  result — if your custom operator doesn't override it, the task simply succeeds
  the moment the trigger fires, without inspecting what the trigger returned.
- Because the task is fully out of the worker pool while deferred, it doesn't
  compete for `MaxLocalTasks` or any pool slot at all until the trigger fires and
  it re-enters as a normal scheduled task.

**Example:**
```python
from dag_parser.dynamic.dag_context import BaseOperator, HttpTrigger

class DeferredHealthCheck(BaseOperator):
    """Waits for an endpoint to go healthy WITHOUT holding a worker slot
    (contrast with HttpSensor, feature 46, which always holds/reschedules
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

## Part 9 — Reliability, in One Place

This section is a short recap tying together the reliability-related features
already covered individually, so you can see how they fit together as a system.

### 53. How SLA, timeout, and retry mechanisms fit together

**How it works:**
- **Two independent SLA clocks**: DAG-level SLA (feature 7) measures elapsed
  time from the run's actual start; task-level SLA (feature 17) measures elapsed
  time from the run's *logical date* instead. A run that queues for a while
  before actually starting has already consumed part of its tasks' SLA budgets
  by the time it begins, even though the DAG-level clock hasn't started yet.
- **Three independent timeout layers**: task-level `execution_timeout` (feature
  16), DAG-level `dagrun_timeout_seconds` (feature 6), and a global
  abandoned-run safety net that only fires when a run has genuinely gone quiet
  (no task actively heartbeating) — a healthy, slow DAG is never killed by that
  global fallback alone.
- **Retry backoff** (feature 13) applies consistently to every retryable
  failure, whatever caused it — including a task recovered after its worker
  process itself was restarted, not just an ordinary in-process failure. You can
  rely on your configured fixed or exponential backoff curve regardless of the
  underlying cause.
- When a run is force-failed by a timeout, its still-`running`/`scheduled`/
  `queued`/`up_for_retry` tasks are swept into `failed` along with it. Tasks that
  are `deferred` (feature 28) or `up_for_reschedule` (feature 51) at that moment
  are not automatically swept — if your run's timeout might fire while a
  deferred/reschedule-mode task is still in flight, plan to check on that task
  manually afterward.
- All hard failures (retries exhausted, timeout, or an infrastructure-level
  recovery) converge on the same dead-letter queue, which is a reasonable single
  place to review any of these failure modes.

**Example:**
```python
with DAG(
    dag_id="nightly_etl",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    dagrun_timeout_seconds=3600 * 4,   # explicit strict run-level timeout
    expected_duration_seconds=3600 * 2,  # DAG-level SLA: flagged (not failed) past 2h
) as dag:

    process = PythonOperator(
        task_id="process",
        python_callable=lambda: None,
        execution_timeout=1800,   # task-level cap, independent of the run-level one
        sla=900,                  # task-level SLA, clocked from the run's logical date
        retries=3,
        retry_exponential_backoff=True,
        retry_delay_seconds=30,
        max_retry_delay_seconds=600,
    )
```

---

## Part 10 — Operators & Integrations Catalog

### 54. Compute & scripts

**What it does:** The core scripting layer — `BashOperator`, `PythonOperator`,
`ExternalPythonOperator`, `PythonVirtualenvOperator`, and `BranchPythonOperator`.

**How it works:**
- All five run as CPU-bound, local work, subject to the deployment's local-task
  concurrency cap — heavier fan-out of these operators competes for the same
  local-execution budget.
- `BashOperator` runs your command with `bash -c "<your command>"` from the
  orchestrator process's own working directory — **not** your DAGs repo
  checkout. Use an explicit `cd $DAGS_REPO_PATH/...` (or absolute paths) if your
  command needs to reference files checked out from Git.
- `ExternalPythonOperator`/`PythonVirtualenvOperator` re-import your DAG module
  fresh inside the target interpreter — that interpreter/venv must be able to
  `import dag_parser` and any packages your callable needs.
- `BranchPythonOperator` is the only one of the five that changes the scheduling
  graph (feature 33) — the other four simply run and report success/failure.
- None of the five inherit the orchestrator's own environment (feature 22),
  regardless of which one you pick.

**Example:**
```python
# BashOperator: cwd is the orchestrator's own process dir, NOT the dags repo
fix_path_example = BashOperator(
    task_id="fix_path_example",
    bash_command="cd $DAGS_REPO_PATH/dags && ls -la",  # explicit cd required
)
```

---

### 55. SQL databases

**What it does:** Running a query against a data warehouse or database
connection. `SnowflakeOperator`, `SQLExecuteQueryOperator`, `MySqlOperator`,
`PostgresOperator`, and `RedshiftOperator` all share the same shape — `sql=`
plus `connection_id=` — pick whichever matches your connection's type.

**Parameters:**

| Param | Type | Notes |
|---|---|---|
| `connection_id` | string | Must reference an existing connection of the matching type. |
| `sql` | string | The query to run. |
| `parameters` | dict | Bound into the query as real parameterized-query placeholders. |

**How it works:**
- Use `SnowflakeOperator` for Snowflake, and `SQLExecuteQueryOperator`
  (database-agnostic) or the type-specific `MySqlOperator`/`PostgresOperator`/
  `RedshiftOperator` for the matching database.
- `parameters` values are bound as real query parameters, not string-substituted
  into the SQL text — passing user-supplied values (e.g. from a manually
  triggered run's `conf`) is safe without any manual escaping on your part.
- Only the first result row is captured as `return_value` (feature 37) — a
  query returning many rows drops everything after row 1.
- `SnowflakeOperator` maintains a connection pool per `connection_id` that
  persists across task executions on the same worker — if you change a
  connection's credentials, an already-warm pool won't pick up the change until
  the worker restarts.

**Example:**
```python
from dag_parser.dynamic.dag_context import PostgresOperator

extract = SnowflakeOperator(
    task_id="extract",
    connection_id="snowflake_prod",
    sql="SELECT * FROM sales.orders WHERE region = %(region)s",
    parameters={"region": "US-WEST"},   # safely bound, not string-substituted
)

load_summary = PostgresOperator(
    task_id="load_summary",
    connection_id="reporting_pg",
    sql="INSERT INTO summary (region, total) VALUES (%(region)s, %(total)s)",
    parameters={"region": "US-WEST", "total": 12000},
)
```

---

### 56. Data movement

**What it does:** `S3ToRedshiftOperator` loads data from S3 into Redshift via
`COPY`. `GCSToBigQueryOperator` loads data from GCS into BigQuery via a load job.

**Parameters (`S3ToRedshiftOperator`):**

| Param | Type | Notes |
|---|---|---|
| `s3_conn_id` / `iam_role` | string | Provide exactly one — whichever is set determines the auth method used for the `COPY`. |
| `region` | string | Falls back to the connection's stored region if omitted, in either auth mode. |
| `truncate` | boolean | Runs in the same transaction as the following `COPY`. |
| `s3_key` | string | Wildcards supported. |

**Parameters (`GCSToBigQueryOperator`):**

| Param | Type | Notes |
|---|---|---|
| `source_format` | string | e.g. `PARQUET`, `CSV`. |
| `write_disposition` | string | e.g. `WRITE_TRUNCATE`. |
| `autodetect` | boolean | Schema auto-detection. Set exactly one of `autodetect`/`schema_fields`. |
| `schema_fields` | list | Explicit schema. Set exactly one of `autodetect`/`schema_fields`. |
| `poll_interval` | integer (seconds) | Polling cadence; the load job is bounded by the task's `execution_timeout`. |

**How it works:**
- `truncate=True` and the subsequent `COPY` run inside a single transaction —
  if `COPY` fails for any reason, the truncate is rolled back and the table
  keeps its original data.
- Set exactly one of `iam_role`/`s3_conn_id` — the operator validates this at
  DAG-parse time.
- `GCSToBigQueryOperator`'s poll loop tolerates a transient status-check error
  (retrying rather than failing the task immediately) and is bounded by the
  task's `execution_timeout` (feature 16) — set that explicitly if you need a
  hard cap on how long a load job is allowed to run.
- `autodetect` and `schema_fields` are mutually exclusive — set exactly one;
  the operator validates this at DAG-parse time.
- `destination_project`/`destination_dataset` fall back to the connection's
  stored defaults if omitted from the task — the same connection can target
  different datasets depending on whether a given task fills these in.

**Example:**
```python
load_from_s3 = S3ToRedshiftOperator(
    task_id="load_from_s3",
    s3_conn_id="s3_default",
    redshift_conn_id="redshift_prod",
    s3_bucket="my-data-bucket",
    s3_key="exports/2026/01/*.csv",
    table="orders",
    schema="public",
    copy_options="CSV IGNOREHEADER 1 GZIP",
    truncate=True,   # truncate + COPY run as a single atomic transaction
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
    execution_timeout=1800,   # ceiling for the load job
)
```

---

### 57. HTTP (one-shot call)

**What it does:** Makes a single HTTP request as a task step — distinct from
`HttpSensor` (feature 46), which polls repeatedly waiting for a condition.

**Parameters (`HttpOperator`):**

| Param | Type | Notes |
|---|---|---|
| `url` | string | Renders through Go templating (feature 38). |
| `method` | string | e.g. `GET`, `POST`. |
| `headers` | dict | |
| `body` | string | |
| `connection_id` | string | Optional — resolves stored auth for the target endpoint. |

**How it works:**
- `HttpOperator` (also importable as `SimpleHttpOperator`) is a ready-made
  class — no need to subclass `BaseOperator` yourself.
- `url`/`headers`/`body` render through Go templating (feature 38), not Jinja.
- A response is truncated at 1MB, and any 2xx counts as success — there's no
  response-body validation option here (that's `HttpSensor`-only); add a
  follow-up Python task reading the pushed `return_value` if you need to inspect
  the body.

**Example:**
```python
from dag_parser.dynamic.operators import HttpOperator

notify_downstream = HttpOperator(
    task_id="notify_downstream",
    url="https://api.example.com/v1/events",
    method="POST",
    headers={"Content-Type": "application/json"},
    connection_id="internal_api",
    body='{"event": "pipeline_complete", "dag_id": "{{ .DagID }}"}',
)
```

---

### 58. SSH

**What it does:** Opens an SSH connection and runs a single remote command.

**Parameters:**

| Param | Type | Notes |
|---|---|---|
| `connection_id` | string | Provides host/auth (password or private key — key is tried first if both are present) and the expected host key/fingerprint. |
| `command` | string | The remote command to run. |
| `environment` | dict | Best-effort — see below. |

**How it works:**
- SSH connections verify the remote host's key against the fingerprint stored
  on the connection — set this when creating the connection so PI-Flow can
  detect an unexpected host on the other end.
- `environment={}` may be silently ignored by the remote server depending on its
  `sshd_config` (most servers reject arbitrary `SetEnv` requests unless
  explicitly allowlisted) — a rejected env var is only logged as a warning, never
  surfaced as a task error. Prefer passing values through the command string
  itself if you need to guarantee they arrive.
- Output is `return_value`'d as stdout and stderr combined into one string
  (feature 37) — there's no way to capture them separately from this operator.
- The connection dial itself has a fixed 30-second timeout, separate from the
  command's own execution time — use `execution_timeout` (feature 16) to bound
  the whole task if the command itself might run long.

**Example:**
```python
from dag_parser.dynamic.operators import SSHOperator

remote_cleanup = SSHOperator(
    task_id="remote_cleanup",
    connection_id="prod_bastion",   # host key verified against the connection's stored fingerprint
    command="rm -rf /tmp/staging/{{ .DS }}",
    environment={"STAGE": "prod"},  # may be silently dropped by sshd's AcceptEnv config
)
```

---

### 59. Email / Slack (as task steps)

**What it does:** `EmailOperator` and `SlackAPIPostOperator` send a message as a
normal task step in the graph — distinct from the callback-based alert channels
in Part 7, which fire on an *event* rather than running as their own scheduled
step.

**Parameters (`EmailOperator`):**

| Param | Type | Notes |
|---|---|---|
| `to` | list of strings | *required* |
| `subject` | string | Supports the same `{{dag_id}}`/`{{task_id}}`/`{{run_id}}` tokens as `html_content`. |
| `html_content` | string | Supports the same tokens. |
| `cc` / `bcc` | list of strings | Optional. |
| `from_email` | string | Overrides the deployment's default SMTP "from" address for this task. |
| `files` | list of file paths | Attached to the outgoing email. |
| `custom_headers` | dict | Added to the outgoing MIME message. |

**Parameters (`SlackAPIPostOperator`):**

| Param | Type | Notes |
|---|---|---|
| `connection_id` | string | Token-mode or webhook-mode. |
| `channel` | string | **Required** with a token-mode connection; optional with webhook-mode (most webhooks are already bound to a fixed channel). |
| `text` | string | Message body. |
| `unfurl_links` | boolean | Only has an effect with a token-mode connection. |

**How it works:**
- Set `from_email` to override the "from" address for that specific task;
  leave it unset to use the deployment's globally-configured SMTP address.
- `subject` and `html_content` are both token-substituted the same way.
- `files` attaches the listed local file paths to the outgoing email;
  `custom_headers` are added as extra header lines on the outgoing message.
- `SlackAPIPostOperator`'s `return_value` shape depends on the connection's
  mode: webhook mode returns Slack's raw response body (typically `"ok"`);
  token mode returns `{"ts": "<message timestamp>"}`. A downstream task reading
  this XCom should know which mode the connection uses.

**Example:**
```python
from dag_parser.dynamic.dag_context import EmailOperator
from dag_parser.dynamic.dag_context import SlackAPIPostOperator

send_report = EmailOperator(
    task_id="send_report",
    to=["team@company.com"],
    subject="Report for {{dag_id}}",       # substituted
    html_content="<p>Report for {{ dag_id }} run {{ run_id }}</p>",
    from_email="pipeline-alerts@company.com",
    files=["/tmp/report.csv"],
    custom_headers={"X-Priority": "1"},
)

post_to_slack = SlackAPIPostOperator(
    task_id="post_to_slack",
    connection_id="slack_bot_token",   # token-mode: channel is REQUIRED
    channel="#data-alerts",
    text="Report generation complete",
    unfurl_links=True,
)
```

---

### 60. Databricks

**What it does:** `DatabricksSubmitRunOperator` submits a one-off run via the
Databricks Jobs API, on either an existing or an ephemeral cluster, and waits for
it to finish.

**Parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `task_type` | string | `"notebook_task"` | One of `notebook_task`, `spark_python_task`, `spark_jar_task`. |
| `cluster_id` | string | connection's stored default, if any | Falls back to the connection's own `extra.cluster_id` if omitted. |
| `new_cluster` | dict | none | Set exactly one of `cluster_id`/`new_cluster` — the operator validates this at DAG-parse time. |
| `poll_interval` | integer (seconds) | — | Polling cadence; the poll loop tolerates a transient status-check error and is bounded by `execution_timeout`. |
| `idempotency_token` | string | none | Passed unchanged on every retry attempt. |

**How it works:**
- Set exactly one of `cluster_id` or `new_cluster` per task — setting both (or
  neither, with no connection default available) raises a clear validation
  error at DAG-parse time.
- `cluster_id` falls back to the connection's own stored default if you omit it
  from the task — handy if most tasks target the same cluster.
- The poll loop tolerates a single transient HTTP error (retrying rather than
  failing the task immediately) and is bounded by the task's `execution_timeout`
  (feature 16) — set that explicitly to cap how long you're willing to wait for
  a Databricks job.
- A `notebook_task` that calls `dbutils.notebook.exit(...)` is the most reliable
  way to get a meaningful `return_value` back — `spark_python_task`/
  `spark_jar_task` runs typically don't populate one the same way.
- If you set a static `idempotency_token` on a task that also has `retries`
  configured, a retry using the same token may be deduplicated by Databricks
  itself — only do this if you specifically want that dedup behavior.

**Example:**
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

### 61. Cross-DAG orchestration, summarized

**What it does:** A recap of the operators that compose multiple DAGs together
or wait on state outside the current DAG's own task graph:
`TriggerDagRunOperator` (feature 27) plus the four sensors (Part 8).

**How it works:**
- `TriggerDagRunOperator`'s `wait_for_completion=True` uses the same lightweight,
  slot-free waiting mechanism described in feature 27 — it does not hold a
  worker slot, unlike a `mode="poke"` sensor. Its `poke_interval` controls how
  often the background reconciler checks the child run's state while waiting.
- `TriggerDagRunOperator` is treated as lightweight coordination work for
  scheduling purposes, the same as the sensors — it is not subject to the same
  admission limits as CPU-heavy Python/Bash tasks, so it stays responsive even
  under system load.
- The sensors (Part 8) each wait on a *condition* (an HTTP check, a SQL query, a
  clock, or another task/DAG's state) using the poke/reschedule model
  (feature 51).

**Example:**
```python
from dag_parser.dynamic.dag_context import TriggerDagRunOperator
from dag_parser.dynamic.operators import ExternalTaskSensor

with DAG(dag_id="orchestrator", schedule="@daily", start_date=datetime(2026, 1, 1)) as dag:

    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="child_pipeline",
        wait_for_completion=True,
        poke_interval=30,   # how often the reconciler checks the child run's state
    )

    wait_for_sibling = ExternalTaskSensor(
        task_id="wait_for_sibling",
        external_dag_id="sibling_pipeline",
        allowed_states=["success"],
        mode="reschedule",
    )
```
