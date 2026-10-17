## Section 11: Apache Airflow

### Q519. How does a Python file become a DAG in Airflow 3? Explain parsing and the DAG processor
**ID:** AF-01
**Topic:** Airflow > DAG Parsing
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `airflow`, `dag-processor`, `parsing`, `serialization`

**Short Answer (2–3 lines):**
The DAG processor finds Python files in each DAG bundle and runs them. Every DAG object a file creates is serialised to JSON and saved in the metadata database. The scheduler and the API server then read only that serialised copy and never import your file. Workers load the file again only when they run a task.

**Detailed Explanation:**
Parsing is a loop, and it never stops:

1. **Discover.** Every `[dag_processor] refresh_interval` (300 s by default) the processor lists the files in each configured bundle. It skips paths matched by `.airflowignore`, which uses glob syntax by default in 3.x (regexp was the 2.x default). In safe mode it only parses files whose text contains both "airflow" and "dag".
2. **Execute.** Child processes (`parsing_processes`, default 2) import each file. A file is re-parsed at most every `min_file_process_interval` (30 s). Each import is cut off after `dagbag_import_timeout` (30 s).
3. **Serialise.** Each DAG found is written to the `serialized_dag` table. If the structure changed, a new DAG version is recorded too (see Q558). Errors are saved as import errors and shown in the UI.
4. **Schedule.** The scheduler builds runs and task instances from the serialised DAG. Workers re-import the file at run time to get the real Python callables.

| Component | Imports your file? |
|---|---|
| DAG processor | Yes, every few seconds |
| Scheduler | No (reads serialised JSON) |
| API server / UI | No |
| Worker | Yes, once per task try |

In Airflow 3 the DAG processor is its own required component, started with `airflow dag-processor` (see Q41). Because your file runs on every parse, anything at module level, such as API calls or `Variable.get`, runs again and again (see Q568).

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
# Layout: dags/orders_daily.py (valid) and dags/broken_dag.py (two tasks named "a")
import time
from airflow.dag_processing.dagbag import DagBag

start = time.perf_counter()
bag = DagBag(dag_folder="dags")   # 3.x DagBag no longer takes include_examples
print(f"parsed in {time.perf_counter() - start:.2f}s")
print("dags:", sorted(bag.dag_ids))
for path, err in bag.import_errors.items():
    print("import error:", path.split("/")[-1], "->", err.strip().splitlines()[-1])
```
```text
parsed in 0.02s
dags: ['orders_daily']
import error: broken_dag.py -> airflow.sdk.exceptions.DuplicateTaskIdFound: Task id 'a' has already been added to the Dag
```

**Real-World Use Case:**
A team merged a DAG that called a REST API at module level to build its task list. The DAG processor ran that call about every 30 seconds, all day, and the vendor started rate-limiting the account. Parse times also climbed past the timeout, and the DAG kept vanishing from the UI. They moved the call inside a task and used dynamic task mapping, and parse time fell below one second.

**Common Mistakes / Interviewer Red Flags:**
- Thinking the scheduler imports DAG files in Airflow 3. It reads serialised DAGs; the DAG processor does the parsing.
- Thinking a file is parsed once at deploy time. Parsing is continuous.
- Hiding a broken DAG with `.airflowignore` instead of fixing the import error.

**Follow-up Questions Interviewers Ask:**
- Where do you see parse errors? — The UI's import-errors banner, `airflow dags list-import-errors`, and the DAG processor logs.
- How do you see slow files? — Run `airflow dags report` or watch the `dag_processing.last_duration` metric, which is tagged per file.
- Can one file define many DAGs? — Yes. Every DAG object the file creates is registered (see Q531).

---

### Q520. TaskFlow API vs classic operators: what does `@task` actually do?
**ID:** AF-02
**Topic:** Airflow > TaskFlow API
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `airflow`, `taskflow`, `airflow-sdk`, `xcom`

**Short Answer (2–3 lines):**
`@task` wraps a Python function in an operator. Calling the function inside a DAG creates a task and returns an `XComArg` placeholder, not a value. When you pass that placeholder to another task, Airflow adds the dependency and pulls the XCom at run time. Classic operators are still the best tool for ready-made integrations, and the two styles mix freely.

**Detailed Explanation:**
In Airflow 3 the authoring imports live in `airflow.sdk`: `dag`, `task`, `DAG`, `TaskGroup`, `Asset`, `Variable`, `BaseOperator` and others. Old paths such as `airflow.decorators` are deprecated.

What `@task` gives you:
- **Implicit dependencies.** `load(transform(extract()))` builds `extract >> transform >> load`.
- **Implicit XCom.** The return value is pushed as `return_value`. Arguments that are `XComArg`s are resolved before the function runs.
- **Typed, testable functions.** The business logic is plain Python, and `.function` gives you the raw callable for unit tests (see Q561).
- **Variants.** `@task.bash`, `@task.branch`, `@task.short_circuit`, `@task.sensor`, `@task.virtualenv`, `@task.docker`, `@task.kubernetes`. Each one wraps the matching operator.

How the two styles talk to each other:
| From | To | How |
|---|---|---|
| Classic operator | TaskFlow | Pass `op.output` (an `XComArg`) |
| TaskFlow | Classic | Pass the `XComArg` into a templated field, or use `ti.xcom_pull` in Jinja |
| Either | Either | `>>` / `<<` work on operators and on `XComArg`s |

Calling a decorated function does not run it. Inside the DAG body, `double(3)` only adds a task to the DAG. The body runs at parse time; task code runs on a worker.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3 (apache-airflow-providers-standard)
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def _count_rows(**context):
    # classic style: return value is pushed to XCom as "return_value"
    return 3


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def mixed_styles():
    count = PythonOperator(task_id="count_rows", python_callable=_count_rows)

    @task
    def double(n: int) -> int:          # TaskFlow: args are pulled from XCom for you
        return n * 2

    notify = BashOperator(
        task_id="notify",
        bash_command="echo doubled={{ ti.xcom_pull(task_ids='double') }}",
    )

    doubled = double(count.output)       # .output is an XComArg for the classic task
    doubled >> notify                     # XComArg works with >> as well


dag_obj = mixed_styles()

if __name__ == "__main__":
    print(dag_obj.test().state)
```
```text
... Done. Returned value was: 3
... Done. Returned value was: 6
... Running command: ['/usr/bin/bash', '-c', 'echo doubled=6']
success
```

**Real-World Use Case:**
A team kept `SQLExecuteQueryOperator` from the provider for its warehouse statements. It wrote the custom business rules, such as parsing vendor files and computing reconciliation totals, as `@task` functions. The rule code got pytest coverage, while the integration code was the tested provider version.

**Common Mistakes / Interviewer Red Flags:**
- Expecting `result = extract()` to hold real data inside the DAG body.
- Returning a large DataFrame from `@task`. Return values become XComs (see Q43, Q546).
- Wrapping `PythonOperator` around a provider hook when a provider operator already exists.

**Follow-up Questions Interviewers Ask:**
- How do you get the context in a TaskFlow task? — Add `**context` or named arguments such as `ti` and `data_interval_start`, or call `get_current_context()`.
- How do you return several XComs? — Use `@task(multiple_outputs=True)` with a dict, or a `TypedDict` return annotation (see Q546).

---

### Q521. Operators, hooks and providers: how do they fit together?
**ID:** AF-03
**Topic:** Airflow > Operators and Hooks
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `airflow`, `operators`, `hooks`, `providers`

**Short Answer (2–3 lines):**
A hook knows *how* to talk to an external system. It reads an Airflow connection and returns an authenticated client. An operator defines *what* one task does, and usually calls a hook inside `execute()`. Providers are separately versioned pip packages that ship hooks, operators, sensors, triggers and connection types for a system such as AWS or Snowflake.

**Detailed Explanation:**
| Layer | Responsibility | Example |
|---|---|---|
| Connection | Credentials and endpoint, stored outside code | `snowflake_default` |
| Hook | Turns a connection into a client; reusable | `S3Hook`, `PostgresHook` |
| Operator | A unit of work with parameters, templated fields and retries | `S3CopyObjectOperator` |
| Sensor / Trigger | Waits for a condition (see Q544, Q543) | `S3KeySensor` |
| Provider | Package that bundles all of the above | `apache-airflow-providers-amazon` |

Key points:
- Even "core" operators sit in providers in Airflow 3. `BashOperator`, `PythonOperator` and `EmptyOperator` live in `apache-airflow-providers-standard`, so import them from `airflow.providers.standard...`.
- Providers are released on their own schedule. Pin them in a constraints file, so an upgrade happens on purpose and a rebuild does not change them silently.
- A custom operator subclasses `BaseOperator` and implements `execute(context)`. Use `template_fields` to choose which attributes Jinja renders. The constructor runs at parse time, so keep it cheap and do I/O only in `execute`.
- Write a hook when several operators or tasks need the same client logic. Write an operator when a team repeats the same task shape. A plain `@task` that calls the hook is often enough.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
import json
import os
from datetime import datetime, timezone
from airflow.sdk import BaseHook, BaseOperator, dag

# A connection defined as an environment variable (JSON form); in production it
# would live in a secrets backend. The conn_id is the part after AIRFLOW_CONN_.
os.environ["AIRFLOW_CONN_INVENTORY_API"] = json.dumps(
    {"conn_type": "http", "host": "inventory.internal", "login": "svc", "password": "s3cret",
     "extra": {"page_size": 500}}
)


class InventoryHook(BaseHook):
    """Hook: knows HOW to talk to the system. Reusable by many operators."""

    def __init__(self, conn_id: str = "inventory_api"):
        super().__init__()
        self.conn_id = conn_id

    def get_client_config(self) -> dict:
        conn = self.get_connection(self.conn_id)
        return {"base_url": f"https://{conn.host}", "user": conn.login,
                "page_size": conn.extra_dejson.get("page_size", 100)}


class InventorySnapshotOperator(BaseOperator):
    """Operator: WHAT one task does, using the hook."""

    template_fields = ("snapshot_date",)

    def __init__(self, snapshot_date: str, conn_id: str = "inventory_api", **kwargs):
        super().__init__(**kwargs)
        self.snapshot_date = snapshot_date
        self.conn_id = conn_id

    def execute(self, context):
        cfg = InventoryHook(self.conn_id).get_client_config()
        print(f"GET {cfg['base_url']}/snapshot?date={self.snapshot_date} "
              f"as {cfg['user']} page_size={cfg['page_size']}")
        return {"date": self.snapshot_date, "rows": 1200}


@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def inventory_snapshot():
    InventorySnapshotOperator(task_id="snapshot", snapshot_date="{{ ds }}")


dag_obj = inventory_snapshot()

if __name__ == "__main__":
    print(dag_obj.test(logical_date=datetime(2026, 3, 1, tzinfo=timezone.utc)).state)
```
```text
GET https://inventory.internal/snapshot?date=2026-03-01 as svc page_size=500
success
```

**Real-World Use Case:**
A platform team wrapped its internal data-catalogue API in a hook. It added two operators on top: register a table, and wait for an approval (built as a deferrable operator). It shipped them as an internal provider package with its own version. Dozens of teams used the same code, and a credential rotation only meant changing one connection.

**Common Mistakes / Interviewer Red Flags:**
- Opening network connections in an operator's `__init__`, which runs at every parse.
- Hard-coding credentials in the operator instead of using a `conn_id`.
- Saying "Airflow processes the data". Hooks and operators usually tell another system to do the work.

**Follow-up Questions Interviewers Ask:**
- What is `template_fields`? — The list of attributes Jinja renders before `execute()` (see Q532).
- How do you find a provider's version and hooks? — Run `airflow providers list`, `airflow providers hooks`, or check the provider's docs and changelog.

---

### Q522. Connections and Variables: how should DAGs get credentials and configuration?
**ID:** AF-04
**Topic:** Airflow > Connections and Variables
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `airflow`, `connections`, `variables`, `configuration`

**Short Answer (2–3 lines):**
A Connection stores how to reach a system: type, host, login, password, port and JSON extras. Code looks it up by `conn_id`. A Variable is a key/value setting. Both can come from a secrets backend, environment variables (`AIRFLOW_CONN_*`, `AIRFLOW_VAR_*`) or the metadata DB. Read them inside tasks or templates, never in top-level DAG code.

**Detailed Explanation:**
Lookup order: the secrets backend first, if one is configured (see Q559), then environment variables, then the metadata database. Each environment gets its own values under the same IDs, so one DAG file works in dev, staging and prod.

Ways to read them:
| Where | Syntax | When it runs |
|---|---|---|
| Task code | `Variable.get("k", deserialize_json=True, default=...)` | Task run time |
| Hook | `self.get_connection(conn_id)` | Task run time |
| Jinja | `{{ var.value.k }}`, `{{ var.json.k.field }}`, `{{ conn.my_id.host }}` | Rendered just before `execute` |
| Module top level | `Variable.get(...)` outside a task | **Every parse**: avoid |

In Airflow 3, task code can no longer query the metadata DB. `Variable.get` and `get_connection` in a task go through the Task Execution API (see Q557), or through a secrets backend configured for workers.

Good practice:
- Keep secrets in a secrets backend and plain settings in Variables or code. Don't store one huge JSON Variable that every DAG reads.
- Values of connection passwords and of Variables whose names contain words like `password`, `secret` or `token` are masked in task logs. Treat masking as a safety net, not a design.
- Use one `conn_id` naming convention across environments, for example `warehouse` everywhere, with different values.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
# In real deployments these come from a secrets backend or the UI; env vars keep the demo self-contained.
import os
from datetime import datetime
from airflow.sdk import Variable, dag, task
from airflow.providers.standard.operators.bash import BashOperator

os.environ["AIRFLOW_VAR_EXPORT_CONFIG"] = '{"region": "eu-west-1", "batch_size": 5000}'
os.environ["AIRFLOW_CONN_WAREHOUSE"] = "postgres://etl_user:pa55word@wh.internal:5432/analytics"


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def config_demo():
    @task
    def read_config():
        # Read inside the task (at run time), never at module top level.
        cfg = Variable.get("export_config", deserialize_json=True)
        missing = Variable.get("not_set_yet", default="fallback")
        print("batch_size:", cfg["batch_size"], "| missing:", missing)

    show = BashOperator(
        task_id="templated",
        bash_command=(
            "echo region={{ var.json.export_config.region }} "
            "host={{ conn.warehouse.host }} schema={{ conn.warehouse.schema }}"
        ),
    )
    read_config() >> show


dag_obj = config_demo()

if __name__ == "__main__":
    print(dag_obj.test().state)
```
```text
batch_size: 5000 | missing: fallback
... region=eu-west-1 host=wh.internal schema=analytics
success
```

**Real-World Use Case:**
An analytics platform ran dev, staging and prod from the same Git branch. Each environment's secrets backend held a `warehouse` connection pointing at its own database. A DAG promoted to prod needed no code changes, and nobody had to copy credentials into the UI.

**Common Mistakes / Interviewer Red Flags:**
- Calling `Variable.get` at module level. That is a lookup on every parse, for every file.
- Putting passwords in Variables or DAG code instead of connections or a secrets backend.
- Editing connections in the UI when they actually come from a secrets backend. The UI shows only DB-stored values.

**Follow-up Questions Interviewers Ask:**
- How do you write a connection as an env var? — As a URI (`postgres://user:pw@host:5432/db`) or as JSON in `AIRFLOW_CONN_<ID>`.
- Can a task change a Variable? — Yes, `Variable.set`, but shared mutable state between runs is a smell. Prefer XCom, assets or the 3.3 task state store.

---

### Q523. How do you declare task dependencies, and what do `chain` and `cross_downstream` do?
**ID:** AF-05
**Topic:** Airflow > Task Dependencies
**Level:** Beginner
**Frequency:** Common
**Tags:** `airflow`, `dependencies`, `chain`, `dag-structure`

**Short Answer (2–3 lines):**
Use `a >> b` (or `b << a`), or pass TaskFlow outputs into other tasks so the edges are implied. `chain()` links a sequence; when it gets two lists of the same length, it pairs them element by element. `cross_downstream()` connects every task in one list to every task in another. `Label` only names an edge in the UI.

**Detailed Explanation:**
| Pattern | Code | Edges |
|---|---|---|
| Linear | `a >> b >> c` | a→b, b→c |
| Fan-out / fan-in | `a >> [b, c] >> d` | a→b, a→c, b→d, c→d |
| Pairwise | `chain(a, [b1, b2], [c1, c2])` | a→b1, a→b2, b1→c1, b2→c2 |
| All-to-all | `cross_downstream([b1, b2], [c1, c2])` | 4 edges |
| Linear between lists | `chain_linear([a, b], [c, d])` | Each task in a list links to every task in the next list |

Rules interviewers check:
- `[b, c] >> [d, e]` is a `TypeError`, because Python cannot shift a list by a list. Use `cross_downstream` or `chain_linear`.
- Dependencies only order tasks. Whether a downstream task runs depends on its **trigger rule** (see Q524).
- Dependencies cannot cross DAGs. For that, use assets, `TriggerDagRunOperator` or `ExternalTaskSensor` (see Q542).
- A cycle is caught at parse time and becomes an import error.
- `setup` and `teardown` tasks add a special kind of relationship (see Q528).

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import DAG, Label, chain, cross_downstream
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG("deps_demo", schedule=None, start_date=datetime(2026, 1, 1), catchup=False) as dag:
    start, end = EmptyOperator(task_id="start"), EmptyOperator(task_id="end")
    extract = [EmptyOperator(task_id=f"extract_{s}") for s in ("crm", "erp")]
    clean = [EmptyOperator(task_id=f"clean_{s}") for s in ("crm", "erp")]
    checks = [EmptyOperator(task_id=f"check_{c}") for c in ("rows", "nulls")]
    publish = EmptyOperator(task_id="publish")

    # chain() with equal-length lists pairs them element-wise: extract_crm >> clean_crm, ...
    chain(start, extract, clean)
    # cross_downstream: every clean task feeds every check task (2 x 2 edges)
    cross_downstream(clean, checks)
    # >> with a list fans in; Label only annotates the edge in the UI
    checks >> Label("all checks passed") >> publish >> end

if __name__ == "__main__":
    for t in sorted(dag.tasks, key=lambda t: t.task_id):
        print(f"{t.task_id:12} -> {sorted(t.downstream_task_ids)}")
```
```text
check_nulls  -> ['publish']
check_rows   -> ['publish']
clean_crm    -> ['check_nulls', 'check_rows']
clean_erp    -> ['check_nulls', 'check_rows']
end          -> []
extract_crm  -> ['clean_crm']
extract_erp  -> ['clean_erp']
publish      -> ['end']
start        -> ['extract_crm', 'extract_erp']
```

**Real-World Use Case:**
A finance pipeline pulled data from two source systems in parallel, cleaned each one, and ran a shared set of quality checks on both. `chain` kept each source's extract and clean tasks paired, and `cross_downstream` made every check wait for every cleaned source. Printing `downstream_task_ids` in a unit test then locked the structure in place.

**Common Mistakes / Interviewer Red Flags:**
- Writing `[a, b] >> [c, d]` and being surprised by the error.
- Expecting `chain(a, [b1, b2], [c1, c2, c3])` to work. Adjacent lists must be the same length.
- Adding edges "just in case". Every extra edge serialises work that could run in parallel.

**Follow-up Questions Interviewers Ask:**
- How do you test the structure? — Load the DAG and assert on `task.downstream_task_ids` or `dag.task_dict` (see Q560).
- Do the edges between TaskFlow calls also pass data? — Yes. Passing an output both adds the edge and moves the XCom value. A bare `>>` only orders the tasks.

---

### Q524. What are trigger rules, and which ones do you actually use?
**ID:** AF-06
**Topic:** Airflow > Trigger Rules
**Level:** Intermediate
**Frequency:** Very Common
**Tags:** `airflow`, `trigger-rules`, `dependencies`, `failure-handling`

**Short Answer (2–3 lines):**
A trigger rule decides when a task may run, based on the states of its direct upstream tasks. The default is `all_success`. The everyday alternatives are `all_done` for cleanup, `one_failed` for alerting, and `none_failed_min_one_success` for joining after a branch. A trigger rule only looks at direct parents, not at the whole DAG.

**Detailed Explanation:**
The rules in Airflow 3.3's `TriggerRule` enum:

| Rule | Runs when direct upstreams… | Typical use |
|---|---|---|
| `all_success` (default) | all succeeded | Normal flow |
| `all_failed` | all failed or upstream_failed | Fallback path |
| `all_done` | all finished, in any state | Cleanup, audit row |
| `all_done_min_one_success` | all finished and at least one succeeded (3.1+) | Partial-tolerance join |
| `all_done_setup_success` | all done and a setup succeeded | Used by teardowns (see Q528) |
| `one_success` / `one_failed` / `one_done` | as soon as one does so | Race, early alert |
| `none_failed` | none failed (skips are fine) | Join after optional work |
| `none_failed_min_one_success` | none failed and at least one succeeded | **Join after a branch** (see Q525) |
| `none_skipped` | none were skipped | Rare |
| `all_skipped` | all were skipped | "Nothing happened" handling |
| `always` | no condition | Use with care |

Two behaviours interviewers probe:
- **Propagation.** Under `all_success`, a failure upstream marks the child `upstream_failed`, and a skip marks it `skipped`. Each state cascades down until some task's rule accepts it.
- **DAG run state comes from the leaf tasks.** If the only leaf is an `all_done` cleanup task that succeeds, the run can be marked *success* even though real work failed. Either keep a work task as a leaf or make the cleanup a teardown, because teardowns are ignored when the run's state is worked out.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import TriggerRule, dag, task


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def trigger_rules_demo():
    @task
    def load_eu():
        return "ok"

    @task
    def load_us():
        raise ValueError("source file missing")

    @task  # default all_success: will not run because load_us failed
    def build_report():
        print("report built")

    @task(trigger_rule=TriggerRule.ALL_DONE)  # runs whatever happened upstream
    def cleanup_tmp():
        print("temp files removed")

    @task(trigger_rule=TriggerRule.ONE_FAILED)  # runs as soon as any upstream fails
    def page_on_call():
        print("paging on-call")

    loads = [load_eu(), load_us()]
    loads >> build_report()
    loads >> cleanup_tmp()
    loads >> page_on_call()


dag_obj = trigger_rules_demo()

if __name__ == "__main__":
    run = dag_obj.test()
    for ti in sorted(run.get_task_instances(), key=lambda t: t.task_id):
        print(f"{ti.task_id:14} {ti.state}")
    print("dag run:", run.state)
```
```text
temp files removed
paging on-call
build_report   upstream_failed
cleanup_tmp    success
load_eu        success
load_us        failed
page_on_call   success
dag run: failed
```

**Real-World Use Case:**
A nightly job ended in a single `all_done` task that dropped staging tables. For weeks the runs showed green while the load step was failing, because that cleanup task was the only leaf and it succeeded. The team turned the cleanup into a teardown and added a `one_failed` alert task. After that, the failures showed up the same night.

**Common Mistakes / Interviewer Red Flags:**
- Leaving the join after a branch on the default rule, so it is always skipped.
- Using `always` or `all_done` on tasks that need correct upstream data.
- Assuming a trigger rule can look at tasks that are not direct parents.

**Follow-up Questions Interviewers Ask:**
- What is the difference between `none_failed` and `none_failed_min_one_success`? — The second also requires at least one success, so it won't run when every upstream was skipped.
- How do you alert on failure without a task? — Use `on_failure_callback` or a notifier (see Q552).

---

### Q525. How does branching work in Airflow, and why is the join task often skipped?
**ID:** AF-07
**Topic:** Airflow > Branching
**Level:** Intermediate
**Frequency:** Very Common
**Tags:** `airflow`, `branching`, `trigger-rules`, `taskflow`

**Short Answer (2–3 lines):**
A branch task (`@task.branch` or `BranchPythonOperator`) returns the task_id, or list of task_ids, to follow. Airflow skips its other direct downstream tasks. The skip then spreads: a join task on the default `all_success` rule sees a skipped parent and is skipped too. Give the join `none_failed_min_one_success`.

**Detailed Explanation:**
How it works:
1. The branch callable runs and returns `"a"`, `["a", "b"]`, or `None`/`[]`. Returning nothing skips every direct downstream task.
2. Every task directly downstream of the branch that was not chosen is set to `skipped`.
3. Each later task applies its own trigger rule to its parents' states (see Q524).

Variants:
| Need | Tool |
|---|---|
| Choose a path in Python | `@task.branch` |
| Choose by SQL result | `BranchSQLOperator` (common-sql provider) |
| Choose by time of day or weekday | `BranchDateTimeOperator`, `BranchDayOfWeekOperator` |
| Reusable custom logic | Subclass `BaseBranchOperator` and implement `choose_branch` |
| Just stop or continue | `@task.short_circuit` (see Q526) |

Design rules:
- **Branch on run metadata**, such as `data_interval_start`, `params` or upstream XComs, not on `datetime.now()`. A rerun next week must take the same path.
- Return task_ids that are **direct** downstream tasks. Inside a task group, use the full id, such as `"group.task"`.
- Keep the branch cheap. A heavy check belongs in an upstream task whose result the branch reads.
- If the branches build the same output in different ways, think about one task with a parameter instead. Branches add a lot of skipped boxes to the grid.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime, timezone
from airflow.sdk import TriggerRule, dag, task


@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def branch_by_day():
    @task.branch
    def choose_load(**context) -> str:
        # Branch on run metadata, not on wall-clock time, so reruns pick the same path.
        is_month_start = context["data_interval_start"].day == 1
        return "full_reload" if is_month_start else "incremental_load"

    @task
    def full_reload():
        print("rebuilding whole table")

    @task
    def incremental_load():
        print("merging one day")

    # Without this trigger rule the join would be skipped along with the unchosen branch.
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def refresh_dashboard():
        print("dashboard refreshed")

    choose_load() >> [full_reload(), incremental_load()] >> refresh_dashboard()


dag_obj = branch_by_day()

if __name__ == "__main__":
    for day in (1, 2):
        run = dag_obj.test(logical_date=datetime(2026, 3, day, tzinfo=timezone.utc))
        states = {ti.task_id: ti.state for ti in run.get_task_instances()}
        print(f"2026-03-0{day}:", dict(sorted(states.items())))
```
```text
2026-03-01: {'choose_load': 'success', 'full_reload': 'success', 'incremental_load': 'skipped', 'refresh_dashboard': 'success'}
2026-03-02: {'choose_load': 'success', 'full_reload': 'skipped', 'incremental_load': 'success', 'refresh_dashboard': 'success'}
```

**Real-World Use Case:**
A subscription-metrics DAG fully rebuilt its churn table on the first day of each month and merged one day on the other days. Because the branch read `data_interval_start`, a backfill of last quarter took the full-reload path on exactly the right three dates.

**Common Mistakes / Interviewer Red Flags:**
- Forgetting the join's trigger rule, then "fixing" it with `all_done`, which also runs after a failure.
- Branching on `datetime.now()` or on a Variable someone edits, which makes reruns take a different path.
- Returning a task_id that is not a direct child of the branch task.

**Follow-up Questions Interviewers Ask:**
- Can a branch pick several paths? — Yes. Return a list of task_ids.
- Branch or dynamic task mapping? — Branch picks from a fixed set of paths. Mapping runs one task over a variable number of inputs (see Q529).

---

### Q526. Short-circuit, branch or `AirflowSkipException`: how do you skip work cleanly?
**ID:** AF-08
**Topic:** Airflow > Skipping Tasks
**Level:** Intermediate
**Frequency:** Common
**Tags:** `airflow`, `short-circuit`, `skip`, `trigger-rules`

**Short Answer (2–3 lines):**
`@task.short_circuit` skips downstream tasks when its function returns a falsy value; it is a stop-or-continue gate. A branch chooses between paths. Raising `AirflowSkipException` inside a task marks only that task as skipped. By default a short-circuit skips *all* downstream tasks and ignores their trigger rules. Set `ignore_downstream_trigger_rules=False` to skip only the direct children.

**Detailed Explanation:**
| Tool | Effect | Use when |
|---|---|---|
| `@task.short_circuit` (default) | Falsy result → every downstream task skipped, trigger rules ignored | "No new data today, stop here" |
| `@task.short_circuit(ignore_downstream_trigger_rules=False)` | Direct children skipped; tasks further down apply their own rules | An audit or cleanup task must still run |
| `@task.branch` | Unchosen direct children skipped | Choosing between paths (see Q525) |
| `raise AirflowSkipException(...)` | This task becomes `skipped`; children follow their rules | Feature flag, an optional export, a tenant opt-out |
| `raise AirflowFailException(...)` | Fails at once, with no retries | Bad input that retrying won't fix |

Points to make:
- A skip is not a failure. A DAG run whose leaves are all `success` or `skipped` counts as successful. Make sure "no data" really is fine; sometimes it should be an alert instead.
- A short-circuit that runs before a sensor saves the waiting time as well as the work.
- `on_skipped_callback` fires only when a task raises `AirflowSkipException` while running. It does not fire for tasks skipped by a branch or a trigger rule.
- In Airflow 3, import task exceptions from `airflow.sdk.exceptions`.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import TriggerRule, dag, task
from airflow.sdk.exceptions import AirflowSkipException


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def short_circuit_demo():
    @task
    def count_new_rows() -> int:
        return 0                      # nothing arrived today

    # False/falsy return -> skip downstream. ignore_downstream_trigger_rules=False
    # skips only direct downstream tasks and lets the others apply their own trigger rules.
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def has_new_rows(n: int) -> bool:
        return n > 0

    @task
    def transform():
        print("transforming")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def write_audit_row():
        print("audit row written")       # still runs: it has its own trigger rule

    @task
    def optional_export():
        if True:                          # e.g. feature flag off for this tenant
            raise AirflowSkipException("export disabled for this tenant")

    gate = has_new_rows(count_new_rows())
    gate >> transform() >> write_audit_row()
    optional_export()


dag_obj = short_circuit_demo()

if __name__ == "__main__":
    run = dag_obj.test()
    for ti in sorted(run.get_task_instances(), key=lambda t: t.task_id):
        print(f"{ti.task_id:16} {ti.state}")
    print("dag run:", run.state)
```
```text
audit row written
count_new_rows   success
has_new_rows     success
optional_export  skipped
transform        skipped
write_audit_row  success
dag run: success
```
With the default `ignore_downstream_trigger_rules=True`, the same run also marks `write_audit_row` as `skipped`.

**Real-World Use Case:**
A partner-feed DAG ran every hour, but the partner only published a few times a day. A short-circuit on "new manifest present" cut the run to two cheap tasks on most hours. The `ignore_downstream_trigger_rules=False` setting kept the audit-log task running, so operators could see each empty hour in the audit table.

**Common Mistakes / Interviewer Red Flags:**
- Expecting a downstream `all_done` task to run after a default short-circuit.
- Using a skip to hide errors. If the data should have arrived, fail or alert.
- Catching every exception and turning it into `AirflowSkipException`.

**Follow-up Questions Interviewers Ask:**
- How do you skip one mapped instance? — Raise `AirflowSkipException` inside it. Only that map index is skipped.
- What does `AirflowFailException` do differently? — It fails the task without using the remaining retries.

---

### Q527. What are task groups, and why did they replace SubDAGs?
**ID:** AF-09
**Topic:** Airflow > Task Groups
**Level:** Beginner
**Frequency:** Common
**Tags:** `airflow`, `task-groups`, `dag-structure`, `reuse`

**Short Answer (2–3 lines):**
A task group is a UI and authoring container. It folds related tasks into one collapsible box and prefixes their ids, as in `ingest_crm.load`. It does not change scheduling, so the tasks stay in the same DAG and run with the same executor and concurrency. SubDAGs ran a separate DAG with its own scheduling and deadlock problems, and they were removed in Airflow 3.0.

**Detailed Explanation:**
What task groups give you:
- **Reuse.** A `@task_group` function is a template. Call it once per source with `.override(group_id=...)` to stamp out identical pipelines.
- **Group-level `default_args`.** For example, give everything in the ingest group `retries=2`.
- **Dependencies on the whole group.** `group_a >> group_b` connects group_a's leaf tasks to group_b's root tasks.
- **Readable graphs.** Fifty tasks become five boxes.
- **Mapping.** A task group can be mapped with `expand()` to repeat a whole sub-pipeline per input (see Q530).

What they don't do:
- They add no separate concurrency limit or pool. Use pools (see Q547) or `max_active_tis_per_dagrun`.
- They don't isolate failures. A failure inside a group propagates like any other.
- `prefix_group_id=False` removes the prefix, but then task_ids must be unique across the whole DAG.

SubDAGs versus task groups:
| | SubDAG (removed in 3.0) | Task group |
|---|---|---|
| Scheduling | Separate DAG run | Same DAG run |
| Concurrency issues | Could deadlock the worker slots | None |
| UI | Separate view | Collapsible in the graph |
| Status in 3.x | Gone | Recommended |

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import dag, task, task_group


@task
def extract(source: str) -> str:
    return f"s3://raw/{source}/"

@task
def validate(path: str) -> str:
    return path

@task
def load(path: str) -> None:
    print("loading", path)


@task_group(default_args={"retries": 2})       # group-level defaults apply to its tasks
def ingest(source: str):
    load(validate(extract(source)))


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def ingest_sources():
    @task
    def publish():
        print("all sources loaded")

    groups = [ingest.override(group_id=f"ingest_{s}")(s) for s in ("crm", "billing")]
    groups >> publish()


dag_obj = ingest_sources()

if __name__ == "__main__":
    print(sorted(dag_obj.task_ids))
    print("retries:", dag_obj.get_task("ingest_crm.load").retries)
    print("publish upstream:", sorted(dag_obj.get_task("publish").upstream_task_ids))
    print(dag_obj.test().state)
```
```text
['ingest_billing.extract', 'ingest_billing.load', 'ingest_billing.validate', 'ingest_crm.extract', 'ingest_crm.load', 'ingest_crm.validate', 'publish']
retries: 2
publish upstream: ['ingest_billing.load', 'ingest_crm.load']
loading s3://raw/billing/
loading s3://raw/crm/
success
```

**Real-World Use Case:**
A marketing-data team pulled from six ad platforms, all with the same extract, validate and load shape. One `@task_group` function, called once per platform from a config list, replaced six copied blocks. A fix to validation now reached every platform in one change.

**Common Mistakes / Interviewer Red Flags:**
- Expecting a task group to limit concurrency or retry as a unit.
- Using a short task id in a branch's return value when the task lives inside a group.
- Calling task groups "SubDAGs" or proposing SubDAGs in Airflow 3.

**Follow-up Questions Interviewers Ask:**
- How do you retry a whole group? — Clear the group's tasks in the UI. Clearing a task can also clear its downstream tasks, and setup/teardown pairs inside it (see Q528).
- Task group or separate DAG? — Use a separate DAG when the part has its own schedule, owner or SLA, and link it with assets (see Q538).

---

### Q528. What are setup and teardown tasks, and how are they different from an `all_done` cleanup?
**ID:** AF-10
**Topic:** Airflow > Setup and Teardown
**Level:** Intermediate
**Frequency:** Occasional
**Tags:** `airflow`, `setup-teardown`, `resources`, `cleanup`

**Short Answer (2–3 lines):**
Setup and teardown tasks wrap work that needs a temporary resource, such as an ephemeral cluster, a temp schema or a lock. The teardown runs even when the work fails, as long as its setup succeeded. Teardowns are ignored when the DAG run's state is worked out. Clearing a work task also clears its setup and teardown, so a rerun gets a fresh resource.

**Detailed Explanation:**
Mark tasks with `@setup` / `@teardown`, or call `.as_setup()` / `.as_teardown(setups=...)` on existing operators. The tasks between a pair are in its **scope**.

Why this beats a plain cleanup task with `trigger_rule="all_done"`:

| Concern | `all_done` cleanup | Setup/teardown |
|---|---|---|
| Runs after a work failure | Yes | Yes |
| Runs if the resource was never created | Yes (and may error) | No. The fixed rule `all_done_setup_success` needs a setup to have succeeded |
| Effect on DAG run state | Can turn a failed run green when it is the leaf | Ignored by default; set `on_failure_fail_dagrun=True` to count it |
| Clearing one work task | Doesn't recreate the resource | Clears its setup and teardown as well |
| Graph | Ordinary edge | Dotted setup/teardown edge |

Rules:
- A teardown's trigger rule cannot be changed.
- Every task in a setup's scope implicitly needs that setup to succeed.
- Setups and teardowns can be attached to a whole task group, which gives the group a clear lifecycle.
- They are tasks, so they use worker slots and honour retries. Keep teardowns idempotent ("delete if exists"), because they may run twice.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import dag, setup, task, teardown


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def ephemeral_cluster():
    @setup
    def create_cluster() -> str:
        print("cluster created")
        return "cluster-42"

    @task
    def run_job(cluster_id: str):
        raise RuntimeError(f"job failed on {cluster_id}")

    @task
    def publish():
        print("published")

    @teardown
    def delete_cluster(cluster_id: str):
        print(f"deleted {cluster_id}")       # runs even though run_job failed

    cluster = create_cluster()
    job = run_job(cluster)
    job >> publish()
    # as_teardown links the setup to the teardown; the work in between is "in scope"
    job >> delete_cluster(cluster).as_teardown(setups=cluster)


dag_obj = ephemeral_cluster()

if __name__ == "__main__":
    run = dag_obj.test()
    for ti in sorted(run.get_task_instances(), key=lambda t: t.task_id):
        print(f"{ti.task_id:15} {ti.state}")
    print("dag run:", run.state)
```
```text
cluster created
deleted cluster-42
create_cluster  success
delete_cluster  success
publish         upstream_failed
run_job         failed
dag run: failed
```

**Real-World Use Case:**
A team spun up a short-lived Spark cluster for each nightly run. Before setup/teardown, a failed job left the cluster running until someone noticed it on the bill. After the change, the delete always ran. Clearing the failed job also created a new cluster, instead of trying to reuse the one that had been deleted.

**Common Mistakes / Interviewer Red Flags:**
- Using `all_done` for cleanup and then wondering why failed runs show green.
- Writing a teardown that fails if the resource is already gone.
- Expecting teardown failures to fail the run without `on_failure_fail_dagrun=True`.

**Follow-up Questions Interviewers Ask:**
- What happens if the setup fails? — The work tasks become `upstream_failed`, and the teardown does not run, because no setup succeeded.
- Can one teardown serve several setups? — Yes. Pass a list to `as_teardown(setups=[...])`.

---

### Q529. How do `partial()` and `expand()` work in dynamic task mapping, and what is a map index?
**ID:** AF-11
**Topic:** Airflow > Dynamic Task Mapping
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `airflow`, `dynamic-task-mapping`, `expand`, `map-index`

**Short Answer (2–3 lines):**
`expand()` creates one task instance per element of a list or dict, and the list can come from an upstream task at run time. `partial()` sets the arguments that stay the same for every copy. Each copy gets a `map_index` (0, 1, 2…), and a downstream task that takes the mapped output receives every result as a lazy sequence.

**Detailed Explanation:**
Mapping happens at **run time**. The DAG file says "one copy of `export` per region", and the scheduler creates the copies after `list_regions` has pushed its list. The DAG's shape stays fixed, so parsing stays fast (compare Q531).

The building blocks:
| API | Effect |
|---|---|
| `.partial(fmt="parquet")` | Constant arguments, plus task settings such as `retries` or `pool` |
| `.expand(region=[...])` | One instance per element. Several expand arguments give a **cross product** |
| `.expand_kwargs([{...}, {...}])` | One instance per dict: explicit pairs, no cross product |
| `xcom_arg.zip(other)` | Pair two lists by position before expanding |
| `map_index_template="{{ ... }}"` | Human-readable label in the UI instead of 0/1/2 |

Behaviour to remember:
- An **empty list** creates no instances, and the mapped task is marked `skipped`.
- The downstream "reduce" task gets a `LazySelectSequence`. Iterate it or call `list()` on it once; don't index into it over and over.
- `[core] max_map_length` (1024 by default) caps the number of instances. Cap concurrency with `max_active_tis_per_dag` or `max_active_tis_per_dagrun`.
- Each instance retries, fails and can be cleared on its own. You can rerun index 2 without touching 0 and 1.
- Only arguments that are mapped can vary per instance. Task settings such as `retries`, `pool` and `queue` go in `partial()` and apply to all instances.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def mapping_basics():
    @task
    def list_regions() -> list[str]:
        return ["eu", "us", "apac"]          # decided at run time, not parse time

    # partial(): constant for every copy. expand(): one copy per element.
    @task(map_index_template="{{ task.op_kwargs['region'] }}")   # nicer UI label than 0/1/2
    def export(region: str, fmt: str) -> int:
        rows = {"eu": 120, "us": 300, "apac": 80}[region]
        print(f"export {region} as {fmt}: {rows} rows")
        return rows

    @task
    def total(counts) -> None:                # receives a lazy sequence of all results
        print("total rows:", sum(counts))

    counts = export.partial(fmt="parquet").expand(region=list_regions())
    total(counts)

    # expand over two lists = cross product; expand_kwargs = explicit pairs
    @task
    def copy(src: str, dst: str):
        print(f"copy {src} -> {dst}")

    copy.expand_kwargs([{"src": "a.csv", "dst": "bronze/a"}, {"src": "b.csv", "dst": "bronze/b"}])


dag_obj = mapping_basics()

if __name__ == "__main__":
    run = dag_obj.test()
    print(sorted((ti.task_id, ti.map_index, ti.rendered_map_index) for ti in run.get_task_instances()
                 if ti.map_index >= 0))
    print(run.state)
```
```text
copy a.csv -> bronze/a
copy b.csv -> bronze/b
export us as parquet: 300 rows
export apac as parquet: 80 rows
export eu as parquet: 120 rows
total rows: 500
[('copy', 0, '0'), ('copy', 1, '1'), ('export', 0, 'eu'), ('export', 1, 'us'), ('export', 2, 'apac')]
success
```

**Real-World Use Case:**
A logistics company got between 3 and 40 carrier files a day. One mapped `load_file` task replaced a nightly job that had looped over the files inside a single task. A bad file now failed only its own map index. The on-call engineer fixed the file and cleared that one instance, and the other loads kept their results. (For choosing between mapping and other designs for this case, see the site's guide on processing a variable number of files.)

**Common Mistakes / Interviewer Red Flags:**
- Passing two lists to `expand()` and expecting them to be zipped. You get a cross product.
- Listing files at parse time to build tasks, which runs on every parse.
- Mapping over 50,000 tiny items. Batch them into chunks first, because every instance carries scheduling overhead.

**Follow-up Questions Interviewers Ask:**
- How do you pull one instance's XCom? — `ti.xcom_pull(task_ids="export", map_indexes=1)`.
- Can classic operators be mapped? — Yes, for example `BashOperator.partial(task_id="x").expand(bash_command=[...])`.

---

### Q530. How do you map a whole task group, filter inputs and limit fan-out?
**ID:** AF-12
**Topic:** Airflow > Dynamic Task Mapping
**Level:** Advanced
**Frequency:** Common
**Tags:** `airflow`, `dynamic-task-mapping`, `task-groups`, `concurrency`

**Short Answer (2–3 lines):**
Call `.expand()` on a `@task_group` function, and Airflow creates a copy of the whole sub-pipeline for each input. Copies run depth-first, so each item's second step can start as soon as that item's first step finishes. Shape inputs with an upstream filter task, `XComArg.map()` or `.zip()`. Use `max_active_tis_per_dagrun` and pools so a thousand-way fan-out doesn't take every worker slot.

**Detailed Explanation:**
**Mapped task groups.** `process.expand(file=files)` maps every task in the group with the same index. `process.validate[2]` therefore belongs to `process.parse[2]`. A downstream task that depends on the group's return value receives every copy's result.

**Shaping the input.**
| Tool | Runs as | Use for |
|---|---|---|
| Filter task returning a shorter list | A real task | Dropping items based on data |
| A mapped task returning `None` for an element | A real task | That element is left out of the next expansion |
| `xcom_arg.map(fn)` | Pre-processing inside the downstream task, not a separate task | Reshaping each element, such as building kwargs. `fn` must be a plain function; raise `AirflowSkipException` in it to skip one element, because returning `None` does not work |
| `a.zip(b)` / `a.concat(b)` | Pre-processing | Pairing or joining lists |

**Limits and back-pressure.**
- `[core] max_map_length` (1024 by default) is a hard ceiling. If an upstream task returns a longer list, *that upstream task* fails when it pushes the XCom, and the mapped task becomes `upstream_failed`. Chunk the input upstream.
- `max_active_tis_per_dagrun` limits running copies within one run. `max_active_tis_per_dag` limits them across all runs.
- A pool (see Q547) protects a shared external system across DAGs.
- Each instance is a full task: it is scheduled, logged and has its own XCom. Ten thousand instances can slow the scheduler and the UI. Batching 100 items per instance is often better.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import dag, task, task_group


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def mapped_groups():
    @task
    def list_files() -> list[str]:
        return ["orders_1.csv", "orders_2.csv", "README.txt", "orders_3.csv"]

    # .map() transforms each element lazily without an extra task; returning None is not
    # allowed, so filtering is done in a real task instead.
    @task
    def only_csv(files: list[str]) -> list[str]:
        return [f for f in files if f.endswith(".csv")]

    @task_group
    def process(file: str):
        @task(max_active_tis_per_dagrun=2)        # at most 2 copies of this task at once
        def parse(file: str) -> dict:
            return {"file": file, "rows": len(file) * 10}

        @task
        def validate(stats: dict) -> dict:
            if stats["rows"] <= 0:
                raise ValueError(f"empty file {stats['file']}")
            return stats

        return validate(parse(file))

    @task
    def summarise(results) -> None:
        results = list(results)                   # materialise the lazy sequence once
        print(f"{len(results)} files, {sum(r['rows'] for r in results)} rows")

    csv_files = only_csv(list_files())
    upper = csv_files.map(lambda f: f.upper())    # map() on an XComArg: no new task
    summarise(process.expand(file=upper))


dag_obj = mapped_groups()

if __name__ == "__main__":
    run = dag_obj.test()
    mapped = sorted({(ti.task_id, ti.map_index) for ti in run.get_task_instances() if ti.map_index >= 0})
    print(mapped)
    print(run.state)
```
```text
3 files, 360 rows
[('process.parse', 0), ('process.parse', 1), ('process.parse', 2), ('process.validate', 0), ('process.validate', 1), ('process.validate', 2)]
success
```

**Real-World Use Case:**
A media company processed each partner's daily delivery with download, virus-scan and load steps. They mapped a task group over the partner list and set `max_active_tis_per_dagrun=5` on the download step, because the partners' SFTP servers throttled them. A slow partner no longer held up the others, since each copy moved to its next step on its own.

**Common Mistakes / Interviewer Red Flags:**
- Passing an Airflow task to `.map()`. It must be a plain function.
- Assuming Airflow truncates a list longer than `max_map_length`. It fails the task that produced the list.
- Calling `list()` on the lazy sequence many times inside a loop, which pulls every XCom each time.

**Follow-up Questions Interviewers Ask:**
- Can you branch inside a mapped task group? — Yes. The branch result applies per map index, so the chosen task_ids must be inside the group.
- How do you rerun only failed items? — Clear the failed map indexes. Successful copies keep their state and XComs.

---

### Q531. Generating many DAGs from config vs dynamic task mapping: when do you use each?
**ID:** AF-13
**Topic:** Airflow > Dynamic DAGs
**Level:** Intermediate
**Frequency:** Common
**Tags:** `airflow`, `dynamic-dags`, `dag-factory`, `parsing`

**Short Answer (2–3 lines):**
Generate separate DAGs when units need their own schedule, owner, permissions or history, for example one DAG per tenant. Use dynamic task mapping when the number of items changes from run to run inside one pipeline. When you generate DAGs, the config must be static and cheap to read at parse time, and `get_parsing_context()` lets workers skip building DAGs they don't need.

**Detailed Explanation:**
| Question | Many generated DAGs | One DAG with mapping |
|---|---|---|
| Different schedules or SLAs per unit? | Yes | No |
| Separate pause, permissions or tags? | Yes | No |
| Count changes run to run? | Awkward: the DAG list changes | Natural |
| Parse cost | Grows with the number of DAGs | Constant |
| Where the item list comes from | Parse time: a YAML/JSON file in the bundle | Run time: an upstream task |

Rules for DAG factories:
- **The config travels with the code.** Use a YAML file in the bundle, not a database query or an API call. Parse-time I/O runs every 30 seconds or so, and if it fails, DAGs disappear.
- **Use a plain builder function** that takes Python values and returns a `DAG`. Arguments of an `@dag`-decorated function become DAG **params**, not Python values. Passing a list that way and then calling `.expand()` on it fails at parse time with `expand() got an unexpected type 'DagParam'`.
- **Keep dag_ids stable.** Renaming a generated id orphans its history.
- **`get_parsing_context().dag_id`** is set when a worker parses the file to run one task. Skipping the other DAGs then speeds up task start. The DAG processor leaves it as `None` and builds all of them.
- Libraries such as DAG Factory build DAGs from YAML. They are useful for platform teams, but they still parse as Python.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
# One file -> one DAG per tenant. The config is a static file shipped with the code,
# so parsing stays fast and deterministic (no API or DB calls at top level).
from datetime import datetime
from airflow.sdk import DAG, get_parsing_context, task

TENANTS = {                       # in practice: yaml.safe_load(open(".../tenants.yaml"))
    "acme":   {"schedule": "0 2 * * *", "tables": ["orders", "invoices"]},
    "globex": {"schedule": "30 3 * * *", "tables": ["orders"]},
}


def build_dag(tenant: str, cfg: dict) -> DAG:
    # A plain builder function: arguments of an @dag-decorated function would
    # become DAG params instead of Python values.
    with DAG(dag_id=f"export_{tenant}", schedule=cfg["schedule"], start_date=datetime(2026, 1, 1),
             catchup=False, tags=["export", tenant]) as d:
        @task
        def export_table(table: str):
            print(f"exporting {tenant}.{table}")

        export_table.expand(table=cfg["tables"])
    return d


# A worker running one task sets the parsing context, so it can skip the other DAGs.
# The DAG processor leaves it unset and builds all of them.
current = get_parsing_context().dag_id
for tenant, cfg in TENANTS.items():
    if current is None or current == f"export_{tenant}":
        build_dag(tenant, cfg)       # a DAG created at module level is registered
```
```python
# Runtime: Python 3.11, Apache Airflow 3.3 — check script outside the DAG folder
from airflow.dag_processing.dagbag import DagBag
bag = DagBag(dag_folder="dags")
for dag_id in sorted(bag.dag_ids):
    d = bag.get_dag(dag_id)
    print(dag_id, type(d.timetable).__name__, repr(d.timetable.expression), sorted(d.tags))
print("errors:", bag.import_errors)
```
```text
export_acme CronTriggerTimetable '0 2 * * *' ['acme', 'export']
export_globex CronTriggerTimetable '30 3 * * *' ['export', 'globex']
errors: {}
```

**Real-World Use Case:**
A B2B SaaS company ran one export DAG per customer, generated from a `tenants.yaml` file kept in the DAG repo. Each customer had its own schedule in its own time zone, and support staff could pause a single customer's DAG. Inside each DAG, the tables were mapped, because the table list changed whenever a customer enabled a module.

**Common Mistakes / Interviewer Red Flags:**
- Building the DAG list from a database query at parse time.
- Generating thousands of near-identical DAGs where one mapped DAG would do.
- Changing dag_ids on every deploy, for example by including a version or date in them.

**Follow-up Questions Interviewers Ask:**
- How do you test a factory? — Load the DagBag in CI, assert the expected dag_ids, and check that there are no import errors (see Q560).
- What happens when a tenant is removed from the YAML? — Its DAG stops being parsed and is marked stale. Its history stays in the DB until someone deletes it.

---

### Q532. How does Jinja templating work in Airflow, and which macros should you know?
**ID:** AF-14
**Topic:** Airflow > Templating and Macros
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `airflow`, `jinja`, `macros`, `templating`

**Short Answer (2–3 lines):**
Attributes listed in an operator's `template_fields` are rendered with Jinja on the worker, just before `execute()`. The run's context is available: `ds`, `data_interval_start`, `logical_date`, `run_id`, `params`, `var`, `conn`, `macros` and more. Templates are how one DAG file processes a different date in every run without changing any code.

**Detailed Explanation:**
- **What is templated:** only the fields named in `template_fields`, such as `bash_command` or `sql`. A string ending in a `template_ext` extension like `.sql` is loaded as a template file, searched under the DAG folder and `template_searchpath`.
- **When:** at task run time on the worker. The DAG file only holds the template string, so `{{ ds }}` costs nothing at parse time.
- **TaskFlow:** name context keys as function arguments (`ds=None`, `data_interval_start=None`), or call `get_current_context()`. You don't need Jinja inside Python.

Context and macros used most:
| Expression | Value |
|---|---|
| `{{ ds }}` / `{{ ds_nodash }}` | Logical date as `YYYY-MM-DD` / `YYYYMMDD` |
| `{{ data_interval_start }}`, `{{ data_interval_end }}` | Pendulum datetimes for the run's interval |
| `{{ data_interval_end \| ds }}` | Filters format datetimes (`ds`, `ds_nodash`, `ts`) |
| `{{ macros.ds_add(ds, -7) }}` | Date arithmetic on `ds` strings |
| `{{ macros.ds_format(ds, "%Y-%m-%d", "%d/%m/%Y") }}` | Reformat a date string |
| `{{ params.region }}` | Run parameters (see Q533) |
| `{{ var.value.k }}`, `{{ conn.id.host }}` | Variables and connections (see Q522) |

Airflow 3 removed the old keys `execution_date`, `next_ds`, `prev_ds`, `yesterday_ds` and `tomorrow_ds`. Use `logical_date` and the `data_interval_*` keys, plus `macros.ds_add` when you need date arithmetic. Templates render to strings by default. Set `render_template_as_native_obj=True` on the DAG, or on a single operator since 3.2, to get Python lists and dicts back.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime, timezone
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


def fiscal_quarter(d) -> str:              # a custom macro: plain Python
    return f"FY{d.year}Q{(d.month - 1) // 3 + 1}"


@dag(
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    user_defined_macros={"fiscal_quarter": fiscal_quarter},
)
def templating_demo():
    BashOperator(
        task_id="export",
        # Rendered on the worker just before execute(); the DAG file only holds the string.
        bash_command=(
            "echo ds={{ ds }} "
            "start={{ data_interval_start | ds }} "
            "week_ago={{ macros.ds_add(ds, -7) }} "
            "compact={{ ds_nodash }} "
            "fq={{ fiscal_quarter(data_interval_start) }} "
            "run={{ run_id }}"
        ),
    )

    @task
    def show_context(ds=None, data_interval_start=None, data_interval_end=None):
        # TaskFlow: name the context keys you need as arguments
        print("ds:", ds, "| interval:", data_interval_start, "->", data_interval_end)

    @task
    def removed_keys(**context):
        print("has yesterday_ds:", "yesterday_ds" in context, "| has execution_date:", "execution_date" in context)

    show_context() >> removed_keys()


dag_obj = templating_demo()

if __name__ == "__main__":
    print(dag_obj.test(logical_date=datetime(2026, 5, 20, 6, tzinfo=timezone.utc)).state)
```
```text
ds: 2026-05-20 | interval: 2026-05-20 06:00:00+00:00 -> 2026-05-20 06:00:00+00:00
... ds=2026-05-20 start=2026-05-20 week_ago=2026-05-13 compact=20260520 fq=FY2026Q2 run=manual__2026-10-17T13:42:04.945864+00:00
has yesterday_ds: False | has execution_date: False
success
```
The interval has zero length because a cron string in Airflow 3 uses `CronTriggerTimetable` by default (see Q535).

**Real-World Use Case:**
A warehouse load used `sql="sql/load_orders.sql"`, and the SQL file filtered on `'{{ data_interval_start }}' <= order_ts AND order_ts < '{{ data_interval_end }}'`. Clearing any past run reloaded exactly that window. The team reviewed the rendered SQL in the UI's "Rendered Template" tab before running backfills.

**Common Mistakes / Interviewer Red Flags:**
- Using `{{ ds }}` in a field that isn't in `template_fields`, which leaves the literal braces in the output.
- Building dates with `datetime.now()` in Python instead of using the run's interval.
- Using removed 2.x keys such as `execution_date` or `yesterday_ds` in Airflow 3.

**Follow-up Questions Interviewers Ask:**
- How do you template a field on a custom operator? — Add its attribute name to `template_fields` (see Q521).
- Where do you see the rendered values? — The task instance's Rendered Template view, or `airflow tasks render`.

---

### Q533. How do Params work, and how do you trigger a DAG with validated inputs?
**ID:** AF-15
**Topic:** Airflow > Params
**Level:** Intermediate
**Frequency:** Common
**Tags:** `airflow`, `params`, `manual-runs`, `validation`

**Short Answer (2–3 lines):**
`params` declares typed, JSON-Schema-validated inputs with defaults on a DAG or task. A manual trigger, the API or `dag_run.conf` can override them, and invalid values are rejected before the run is created. Tasks read them as `params` in the context or as `{{ params.x }}` in templates. The trigger form in the UI is generated from them.

**Detailed Explanation:**
- **Declare:** `Param(default, type=..., enum=..., minimum=..., maximum=..., description=...)`. Types follow JSON Schema: `string`, `integer`, `number`, `boolean`, `array`, `object` and `null`, plus formats such as `date`.
- **Override:** the trigger form, `airflow dags trigger --conf '{...}'`, the REST API or `TriggerDagRunOperator(conf=...)`. With `[core] dag_run_conf_overrides_params = True` (the default), conf values replace the defaults.
- **Validate:** values are checked against the schema when the run is created. A scheduled run uses the defaults, so every default must be valid.
- **Read:** `params["region"]` in TaskFlow, or `{{ params.region }}` in templates. Task-level params merge over DAG-level ones.

Params versus other inputs:
| Input | Use for |
|---|---|
| Params | Run-scoped options: reprocess a region, run dry |
| Variables | Environment settings that are the same for every run |
| Connections | Credentials |
| XCom | Values produced during the run |

Good practice: give every param a safe default, so scheduled runs need no conf. Use `enum` for choices. Document params with `description`, which appears in the trigger form. Treat conf as untrusted user input: never paste it straight into SQL; bind it as a query parameter or check it against an allow-list.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import Param, dag, task


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "region": Param("eu", type="string", enum=["eu", "us", "apac"]),
        "lookback_days": Param(1, type="integer", minimum=1, maximum=31),
        "dry_run": Param(False, type="boolean", description="Log the SQL only"),
        "tables": Param(["orders"], type="array", items={"type": "string"}),
    },
)
def reprocess_region():
    @task
    def plan(params=None):
        print(f"region={params['region']} lookback={params['lookback_days']} "
              f"dry_run={params['dry_run']} tables={params['tables']}")

    plan()


dag_obj = reprocess_region()

if __name__ == "__main__":
    print(dag_obj.test().state)                                    # defaults
    print(dag_obj.test(run_conf={"region": "us", "lookback_days": 7,
                                 "tables": ["orders", "refunds"]}).state)
    try:
        dag_obj.test(run_conf={"lookback_days": 90})
    except Exception as e:
        print(type(e).__name__, "->", str(e).splitlines()[0])
```
```text
region=eu lookback=1 dry_run=False tables=['orders']
success
region=us lookback=7 dry_run=False tables=['orders', 'refunds']
success
ParamValidationError -> Invalid input for param lookback_days: 90 is greater than the maximum of 31
```

**Real-World Use Case:**
A data team gave its support analysts a "reprocess region" DAG with no schedule. The analysts triggered it from the UI form and picked a region from a drop-down plus a lookback of up to 31 days. The schema blocked a 365-day request that would have overloaded the warehouse, and the engineers no longer had to run CLI commands for these requests.

**Common Mistakes / Interviewer Red Flags:**
- Reading `dag_run.conf["x"]` directly with no default or validation.
- Formatting conf values straight into SQL strings.
- Using Variables as run inputs. A Variable is shared by every run.

**Follow-up Questions Interviewers Ask:**
- Can a scheduled run receive params? — Only the defaults. Params are meant for manual or API-triggered runs.
- How do you pass params to a downstream DAG? — `TriggerDagRunOperator(conf={...})`, which the target validates against its own params (see Q542).

---
