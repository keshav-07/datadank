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

### Q534. What can you pass to `schedule`, and what does each option create?
**ID:** AF-16
**Topic:** Airflow > Scheduling
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `airflow`, `scheduling`, `cron`, `timetables`

**Short Answer (2–3 lines):**
`schedule` takes `None` (manual or API only), a cron string or preset such as `"@daily"`, a `timedelta`, `"@once"`, `"@continuous"`, one or more assets, or a timetable object. Airflow turns every one of them into a **timetable**. In Airflow 3, cron strings and presets become `CronTriggerTimetable` by default, which runs *at* the cron time instead of after an interval closes.

**Detailed Explanation:**
| Value | Timetable created (3.3) | Runs |
|---|---|---|
| `None` | `NullTimetable` | Only when triggered |
| `"0 2 * * *"`, `"@daily"`, `"@hourly"`, `"@weekly"`, `"@monthly"`, `"@quarterly"`, `"@yearly"` | `CronTriggerTimetable` | At each cron tick |
| `timedelta(hours=4)` | `DeltaTriggerTimetable` | Every 4 h from `start_date` |
| `"@once"` | `OnceTimetable` | One run |
| `"@continuous"` | `ContinuousTimetable` | A new run as soon as the last one ends (needs `max_active_runs=1`) |
| `[asset]`, `a & b`, `a \| b` | `AssetTriggeredTimetable` | When assets update (see Q538) |
| `AssetOrTimeSchedule(...)` | Itself | On a timer **or** on asset updates |
| Custom `Timetable` subclass | Itself | Your own rules (see Q535) |

Other settings that shape scheduling:
- **`start_date`** is the earliest date that can be scheduled. Use a fixed, timezone-aware date, never `datetime.now()`. Its timezone, or the default timezone (UTC), sets the cron's timezone, so `"0 6 * * *"` with a `Europe/London` start date follows daylight-saving changes.
- **`catchup`** is `False` by default in Airflow 3 (`[scheduler] catchup_by_default`). With `True`, Airflow creates every missed run since `start_date` (see Q45).
- **`end_date`** stops scheduling after a date.
- **`max_active_runs`** limits concurrent runs of the DAG (see Q549).

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime, timedelta
from airflow.sdk import DAG, Asset

schedules = {
    "None (manual/API only)": None,
    '"@daily" preset': "@daily",
    '"15 2 * * 1-5" cron': "15 2 * * 1-5",
    "timedelta(hours=4)": timedelta(hours=4),
    '"@once"': "@once",
    '"@continuous"': "@continuous",
    "[Asset(...)]": [Asset("s3://lake/orders/")],
}
for label, sched in schedules.items():
    kwargs = {"max_active_runs": 1} if sched == "@continuous" else {}
    d = DAG(f"d{abs(hash(label))}", schedule=sched, start_date=datetime(2026, 1, 1), **kwargs)
    print(f"{label:24} -> {type(d.timetable).__name__}")
```
```text
None (manual/API only)   -> NullTimetable
"@daily" preset          -> CronTriggerTimetable
"15 2 * * 1-5" cron      -> CronTriggerTimetable
timedelta(hours=4)       -> DeltaTriggerTimetable
"@once"                  -> OnceTimetable
"@continuous"            -> ContinuousTimetable
[Asset(...)]             -> AssetTriggeredTimetable
```

**Real-World Use Case:**
A retail team scheduled store-sales loads at `"30 5 * * *"` with a `start_date` in `America/New_York`. The loads stayed at 05:30 local time through the daylight-saving changes, so the morning reports were ready when stores opened, without anyone editing UTC offsets twice a year.

**Common Mistakes / Interviewer Red Flags:**
- Using `start_date=datetime.now()`. The start date moves on every parse and runs never begin.
- Using `schedule_interval` or `timetable=`, which were removed in Airflow 3. Use `schedule`.
- Expecting `"@daily"` in Airflow 3 to run at midnight for "yesterday" with a one-day data interval. That is the old `CronDataIntervalTimetable` behaviour (see Q535).

**Follow-up Questions Interviewers Ask:**
- How do you see the next run? — `airflow dags next-execution <dag_id>`, or the DAG header in the UI.
- How do you run on the 3rd business day of the month? — A custom timetable, or a daily cron plus a short-circuit (see Q526).

---

### Q535. `CronTriggerTimetable` vs `CronDataIntervalTimetable`: what changed in Airflow 3, and when do you write a custom timetable?
**ID:** AF-17
**Topic:** Airflow > Timetables
**Level:** Intermediate
**Frequency:** Common
**Tags:** `airflow`, `timetables`, `data-interval`, `airflow-3`

**Short Answer (2–3 lines):**
`CronDataIntervalTimetable`, the 2.x behaviour, gives each run an interval and starts it only after that interval ends. `CronTriggerTimetable`, the 3.x default for cron strings, fires at the cron time, and by default the data interval has zero length. Pass `interval=` to get a look-back window. Write a custom timetable only when cron, `MultipleCronTriggerTimetable`, `EventsTimetable` or a delta timetable can't express the calendar.

**Detailed Explanation:**
The first run for `"0 6 * * *"` with `start_date` 2026-03-01 (output below):

| Timetable | logical_date | data interval | actually runs at |
|---|---|---|---|
| `CronTriggerTimetable` (3.x default) | 03-01 06:00 | 03-01 06:00 → 03-01 06:00 | 03-01 06:00 |
| `CronTriggerTimetable(interval=1 day)` | 02-28 06:00 | 02-28 06:00 → 03-01 06:00 | 03-01 06:00 |
| `CronDataIntervalTimetable` (2.x default) | 03-01 06:00 | 03-01 06:00 → 03-02 06:00 | 03-02 06:00 |

Why this matters when you migrate: `{{ ds }}` and `data_interval_*` move between the two timetables. A task filtering on `data_interval_start <= ts < data_interval_end` would select **nothing** under a zero-length interval. Options:
- Set `[scheduler] create_cron_data_intervals = True` to keep the 2.x semantics for every cron-string DAG. Do it *before* upgrading.
- Pass an explicit `CronDataIntervalTimetable(...)` on the DAGs that depend on intervals.
- Or use `CronTriggerTimetable(..., interval=timedelta(days=1))` and check your templates.

Built-in timetables to know: `DeltaTriggerTimetable` / `DeltaDataIntervalTimetable`, `MultipleCronTriggerTimetable` (several crons, one DAG), `EventsTimetable` (a fixed list of datetimes), `AssetOrTimeSchedule`, and the partition timetables from 3.2+ (see Q541).

A **custom timetable** subclasses `Timetable` and implements `next_dagrun_info` and `infer_manual_data_interval`. It is registered through a plugin, because the scheduler must import it to deserialise DAGs. Typical cases are trading-day calendars and "business day N of the month".

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
# Compare how two timetables label and time the first run for "every day at 06:00".
from datetime import datetime, timedelta, timezone
from airflow.timetables.base import TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.timetables.trigger import CronTriggerTimetable

start = datetime(2026, 3, 1, tzinfo=timezone.utc)
tables = {
    "CronTriggerTimetable (3.x default for cron strings)": CronTriggerTimetable("0 6 * * *", timezone="UTC"),
    "CronTriggerTimetable with interval=1 day": CronTriggerTimetable(
        "0 6 * * *", timezone="UTC", interval=timedelta(days=1)),
    "CronDataIntervalTimetable (2.x behaviour)": CronDataIntervalTimetable("0 6 * * *", timezone="UTC"),
}
for name, tt in tables.items():
    info = tt.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(earliest=start, latest=None, catchup=True),
    )
    print(name)
    print("  logical_date:", info.logical_date)
    print("  interval:    ", info.data_interval.start, "->", info.data_interval.end)
    print("  runs at:     ", info.run_after)
```
```text
CronTriggerTimetable (3.x default for cron strings)
  logical_date: 2026-03-01 06:00:00+00:00
  interval:     2026-03-01 06:00:00+00:00 -> 2026-03-01 06:00:00+00:00
  runs at:      2026-03-01 06:00:00+00:00
CronTriggerTimetable with interval=1 day
  logical_date: 2026-02-28 06:00:00+00:00
  interval:     2026-02-28 06:00:00+00:00 -> 2026-03-01 06:00:00+00:00
  runs at:      2026-03-01 06:00:00+00:00
CronDataIntervalTimetable (2.x behaviour)
  logical_date: 2026-03-01 06:00:00+00:00
  interval:     2026-03-01 06:00:00+00:00 -> 2026-03-02 06:00:00+00:00
  runs at:      2026-03-02 06:00:00+00:00
```

**Real-World Use Case:**
After an upgrade to Airflow 3, a team's hourly incremental loads started reporting zero rows. Their SQL filtered on `data_interval_start`/`data_interval_end`, and the new default timetable gave a zero-length interval. They switched those DAGs to an explicit `CronDataIntervalTimetable` and added a CI test that asserts the interval length of every interval-based DAG.

**Common Mistakes / Interviewer Red Flags:**
- Saying "a daily DAG always runs for yesterday". That depends on the timetable.
- Changing `create_cron_data_intervals` after Airflow 3 runs already exist. Logical dates shift, and runs can be duplicated or skipped.
- Writing a custom timetable without registering it in a plugin.

**Follow-up Questions Interviewers Ask:**
- Which one fits "run at 9:00 and use whatever is there"? — `CronTriggerTimetable`, the default.
- How does `EventsTimetable` help? — It schedules runs on an explicit list of dates, such as earnings dates, with no cron at all.

---

### Q536. How do scheduler-managed backfills work in Airflow 3?
**ID:** AF-18
**Topic:** Airflow > Backfills
**Level:** Intermediate
**Frequency:** Common
**Tags:** `airflow`, `backfill`, `reprocessing`, `airflow-3`

**Short Answer (2–3 lines):**
In Airflow 3 a backfill is a database object. The scheduler creates and runs its DAG runs like any others, with `max_active_runs`, pools and the normal executors. You start one from the UI, the REST API or `airflow backfill create`. You choose the date range and a **reprocess behaviour** (`none`, `failed` or `completed`), and you can run it backwards or as a dry run.

**Detailed Explanation:**
In 2.x, `airflow dags backfill` ran a loop in your terminal: closing the terminal killed it, and it ignored part of the scheduler's limits. Airflow 3 moved backfills into the scheduler (AIP-78).

| Option | Meaning |
|---|---|
| `--from-date` / `--to-date` | Logical-date window. Both ends are inclusive, but they are datetimes: a `--to-date` of midnight leaves out a 03:00 run on that day |
| `--reprocess-behavior none` | Only create runs for dates that have none (the default) |
| `failed` | Also re-run dates whose latest run failed |
| `completed` | Re-run every date, including successful ones |
| `--max-active-runs` | Concurrency for *this* backfill, applied separately from the DAG's own `max_active_runs` |
| `--run-backwards` | Newest dates first. Not allowed when tasks use `depends_on_past` |
| `--dag-run-conf` | Conf/params for every run |
| `--run-on-latest-version` | Use the latest bundle version instead of each run's original one (the default for backfills; see Q558) |
| `--dry-run` | List the runs it would create |

Backfill runs have `run_type=backfill`, appear in the grid, and can be paused or cancelled. A date that is currently running or queued is never duplicated. Backfills need a time-based schedule. For DAGs on partition timetables, the window is read as partition dates.

The DAG still has to be **idempotent per interval** (see Q46, Q563), because a backfill simply replays history.

**Example / Code:**
```bash
# Runtime: Apache Airflow 3.3 CLI; DAG "daily_revenue" has schedule "0 3 * * *"
airflow backfill create --dag-id daily_revenue \
  --from-date 2026-03-01 --to-date 2026-03-05 \
  --reprocess-behavior failed --max-active-runs 2 --dry-run
```
```text
Performing dry run of backfill.
Printing params:
    - dag_id = daily_revenue
    - from_date = 2026-03-01 00:00:00+00:00
    - to_date = 2026-03-05 00:00:00+00:00
    - max_active_runs = 2
    - reverse = False
    - dag_run_conf = None
    - reprocess_behavior = ReprocessBehavior.FAILED
    - run_on_latest_version = True
Runs to be attempted:
+---------------------------+-----------------+------------------+
| logical_date              | partition_key   | partition_date   |
+===========================+=================+==================+
| 2026-03-01 03:00:00+00:00 |                 |                  |
+---------------------------+-----------------+------------------+
| 2026-03-02 03:00:00+00:00 |                 |                  |
+---------------------------+-----------------+------------------+
| 2026-03-03 03:00:00+00:00 |                 |                  |
+---------------------------+-----------------+------------------+
| 2026-03-04 03:00:00+00:00 |                 |                  |
+---------------------------+-----------------+------------------+
```
The 5 March run is missing because the window ends at 00:00 that day. Use `--to-date 2026-03-05T23:59` to include it.

**Real-World Use Case:**
A currency-conversion bug inflated revenue for six weeks. After deploying the fix, the team ran a dry run, then `--reprocess-behavior completed --max-active-runs 3 --run-backwards`. The most recent weeks, which finance needed first, were corrected within hours, and the warehouse never ran more than three rebuilds at once.

**Common Mistakes / Interviewer Red Flags:**
- Describing the 2.x `airflow dags backfill` CLI loop as current behaviour.
- Assuming the default (`none`) reprocesses dates that already ran. It only fills gaps.
- Backfilling a DAG whose tasks read "latest" data instead of their own interval.

**Follow-up Questions Interviewers Ask:**
- Catchup or backfill? — Catchup automatically fills missed intervals since `start_date`. A backfill is an explicit range that you start (see Q45).
- How do you protect production during a big backfill? — Use a low `--max-active-runs`, pools on the heavy tasks, and lower priority weights (see Q547, Q548).

---

### Q537. Manual and API-triggered runs in Airflow 3: why can `logical_date` be `None`?
**ID:** AF-19
**Topic:** Airflow > Dag Runs
**Level:** Intermediate
**Frequency:** Common
**Tags:** `airflow`, `manual-runs`, `logical-date`, `airflow-3`

**Short Answer (2–3 lines):**
Airflow 3 separates *when a run may start* (`run_after`) from *which data it represents* (`logical_date`). Asset-triggered runs, and API or UI triggers that give no date, get `logical_date=None` and no data interval. That lets many runs exist side by side, for inference or ad-hoc jobs. Tasks that support several trigger types must handle a missing date.

**Detailed Explanation:**
| Run created by | `logical_date` | data interval | `run_after` |
|---|---|---|---|
| Schedule (cron/timetable) | Set by the timetable | Set by the timetable | When it may start |
| UI/API trigger **with** a date | That date | Inferred by the timetable | Trigger time |
| UI/API trigger **without** a date (REST default) | `None` | Absent | Trigger time |
| Asset event | `None` | Absent | Event time |
| Backfill | Each date in the range | From the timetable | Scheduled by the backfill |

What changed compared with 2.x:
- A logical date was always present and unique per DAG. With `None`, multiple runs no longer collide, which is why inference-style DAGs (AIP-83) became possible.
- Triggering with a **future** logical date is no longer allowed. Use `logical_date=None` instead.
- When the run has no logical date, context keys such as `data_interval_start` are missing from the context. The 3.0 release notes say reading them raises a `KeyError`.
- The `conf` context key and `dag_run.external_trigger` were removed. Use `params` and `dag_run.run_type`.

Defensive pattern: read `dag_run.logical_date`, fall back to `dag_run.run_after` or a param, and log which one you used. Or make the date a required `Param` on DAGs that are only triggered manually (see Q533).

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime, timezone
from airflow.sdk import dag, task


@dag(schedule="0 4 * * *", start_date=datetime(2026, 1, 1), catchup=False)
def run_types_demo():
    @task
    def which_day(**context) -> str:
        dr = context["dag_run"]
        # logical_date is None for API/UI triggers without a date and for asset-triggered runs.
        # Fall back to run_after, which every run has, and say so in the log.
        anchor = dr.logical_date or dr.run_after
        print(f"run_type={dr.run_type} logical_date={dr.logical_date} "
              f"run_after={dr.run_after:%Y-%m-%d %H:%M} -> processing {anchor:%Y-%m-%d}")
        print("has data_interval_start:", "data_interval_start" in context)
        return anchor.strftime("%Y-%m-%d")

    which_day()


dag_obj = run_types_demo()

if __name__ == "__main__":
    dag_obj.test(logical_date=datetime(2026, 4, 2, 4, tzinfo=timezone.utc))   # like a scheduled/dated run
    dag_obj.test(logical_date=None, run_after=datetime(2026, 4, 9, 10, 30, tzinfo=timezone.utc))
```
```text
run_type=DagRunType.MANUAL logical_date=2026-04-02 04:00:00+00:00 run_after=2026-10-17 13:45 -> processing 2026-04-02
has data_interval_start: True
run_type=DagRunType.MANUAL logical_date=None run_after=2026-04-09 10:30 -> processing 2026-04-09
has data_interval_start: False
```

**Real-World Use Case:**
A fraud team's scoring DAG was triggered through the REST API for every batch of flagged transactions, often dozens of times an hour. In 2.x each trigger needed a unique, made-up logical date. In 3.x they trigger without one and pass the batch id as a param. Each run reads its inputs from conf, and nothing in the DAG depends on dates.

**Common Mistakes / Interviewer Red Flags:**
- Assuming every run has `ds` and a data interval.
- Inventing future logical dates to make runs unique.
- Using `run_after` as the data date for scheduled runs. It is when the run may start, not which data it covers.

**Follow-up Questions Interviewers Ask:**
- How do you trigger a dated rerun from the CLI? — `airflow dags trigger <dag_id> --logical-date 2026-04-02T04:00:00+00:00`.
- Which data interval does a manual run with a date get? — Whatever the timetable infers. Don't assume it equals the logical date; check the context (see Q535).

---

### Q538. How does asset-aware scheduling work, and how do you combine asset conditions?
**ID:** AF-20
**Topic:** Airflow > Assets
**Level:** Intermediate
**Frequency:** Very Common
**Tags:** `airflow`, `assets`, `data-aware-scheduling`, `airflow-3`

**Short Answer (2–3 lines):**
A task lists an `Asset` in `outlets`. When the task succeeds, Airflow records an asset event. A DAG with `schedule=[asset]`, or a condition such as `(a & b) | c`, is queued once the condition is met by events received since its last run. Events can carry `extra` metadata, and consumers read them through `triggering_asset_events`.

**Detailed Explanation:**
- **Identity.** In Airflow 3, `Asset(name=..., uri=...)` can be identified by name, URI or both. The URI is normalised, so a trailing `/` is dropped. Pick one convention and use it everywhere.
- **Producing.** A task with `outlets=[asset]` emits an event when it **succeeds**. Use `outlet_events[asset].extra = {...}` to attach row counts, partitions or paths. Events can also come from the REST API, or from other DAGs through `AssetAlias`.
- **Consuming.** `schedule=[a, b]` means *all of them*. `a & b` is all, `a | b` is any, and the operators nest. Each consumer keeps a queue of the events received since its last run. Two producer events before one consumer run give **one** run that sees both events.
- **Mixing with time.** `AssetOrTimeSchedule(timetable=..., assets=...)` runs on a timer *or* on updates.
- **What it doesn't do.** Airflow doesn't check the data. An event means "the producer task succeeded", not "the data is valid". Put quality checks before the outlet task.

| | Assets | `TriggerDagRunOperator` | `ExternalTaskSensor` |
|---|---|---|---|
| Coupling | Producer doesn't know its consumers | Producer names the target | Consumer names producer and task |
| Waiting | None; event-driven | None; push | Polls or defers |
| Lineage in the UI | Asset graph | Partial | Partial |

See Q542 for the trade-offs and the site's guide on cross-DAG dependencies.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import Asset, dag, task

orders = Asset(name="silver_orders", uri="s3://lake/silver/orders/")
customers = Asset(name="silver_customers", uri="s3://lake/silver/customers/")
fx_rates = Asset(name="fx_rates", uri="s3://lake/ref/fx_rates/")


@dag(schedule="@hourly", start_date=datetime(2026, 1, 1), catchup=False)
def load_orders():
    @task(outlets=[orders])          # success of this task emits an asset event
    def write_orders(*, outlet_events):
        outlet_events[orders].extra = {"rows": 1834, "partition": "2026-05-20T10"}

    write_orders()


# Runs when orders AND customers have both updated since the last run, OR fx_rates updated.
@dag(schedule=(orders & customers) | fx_rates, start_date=datetime(2026, 1, 1), catchup=False)
def build_revenue_mart():
    @task
    def build(*, triggering_asset_events):
        for asset, events in triggering_asset_events.items():
            print(asset.name, [e.extra for e in events])

    build()


producer = load_orders()
consumer = build_revenue_mart()

if __name__ == "__main__":
    print("consumer timetable:", type(consumer.timetable).__name__)
    def show(c):  # compact view of the condition tree
        return c.name if hasattr(c, "name") else f"{type(c).__name__}({', '.join(show(o) for o in c.objects)})"
    print("condition:", show(consumer.timetable.asset_condition))
    print("normalised uri:", orders.uri)
    print("producer run:", producer.test().state)
    from airflow.models.asset import AssetEvent
    from airflow.utils.session import create_session
    with create_session() as s:
        for ev in s.query(AssetEvent).order_by(AssetEvent.id.desc()).limit(1):
            print("event:", ev.asset.name, ev.extra, "from", ev.source_dag_id, ev.source_task_id)
```
```text
consumer timetable: AssetTriggeredTimetable
condition: AssetAny(AssetAll(silver_orders, silver_customers), fx_rates)
normalised uri: s3://lake/silver/orders
producer run: success
event: silver_orders {'rows': 1834, 'partition': '2026-05-20T10'} from load_orders write_orders
```
The DB query at the end only inspects the result in this local test. Task code in Airflow 3 cannot query the metadata DB (see Q557).

**Real-World Use Case:**
A company replaced a 6 a.m. cron on its revenue mart, which ran whether or not the orders load had finished, with `schedule=(orders & customers) | fx_rates`. The mart now built minutes after both loads landed. The extra rebuild when FX rates were corrected happened automatically, and the empty-dashboard incidents on late-load days stopped.

**Common Mistakes / Interviewer Red Flags:**
- Thinking an asset event proves the data is correct, or that Airflow watches the storage path. It only records that the producer task succeeded.
- Expecting one consumer run per producer event. Events are batched until the consumer runs.
- Using different URIs for the same data in different DAGs, such as with and without a trailing slash, or `s3a://` versus `s3://`.

**Follow-up Questions Interviewers Ask:**
- Can an external system emit an event? — Yes, through the REST API's asset-events endpoint, or event-driven with an `AssetWatcher` (see Q540).
- What is `AssetAlias`? — A placeholder whose concrete asset is chosen at run time, for example when the output path depends on the data (see Q539).

---

### Q539. What do the `@asset` decorator, `Metadata` and `AssetAlias` add on top of plain asset outlets?
**ID:** AF-21
**Topic:** Airflow > Assets
**Level:** Intermediate
**Frequency:** Occasional
**Tags:** `airflow`, `assets`, `asset-alias`, `lineage`

**Short Answer (2–3 lines):**
`@asset` is shorthand for a DAG with one task that produces one asset, all named after the function. A parameter named after another asset makes that asset an inlet. Yielding `Metadata` attaches extra data to the event the task emits. `AssetAlias` lets a task decide *at run time* which concrete assets it produced, and downstream DAGs can schedule on the alias.

**Detailed Explanation:**
| Feature | Solves | Notes |
|---|---|---|
| `@asset(uri=..., schedule=...)` | Boilerplate for asset-centric pipelines | Creates an `Asset`, a DAG and a task, all with the function's name |
| Inlet parameters, e.g. `def summary(self, context, raw_orders)` | Reading upstream events | `self`, `context` and `outlet_events` are reserved parameter names |
| `yield Metadata(asset, {...})` | Attaching row counts or ranges to the event | Must be JSON-serialisable; it is stored in the DB |
| `context["inlet_events"][asset][-1]` | Reading past events of an inlet | Lazy; ordered oldest to newest |
| `@asset.multi(outlets=[a, b])` | One function producing several assets | |
| `AssetAlias("name")` | Outputs known only at run time, such as one asset per country | The task adds concrete assets with `outlet_events[alias].add(Asset(...), extra=...)` |
| `Asset.ref(name=...)` | Referring to an asset defined in another file | Avoids duplicate definitions |

When to use which:
- Use `@asset` when the team thinks in data products ("the daily_summary table") and each product is one step. Use normal DAGs when a pipeline has many steps but publishes one result.
- Use `AssetAlias` when the set of outputs depends on the data. Defining a fixed asset for each possible output would be impractical.
- Asset events are lineage records too. The UI's asset graph shows which DAG produced and consumed what, so use names people recognise.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from airflow.sdk import Asset, AssetAlias, Metadata, asset, dag, task


# @asset = one DAG + one task + one Asset, all named after the function.
@asset(uri="s3://lake/raw/orders", schedule="@daily")
def raw_orders(self):
    rows = 4210                                   # ... write the file ...
    yield Metadata(self, {"row_count": rows})     # becomes the asset event's extra


# Naming a parameter after another asset makes it an inlet; schedule on it to run after it.
@asset(uri="s3://lake/gold/daily_summary", schedule=raw_orders)
def daily_summary(self, context, raw_orders):
    last = context["inlet_events"][raw_orders][-1]   # most recent upstream event
    print("upstream rows:", last.extra["row_count"])


# AssetAlias: the concrete asset (here, which country partition) is only known at run time.
@dag(schedule=None)
def export_by_country():
    @task(outlets=[AssetAlias("country_exports")])
    def export(*, outlet_events):
        for country in ("de", "fr"):
            outlet_events[AssetAlias("country_exports")].add(
                Asset(name=f"export_{country}", uri=f"s3://exports/{country}/"), extra={"country": country})

    export()

export_by_country()


@dag(schedule=AssetAlias("country_exports"))     # runs when any asset resolved by the alias updates
def notify_partners():
    @task
    def notify(*, triggering_asset_events):
        print(sorted(a.name for a in triggering_asset_events))

    notify()

notify_partners()
```
```text
# DagBag + dag.test() of raw_orders, daily_summary and export_by_country:
daily_summary      tasks=['daily_summary'] timetable=AssetTriggeredTimetable outlets=['daily_summary']
export_by_country  tasks=['export'] timetable=NullTimetable outlets=['country_exports']
notify_partners    tasks=['notify'] timetable=AssetTriggeredTimetable outlets=[]
raw_orders         tasks=['raw_orders'] timetable=CronTriggerTimetable outlets=['raw_orders']
raw_orders run: success
upstream rows: 4210
daily_summary run: success
export run: success
event: export_fr {'country': 'fr'}
event: export_de {'country': 'de'}
```

**Real-World Use Case:**
A data-products team declared each gold table as an `@asset`. They yielded the row count and the max event time as `Metadata`. A freshness dashboard read these from the asset events API, so it could report "last updated, rows" for every product without scanning the warehouse.

**Common Mistakes / Interviewer Red Flags:**
- Defining the same asset with slightly different names or URIs in two files. Use `Asset.ref` or a shared module.
- Putting large payloads in event `extra`. Like XCom, it belongs in the metadata DB only when small.
- Assuming `@asset` works for multi-step pipelines. It creates exactly one task; use `@task` with `outlets` in a normal DAG instead.

**Follow-up Questions Interviewers Ask:**
- How do classic operators attach metadata? — Through `context["outlet_events"][asset].extra` inside `execute`, or by subclassing the operator.
- Are asset events emitted on failure? — No. Only a successful task emits its outlets' events.

---

### Q540. How do `AssetWatcher` and event-driven scheduling work?
**ID:** AF-22
**Topic:** Airflow > Event-Driven Scheduling
**Level:** Advanced
**Frequency:** Common
**Tags:** `airflow`, `asset-watcher`, `event-driven`, `triggerer`

**Short Answer (2–3 lines):**
An `AssetWatcher` attaches a trigger to an asset. The trigger runs in the **triggerer** and listens to an outside source, such as SQS, Kafka, Redis pub/sub or a custom poller. Each time it fires, Airflow records an asset event, and DAGs scheduled on that asset run. Only triggers that subclass `BaseEventTrigger` are allowed, and their payload reaches tasks through `triggering_asset_events`.

**Detailed Explanation:**
How the pieces fit together (added in 3.0):
1. Define `Asset(name=..., watchers=[AssetWatcher(name=..., trigger=SomeEventTrigger(...))])`.
2. The DAG processor registers the watcher, and a triggerer starts running the trigger's async `run()`.
3. When the trigger yields a `TriggerEvent`, the triggerer creates an asset event whose `extra` contains the event payload. For the messaging triggers that means the message, under `payload`.
4. The trigger is then started again to wait for the next event, and every DAG scheduled on the asset is queued.

Ready-made: `MessageQueueTrigger(scheme="sqs" | "kafka" | "redis+pubsub", ...)` from the `common.messaging` provider, backed by the AWS, Kafka and Redis providers.

Rules for a custom event trigger:
- Subclass **`BaseEventTrigger`**. Ordinary deferral triggers are not accepted, because many of them fire again immediately when restarted, which would trigger endless runs.
- It must be **idempotent across restarts**. Consume, acknowledge or move what it saw, as the example does by moving each file. Otherwise the same file fires again after the restart.
- It must be fully async. A blocking call stalls every trigger on that triggerer (see Q543).
- `serialize()` returns an importable classpath, and the module must be on the triggerer's path. Changing it needs a triggerer restart.
- Since 3.3, siblings that poll the same source can share one poll loop via `shared_stream_key()`.

Delivery is about as reliable as the source and the trigger make it. Keep consumers **idempotent**, and use the payload, such as a message id or file name, as the deduplication key.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3 — plugins/inbox_trigger.py (must be importable by the triggerer)
import asyncio
import os
from airflow.triggers.base import BaseEventTrigger, TriggerEvent


class InboxFileTrigger(BaseEventTrigger):
    """Fires once per file dropped into an inbox directory, then moves the file away
    so the same file can never fire twice (the trigger is restarted after each event)."""

    def __init__(self, inbox: str, done_dir: str, poll_seconds: float = 5.0):
        super().__init__()
        self.inbox, self.done_dir, self.poll_seconds = inbox, done_dir, poll_seconds

    def serialize(self):
        return ("inbox_trigger.InboxFileTrigger",
                {"inbox": self.inbox, "done_dir": self.done_dir, "poll_seconds": self.poll_seconds})

    async def run(self):
        while True:
            for name in sorted(os.listdir(self.inbox)):
                os.makedirs(self.done_dir, exist_ok=True)
                os.replace(os.path.join(self.inbox, name), os.path.join(self.done_dir, name))
                yield TriggerEvent({"file": name})
                return
            await asyncio.sleep(self.poll_seconds)
```
```python
# Runtime: Python 3.11, Apache Airflow 3.3 — dags/partner_drop.py
from airflow.sdk import Asset, AssetWatcher, dag, task
from inbox_trigger import InboxFileTrigger

partner_drop = Asset(
    name="partner_drop",
    uri="file:///srv/inbox/partner_a",
    watchers=[AssetWatcher(name="partner_a_inbox",
                           trigger=InboxFileTrigger("/srv/inbox/partner_a", "/srv/done/partner_a"))],
)


@dag(schedule=[partner_drop])            # no cron: one run per event (batched if they pile up)
def ingest_partner_drop():
    @task
    def ingest(*, triggering_asset_events):
        for event in triggering_asset_events[partner_drop]:
            print("ingesting", event.extra)

    ingest()


ingest_partner_drop()
```
```text
# Parsed with DagBag, then the trigger's run() driven with asyncio as the triggerer would:
errors: {}
watchers: [('partner_a_inbox', 'InboxFileTrigger')]
event: {'file': 'orders_0520.csv'} | inbox now: []
```
`os.listdir` is a quick local call. On a network filesystem, use an async client or run it in a thread with `asyncio.to_thread`.

**Real-World Use Case:**
An order-events team replaced a sensor DAG that ran every five minutes and mostly found nothing. They added an SQS-backed `MessageQueueTrigger` on an `orders_events` asset. Runs started within seconds of a message arriving, and the idle runs disappeared. The consumer used the message id from the payload as its deduplication key.

**Common Mistakes / Interviewer Red Flags:**
- Using a normal `BaseTrigger` (for example a file sensor's trigger) in a watcher.
- A trigger that doesn't consume or acknowledge what it saw, so it fires again forever.
- Treating event-driven runs as exactly-once.

**Follow-up Questions Interviewers Ask:**
- What happens if the triggerer is down? — Nothing fires until it is back. The trigger resumes on another triggerer, but events the source didn't keep may be lost, depending on the source.
- How is this different from a deferrable sensor? — A sensor waits inside a DAG run that already exists. A watcher *creates* runs (see Q543).

---

### Q541. What is asset partitioning in Airflow 3.2+, and when would you use it?
**ID:** AF-23
**Topic:** Airflow > Asset Partitioning
**Level:** Advanced
**Frequency:** Occasional
**Tags:** `airflow`, `asset-partitions`, `timetables`, `airflow-3`

**Short Answer (2–3 lines):**
Asset partitioning gives each asset event a `partition_key`, such as an hour, a day or a region, so that downstream runs are matched to the exact partitions they need. A producer uses `CronPartitionTimetable`, or assigns keys at run time with `PartitionedAtRuntime`. A consumer uses `PartitionedAssetTimetable` with partition mappers. Since 3.3, `RollupMapper` can hold a daily run until its hourly partitions arrive.

**Detailed Explanation:**
Plain asset scheduling only knows that something was updated. Partitioning adds *which slice* was updated.

| Piece | Role |
|---|---|
| `CronPartitionTimetable("0 * * * *")` | Producer: each scheduled run gets a key for its tick, and its events carry it |
| `PartitionedAtRuntime()` (3.3) | Producer: the task calls `outlet_events[asset].add_partitions([...])` once it knows the keys |
| `PartitionedAssetTimetable(assets=..., default_partition_mapper=...)` | Consumer: one run per downstream key |
| `IdentityMapper`, `StartOfHourMapper`, `StartOfDayMapper`, …, `ProductMapper`, `AllowedKeyMapper` | Translate upstream keys into downstream keys |
| `RollupMapper(upstream_mapper, window=DayWindow(), wait_policy=...)` (3.3) | Many to one: wait for every hour in a day |
| `WaitForAll()` / `MinimumCount(n)` | Release a rollup when all keys, or at least `n`, have arrived |
| `FanOutMapper`, `SegmentWindow` + `FixedKeyMapper` (3.3) | One to many, and categorical rollups such as all regions |

Inside the run, read `dag_run.partition_key`. When the mapper is temporal, `dag_run.partition_date` is set too. Backfills of partitioned DAGs read `--from-date/--to-date` as a partition-date range (see Q536). Partitioned events don't trigger DAGs that aren't partition-aware, and a consumer on `PartitionedAssetTimetable` ignores events with no key.

Points to raise in an interview:
- It is new and still changing quickly between minor versions. Pin the version and read the release notes before relying on mappers. *Product behaviour changes; checked October 2026.*
- A `DayWindow` assumes 24 hourly steps. Use UTC keys for rollups that cross a daylight-saving change, otherwise a day can wait forever for an hour that never exists.
- `[scheduler] partition_mapper_max_downstream_keys` bounds how far one event can fan out.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3 (asset partitioning: 3.2+, rollups: 3.3+)
from airflow.sdk import (DAG, Asset, CronPartitionTimetable, DayWindow, MinimumCount,
                         PartitionedAssetTimetable, RollupMapper, StartOfHourMapper, task)

hourly_clicks = Asset(name="hourly_clicks", uri="s3://lake/bronze/clicks")

# Producer: each run carries a partition key for its hour and stamps it on the asset event.
with DAG("ingest_hourly_clicks",
         schedule=CronPartitionTimetable("0 * * * *", timezone="UTC"), catchup=False) as producer:
    @task(outlets=[hourly_clicks])
    def ingest(dag_run=None):
        print("ingesting partition", dag_run.partition_key)

    ingest()

# Consumer: one run per UTC day, released when at least 23 of the 24 hours have landed.
with DAG("daily_click_summary",
         schedule=PartitionedAssetTimetable(
             assets=hourly_clicks,
             default_partition_mapper=RollupMapper(
                 upstream_mapper=StartOfHourMapper(),
                 window=DayWindow(),
                 wait_policy=MinimumCount(23),     # tolerate one late hour
             ),
         ),
         catchup=False) as consumer:
    @task
    def summarise(dag_run=None):
        print("summarising day", dag_run.partition_key)   # e.g. "2026-05-20"

    summarise()

if __name__ == "__main__":
    print("consumer timetable:", type(consumer.timetable).__name__)
    # How the scheduler computes the producer's next run (core-side class, used here only to show it):
    import pendulum
    from airflow.timetables.base import TimeRestriction
    from airflow.timetables.trigger import CronPartitionTimetable as CoreCronPartition

    info = CoreCronPartition("0 * * * *", timezone="UTC").next_dagrun_info_v2(
        last_dagrun_info=None,
        restriction=TimeRestriction(earliest=pendulum.datetime(2026, 5, 20, 9, 30, tz="UTC"),
                                    latest=None, catchup=True))
    print("next producer run:", info.run_after, "| partition_key:", info.partition_key)
```
```text
consumer timetable: PartitionedAssetTimetable
next producer run: 2026-05-20 10:00:00+00:00 | partition_key: 2026-05-20T10:00:00
```

**Real-World Use Case:**
A clickstream platform ingested hourly files, some of which arrived hours late. Before partitioning, the daily summary ran on a fixed cron and sometimes missed late hours. The fix was a scheduled re-run the next day. With a `RollupMapper` on `DayWindow`, each day's summary ran once its hours were in. Run history was now one run per day, which made reprocessing a single day simple.

**Common Mistakes / Interviewer Red Flags:**
- Calling this "Airflow 3.0 functionality". Partitioning arrived in 3.2, and rollups and runtime partitions in 3.3.
- Expecting a partitioned producer to trigger an ordinary asset-scheduled DAG.
- Building hourly-to-daily rollups on local-time keys across daylight-saving changes.

**Follow-up Questions Interviewers Ask:**
- How do you rerun one partition? — Trigger the consumer with that `partition_key` from the UI or the REST API, or clear its run.
- How do you see what a rollup is waiting for? — Partial windows show as pending on the DAG's next-run assets view.

---

### Q542. TriggerDagRunOperator, ExternalTaskSensor or assets: how do you choose for cross-DAG dependencies?
**ID:** AF-24
**Topic:** Airflow > Cross-DAG Dependencies
**Level:** Beginner
**Frequency:** Very Common
**Tags:** `airflow`, `cross-dag`, `trigger-dagrun`, `external-task-sensor`

**Short Answer (2–3 lines):**
Use assets when "run after that data is updated" is enough. The producer doesn't need to know its consumers. Use `TriggerDagRunOperator` when the producer must start a particular DAG and pass it conf. Use `ExternalTaskSensor` only when two DAGs keep their own schedules and one must wait for a specific task in the other, and make the schedules line up.

**Detailed Explanation:**
| Need | Best fit | Watch out for |
|---|---|---|
| Many consumers react to one dataset | Assets (see Q538) | Events mean "task succeeded", not "data valid" |
| Producer passes run parameters | `TriggerDagRunOperator(conf=...)` | Producer and target are tightly coupled; target validates params |
| Wait for the other DAG to finish too | `TriggerDagRunOperator(wait_for_completion=True, deferrable=True)` | Without `deferrable`, it holds a worker slot while it waits |
| Two scheduled DAGs, one task depends on another's | `ExternalTaskSensor` | Logical dates must match; use `execution_delta` or `execution_date_fn` |
| Clear downstream when upstream is cleared | `ExternalTaskMarker` | Needs a matching `ExternalTaskSensor` |

`ExternalTaskSensor` looks for the *same logical date* in the other DAG unless you shift it. A 03:00 DAG waiting on a 02:00 DAG needs `execution_delta=timedelta(hours=1)`. If you get this wrong, the sensor waits until it times out. Run it with `mode="reschedule"` or `deferrable=True`, and always set `timeout` (see Q544). In Airflow 3, logical dates can be `None` for manual and asset-triggered runs (see Q537), which makes date-matching sensors even more fragile. That is one more reason to prefer assets.

In Airflow 3, these operators live in `apache-airflow-providers-standard`. A run created by `TriggerDagRunOperator` shows up with its own run type, so you can tell it apart from manual runs.

For a longer walk-through, see the site's guide on making one Airflow DAG depend on another.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3 (apache-airflow-providers-standard)
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor


@dag(schedule="0 2 * * *", start_date=datetime(2026, 1, 1), catchup=False)
def load_sales():
    @task
    def load() -> int:
        return 1834

    # Push: the producer names its downstream and hands over parameters.
    load() >> TriggerDagRunOperator(
        task_id="trigger_finance_report",
        trigger_dag_id="finance_report",
        conf={"source": "load_sales", "rows": "{{ ti.xcom_pull(task_ids='load') }}"},
        wait_for_completion=False,
    )


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def finance_report():
    @task
    def build(params=None, dag_run=None):
        print("triggered with conf:", dag_run.conf)

    build()


# Pull: a DAG on its own schedule waits for a task in another DAG.
@dag(schedule="0 3 * * *", start_date=datetime(2026, 1, 1), catchup=False)
def marketing_mart():
    ExternalTaskSensor(
        task_id="wait_for_sales_load",
        external_dag_id="load_sales",
        external_task_id="load",
        execution_delta=timedelta(hours=1),     # 03:00 run looks for the 02:00 run
        mode="reschedule", poke_interval=300, timeout=3 * 3600,
    )


producer, consumer, sensor_dag = load_sales(), finance_report(), marketing_mart()

if __name__ == "__main__":
    print("producer:", producer.test().state)
    from airflow.models.dagrun import DagRun
    for dr in DagRun.find(dag_id="finance_report"):
        print("finance_report run:", dr.run_type, dr.state, dr.conf)
```
```text
producer: success
finance_report run: operator_triggered queued {'source': 'load_sales', 'rows': '1834'}
```
The triggered run waits in `queued` because no scheduler is running in this local test. `rows` arrives as a string because templates render to strings (see Q532).

**Real-World Use Case:**
A company had twelve report DAGs that each used an `ExternalTaskSensor` on one ingestion DAG, so every schedule change broke some sensors. They replaced the sensors with an asset on the ingestion output. The twelve reports now run when the data lands, and the ingestion team can change its schedule without telling anyone.

**Common Mistakes / Interviewer Red Flags:**
- An `ExternalTaskSensor` between DAGs on different schedules with no `execution_delta`.
- Chains of `TriggerDagRunOperator(wait_for_completion=True)` without deferrable mode, which tie up worker slots.
- Building a web of cross-DAG links that should have been one DAG or task groups.

**Follow-up Questions Interviewers Ask:**
- How do you see dependencies between DAGs? — The UI's DAG dependencies view, or `airflow dags show-dependencies`.
- Can `TriggerDagRunOperator` reuse a run id? — Yes, with `trigger_run_id`. `reset_dag_run=True` clears an existing run instead of failing.

---

### Q543. How does the triggerer work, and how would you write a deferrable operator?
**ID:** AF-25
**Topic:** Airflow > Deferrable Operators
**Level:** Advanced
**Frequency:** Common
**Tags:** `airflow`, `deferrable`, `triggerer`, `asyncio`

**Short Answer (2–3 lines):**
A deferrable operator starts the work, then calls `self.defer(trigger=..., method_name=...)`. The task moves to `deferred` and gives up its worker slot. The triggerer is an asyncio process that runs thousands of triggers in one event loop. When a trigger yields an event, the task is scheduled again and continues in `method_name` with the event payload.

**Detailed Explanation:**
Lifecycle:
1. `execute()` runs on a worker and submits the job, which is quick.
2. `self.defer(...)` raises a special exception. The trigger is serialised (classpath and kwargs) and stored, and the task becomes `deferred`. It holds no worker slot, and by default no pool slot either.
3. A triggerer picks the trigger up and runs `async def run()`. Each triggerer handles up to `[triggerer] capacity` triggers, 1000 by default. Run two or more triggerers for high availability; they split the triggers between them.
4. The trigger yields a `TriggerEvent(payload)`. The task goes back to `scheduled`, a worker starts it again, and it calls `execute_complete(context, event)`. That method is a fresh call, so any state must come from the event or from `kwargs` passed to `defer`.

Design rules:
- **Never block in `run()`.** Use async clients, `asyncio.sleep` and `asyncio.to_thread` for unavoidable sync calls. One blocking call stalls every trigger in that process. Set `PYTHONASYNCIODEBUG=1` to find them.
- **Triggers must be importable by the triggerer** and must not depend on worker-only state.
- **Timeouts.** `defer(timeout=...)` bounds the wait. `execution_timeout` covers the *whole* task, including the deferred time.
- **`start_from_trigger=True`** lets suitable operators go straight to the triggerer without starting on a worker at all.
- Many provider operators support `deferrable=True`. `[operators] default_deferrable = True` turns it on everywhere it is supported.

Compared with sensors in reschedule mode (see Q44, Q544): reschedule releases the slot between pokes, but every poke is still a full task start. Deferral is cheaper when there are many long waits.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3 — job_trigger.py, importable by workers and the triggerer
import asyncio
from airflow.triggers.base import BaseTrigger, TriggerEvent


class JobDoneTrigger(BaseTrigger):
    """Polls an external job asynchronously inside the triggerer's event loop."""

    def __init__(self, job_id: str, poll_seconds: float = 30.0):
        super().__init__()
        self.job_id, self.poll_seconds = job_id, poll_seconds

    def serialize(self):
        # classpath + kwargs: stored in the DB so any triggerer can rebuild this object
        return ("job_trigger.JobDoneTrigger", {"job_id": self.job_id, "poll_seconds": self.poll_seconds})

    async def run(self):
        polls = 0
        while True:
            polls += 1
            status = "SUCCEEDED" if polls >= 3 else "RUNNING"   # stand-in for an async API call
            if status in ("SUCCEEDED", "FAILED"):
                yield TriggerEvent({"job_id": self.job_id, "status": status, "polls": polls})
                return
            await asyncio.sleep(self.poll_seconds)              # never time.sleep() here
```
```python
# Runtime: Python 3.11, Apache Airflow 3.3 — the DAG file
from datetime import datetime, timedelta
from airflow.sdk import BaseOperator, dag
from airflow.sdk.exceptions import AirflowException
from job_trigger import JobDoneTrigger


class RunJobOperator(BaseOperator):
    def __init__(self, job_name: str, poll_seconds: float = 30.0, **kwargs):
        super().__init__(**kwargs)
        self.job_name, self.poll_seconds = job_name, poll_seconds

    def execute(self, context):
        job_id = f"{self.job_name}-0001"          # submit the job (fast, synchronous)
        print("submitted", job_id, "- deferring")
        # Frees the worker slot; the task goes to state "deferred" until the trigger fires.
        self.defer(trigger=JobDoneTrigger(job_id, self.poll_seconds),
                   method_name="execute_complete", timeout=timedelta(hours=2))

    def execute_complete(self, context, event: dict):
        # A fresh worker process resumes here with the trigger's payload.
        if event["status"] != "SUCCEEDED":
            raise AirflowException(f"job {event['job_id']} ended {event['status']}")
        print(f"job {event['job_id']} finished after {event['polls']} polls")
        return event["job_id"]


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def deferrable_demo():
    RunJobOperator(task_id="run_job", job_name="nightly_spark", poll_seconds=0.5)


dag_obj = deferrable_demo()

if __name__ == "__main__":
    print(dag_obj.test().state)
```
```text
submitted nightly_spark-0001 - deferring
job nightly_spark-0001 finished after 3 polls
success
```
`dag.test()` runs the trigger in-process, so you can test deferral without a triggerer.

**Real-World Use Case:**
A platform submitted about 800 Databricks and EMR jobs a night, each lasting 20 to 90 minutes. With classic operators that wait synchronously, 800 worker slots sat idle while they waited. With deferrable operators, two small triggerer pods handled all the waits, and the worker pool shrank to the size of the actual compute work.

**Common Mistakes / Interviewer Red Flags:**
- `time.sleep()` or a synchronous HTTP client inside `run()`.
- Keeping state on `self` between `execute` and `execute_complete`. It is a new process.
- Not running a triggerer at all, so deferred tasks wait forever.

**Follow-up Questions Interviewers Ask:**
- Where do trigger logs go? — Into the task's log, interleaved with the triggerer's output for that trigger.
- What if a triggerer dies? — Its triggers are reassigned to a live triggerer, so `run()` should be safe to restart.

---

### Q544. Which sensor settings matter, and how do you write a sensor with `@task.sensor`?
**ID:** AF-26
**Topic:** Airflow > Sensors
**Level:** Intermediate
**Frequency:** Common
**Tags:** `airflow`, `sensors`, `poke-return-value`, `timeouts`

**Short Answer (2–3 lines):**
A sensor checks a condition repeatedly. The settings that matter are `poke_interval`, `timeout`, `mode` (`poke` or `reschedule`), `soft_fail` and `exponential_backoff`, plus `deferrable=True` where the sensor supports it. `@task.sensor` turns a function into a sensor. Returning `PokeReturnValue(is_done=..., xcom_value=...)` both ends the wait and hands a value to the next task.

**Detailed Explanation:**
This question is about *configuring* sensors. For poke versus reschedule versus deferrable, see Q44 and the site's guide on how sensors should wait.

| Setting | Meaning | Guidance |
|---|---|---|
| `poke_interval` | Seconds between checks | Minutes, not seconds, for slow sources; respect API rate limits |
| `timeout` | Total seconds before the sensor gives up | **Always set it.** The default is 7 days |
| `mode="poke"` | Holds a worker slot the whole time | Only for waits of a few minutes |
| `mode="reschedule"` | Frees the slot between checks; state `up_for_reschedule` | For waits of minutes to hours, when no deferrable version exists |
| `deferrable=True` | Hands the wait to the triggerer | Best for long or numerous waits (see Q543) |
| `soft_fail=True` | Timeout gives `skipped` instead of `failed` | When "nothing arrived" is an acceptable result |
| `exponential_backoff=True` | Interval grows between checks | Flaky or expensive sources |
| `max_wait` | Caps the backoff interval | Use with backoff |
| `silent_fail=True` | Errors other than a timeout count as "not yet" | Rarely a good idea |

Things to know:
- `timeout` counts total time since the first check, and retries don't reset it. In reschedule mode, `execution_timeout` doesn't limit the overall wait either; `timeout` does.
- A sensor that returns a value saves a second lookup. Pass the discovered path forward with `PokeReturnValue`.
- Sensors fill pools like any other task. Hundreds of `poke` sensors can deadlock a pool.
- If the producer is inside Airflow, an asset is usually better than any sensor (see Q538).

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import PokeReturnValue, dag, task

ATTEMPTS = {"n": 0}            # stand-in for "is the partner manifest there yet?"


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def sensor_demo():
    @task.sensor(
        poke_interval=1,            # seconds between checks (use minutes in production)
        timeout=30,                 # total seconds before giving up
        mode="poke",                # "reschedule" frees the worker slot between pokes
        exponential_backoff=False,
        soft_fail=False,            # True = mark skipped instead of failed on timeout
    )
    def wait_for_manifest() -> PokeReturnValue:
        ATTEMPTS["n"] += 1
        found = ATTEMPTS["n"] >= 3
        print(f"poke {ATTEMPTS['n']}: found={found}")
        # is_done ends the wait; xcom_value is passed downstream like a return value
        return PokeReturnValue(is_done=found, xcom_value="s3://partner/2026-05-20/manifest.json" if found else None)

    @task
    def load(manifest: str):
        print("loading from", manifest)

    load(wait_for_manifest())


dag_obj = sensor_demo()

if __name__ == "__main__":
    print(dag_obj.test().state)
```
```text
poke 1: found=False
poke 2: found=False
poke 3: found=True
loading from s3://partner/2026-05-20/manifest.json
success
```
The module-level counter only works because `dag.test()` runs every poke in one process. A real sensor must check outside state, because in reschedule mode each poke is a new process.

**Real-World Use Case:**
A bank's partner sent a manifest file some time between 01:00 and 05:00. The team used an S3 key sensor in deferrable mode with `timeout=5*3600` and `soft_fail=False`. When the partner didn't deliver, the sensor failed at 06:00 and paged the on-call engineer, instead of waiting quietly for the default seven days.

**Common Mistakes / Interviewer Red Flags:**
- Leaving `timeout` at its default, so a missing file holds a slot or a deferral for a week.
- A `poke_interval` of a few seconds against a rate-limited API.
- Keeping state in Python globals inside a reschedule-mode sensor.

**Follow-up Questions Interviewers Ask:**
- What does `up_for_reschedule` mean? — A reschedule-mode sensor between checks. It holds no slot.
- How do you make sensors deferrable by default? — Set `[operators] default_deferrable = True` for operators and sensors that support it.

---

### Q545. How do custom and object-storage XCom backends work?
**ID:** AF-27
**Topic:** Airflow > XCom
**Level:** Advanced
**Frequency:** Occasional
**Tags:** `airflow`, `xcom`, `object-storage`, `custom-backend`

**Short Answer (2–3 lines):**
An XCom backend controls how XCom values are serialised and where they are stored. The `common.io` provider ships `XComObjectStorageBackend`. Values over a size threshold go to S3, GCS, Azure or a file path, and the database keeps only a reference. Reads resolve the reference without the DAG code changing. A custom backend subclasses `BaseXCom` and overrides `serialize_value`, `deserialize_value` and `purge`.

**Detailed Explanation:**
Q43 explains why the default backend should only hold small values. A backend changes that without touching DAG code.

Configuration for the built-in object-storage backend:
| Setting | Example |
|---|---|
| `[core] xcom_backend` | `airflow.providers.common.io.xcom.backend.XComObjectStorageBackend` |
| `[common.io] xcom_objectstorage_path` | `s3://aws_default@my-bucket/xcom` |
| `[common.io] xcom_objectstorage_threshold` | Bytes. `-1` = always DB (the default), `0` = always object storage |
| `[common.io] xcom_objectstorage_compression` | `gzip`, `bz2`, `zip`, `lzma`, `snappy` |

The environment-variable form replaces the section's dot with an underscore: `AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH`.

Things to design for:
- **Where it runs.** In Airflow 3 serialisation happens in the worker's Task SDK. Workers need credentials for the bucket, and so does the API server if the UI is to show values.
- **Lifecycle.** Objects outlive their DB rows unless `purge` runs, for example when task instances are cleared. Add a bucket lifecycle rule as a safety net.
- **Security.** Bucket permissions now control who can read task outputs.
- **What it doesn't fix.** Moving 2 GB between tasks is still slow. For real data, pass paths to tables or files you write yourself (see the site's guide on XCom vs external storage).
- **A custom backend** fits special encodings, such as storing DataFrames as Parquet, or an internal blob store. Keep `serialize_value` deterministic and its errors clear, because every task depends on it.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3, apache-airflow-providers-common-io
# Config (env vars here; airflow.cfg or Helm values in production):
#   AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.common.io.xcom.backend.XComObjectStorageBackend
#   AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH=file:///srv/xcom   (s3://aws_conn@bucket/xcom in prod)
#   AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD=1024           (bytes; smaller values stay in the DB)
#   AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_COMPRESSION=gzip
from datetime import datetime
from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def xcom_backend_demo():
    @task
    def small() -> int:
        return 42                                        # under threshold -> metadata DB

    @task
    def big() -> list[dict]:
        return [{"id": i, "sku": f"SKU-{i:05d}"} for i in range(2000)]   # over threshold -> object storage

    @task
    def use(n: int, rows: list[dict]):
        print("small:", n, "| big rows:", len(rows), rows[-1])   # transparently read back

    use(small(), big())


dag_obj = xcom_backend_demo()

if __name__ == "__main__":
    print(dag_obj.test().state)
```
```text
small: 42 | big rows: 2000 {'id': 1999, 'sku': 'SKU-01999'}
success
# object written:   <store>/xcom_backend_demo/manual__2026-10-17T18:43:24.878067+00:00/big/1828eda6-....gz
# metadata DB rows: small -> 42
#                   big   -> file:///<store>/xcom_backend_demo/manual__.../big/1828eda6-....gz
```

**Real-World Use Case:**
A team's tasks returned API pages of a few hundred KB, and the `xcom` table grew to tens of GB, which slowed DB backups and upgrades. They switched to the object-storage backend with a 64 KB threshold, gzip compression and a 30-day lifecycle rule on the bucket. The table shrank to a few MB, and no DAG code changed.

**Common Mistakes / Interviewer Red Flags:**
- Treating an object-storage backend as permission to pass big datasets through XCom.
- Forgetting worker credentials for the bucket. The tasks then fail when they push, not at parse time.
- Writing `AIRFLOW__COMMON.IO__...` as an environment variable. Shells won't export the dot, and Airflow reads `COMMON_IO`.

**Follow-up Questions Interviewers Ask:**
- How do you implement a custom backend? — Subclass `airflow.sdk.BaseXCom`, override `serialize_value` and `deserialize_value` (and `purge`), and point `[core] xcom_backend` to it.
- Why did pandas 3 come up in the 3.3.1 notes? — It changed how DataFrame XComs are stored and read back. Check serialisation whenever you upgrade libraries.

---

### Q546. How do TaskFlow tasks pass values: return values, `multiple_outputs` and named XComs?
**ID:** AF-28
**Topic:** Airflow > XCom
**Level:** Beginner
**Frequency:** Common
**Tags:** `airflow`, `xcom`, `taskflow`, `multiple-outputs`

**Short Answer (2–3 lines):**
A TaskFlow return value becomes the `return_value` XCom, and passing it to another task wires the dependency. With `multiple_outputs=True`, or a `dict`/`TypedDict` return annotation, each key is also pushed as its own XCom, and `result["key"]` passes one key on. `ti.xcom_push` and `ti.xcom_pull` remain for named values and classic operators.

**Detailed Explanation:**
| Need | How |
|---|---|
| Pass the whole result | `b(a())` |
| Pass one field of a dict result | `meta = a(); b(meta["path"])`, with `multiple_outputs` on |
| A value under a custom key | `ti.xcom_push(key="published_path", value=...)` |
| Read from a classic operator | `op.output`, or `op.output["key"]` |
| Read in Jinja | `{{ ti.xcom_pull(task_ids='a', key='rows') }}` |
| Read one mapped instance | `ti.xcom_pull(task_ids="a", map_indexes=2)` |

Things to remember:
- `multiple_outputs` is inferred from a `dict` or `TypedDict` return annotation. Pass `multiple_outputs=False` to stop it.
- Values must be serialisable. Primitives, lists, dicts, dataclasses and a few library types work through Airflow's serde. Pickling is not the default.
- XComs belong to one DAG run and one try. Clearing a task deletes its XComs, so downstream tasks must not expect old values.
- Return **pointers** (paths, table names, counts, ids), not data (see Q43, Q545).
- `do_xcom_push=False` on classic operators stops them from pushing anything, for example a huge stdout from a Bash command.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def passing_data():
    @task(multiple_outputs=True)       # also inferred from a dict / TypedDict return annotation
    def extract() -> dict:
        # Return a pointer to the data, not the data itself
        return {"path": "s3://lake/raw/orders/2026-05-20.parquet", "rows": 1834}

    @task
    def validate(path: str, rows: int) -> str:
        if rows == 0:
            raise ValueError("empty extract")
        return path

    @task
    def publish(path: str, ti=None):
        ti.xcom_push(key="published_path", value=path)     # an extra, named XCom
        print("published", path)

    @task
    def audit(ti=None):
        # Pull by task id and key; TaskFlow wiring would be nicer, this shows the raw API
        print("audit saw:", ti.xcom_pull(task_ids="publish", key="published_path"),
              "| rows key:", ti.xcom_pull(task_ids="extract", key="rows"))

    meta = extract()
    publish(validate(meta["path"], meta["rows"])) >> audit()   # meta["path"] -> XComArg for one key


dag_obj = passing_data()

if __name__ == "__main__":
    print(dag_obj.test().state)
```
```text
published s3://lake/raw/orders/2026-05-20.parquet
audit saw: s3://lake/raw/orders/2026-05-20.parquet | rows key: 1834
success
```

**Real-World Use Case:**
An ingestion task returned `{"path": ..., "rows": ..., "max_ts": ...}`. A quality task used `rows`, the load task used `path`, and the watermark task used `max_ts`. Each took only the key it needed, so the graph showed exactly which value each step depended on.

**Common Mistakes / Interviewer Red Flags:**
- `xcom_pull` with no `task_ids`, or a misspelt key, which returns `None` without an error.
- Expecting XComs to survive a clear, or to be shared between DAG runs.
- Returning a DataFrame "because it worked in dev".

**Follow-up Questions Interviewers Ask:**
- Why not just use globals? — Every task runs in its own process, possibly on another machine. Only XCom, storage or the state store crosses that boundary.
- What is the 3.3 task state store for? — Key/value state that persists across retries and runs, such as a watermark. XComs, by contrast, belong to one run.

---

### Q547. What are pools, and how do you use them to protect a shared system?
**ID:** AF-29
**Topic:** Airflow > Pools
**Level:** Beginner
**Frequency:** Common
**Tags:** `airflow`, `pools`, `concurrency`, `resource-management`

**Short Answer (2–3 lines):**
A pool is a named number of slots shared by every DAG. A task that names the pool takes `pool_slots` slots, 1 by default, while it runs. When the pool is full, further tasks wait in `scheduled`. Pools cap load on a shared system, such as at most 4 concurrent warehouse rebuilds, whichever DAG starts them.

**Detailed Explanation:**
- **Default.** Tasks without a pool use `default_pool`, which has 128 slots (`[core] default_pool_task_slot_count`).
- **Create pools** with `airflow pools set NAME SLOTS "description"`, the REST API, the UI, or your infrastructure code. Pools are deployment state, not DAG code, so manage them like config.
- **Weighting.** `pool_slots=3` lets one heavy query count as three light ones.
- **Deferred tasks** don't use pool slots unless the pool has `include_deferred=True`.
- **Order in a full pool.** Higher `priority_weight` goes first (see Q548).
- **Mapping.** Every mapped instance takes slots, so a pool is the natural limit for a large fan-out against one system (see Q530).

Pools compared with other limits:
| Limit | Scope |
|---|---|
| Pool | Across all DAGs, per resource |
| `max_active_tis_per_dag` / `max_active_tis_per_dagrun` | One task (all of its copies) |
| `max_active_tasks` | One DAG across its runs |
| `parallelism` | The whole scheduler |

See Q549 for how they combine.

A task that names a pool which doesn't exist won't be scheduled. Create pools before deploying the DAGs that use them, and test that every referenced pool exists in CI (see Q562).

**Example / Code:**
```bash
# Runtime: Apache Airflow 3.3 CLI
airflow pools set warehouse_heavy 4 "Big warehouse queries" --include-deferred
airflow pools list
```
```text
Pool warehouse_heavy created
pool            | slots | description           | include_deferred | team_name
================+=======+=======================+==================+==========
default_pool    | 128   | Default pool          | False            | None
warehouse_heavy | 4     | Big warehouse queries | True             | None
```
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import dag, task


@dag(schedule="0 4 * * *", start_date=datetime(2026, 1, 1), catchup=False)
def nightly_marts():
    # Every mapped copy competes for the same 4 slots, across ALL DAGs using this pool.
    @task(pool="warehouse_heavy", pool_slots=1)
    def build_mart(name: str):
        print("building", name)

    # A very heavy statement can take several slots of the same pool.
    @task(pool="warehouse_heavy", pool_slots=3)
    def rebuild_fact_sales():
        print("rebuilding fact_sales")

    build_mart.expand(name=["finance", "marketing", "ops"]) >> rebuild_fact_sales()


dag_obj = nightly_marts()

if __name__ == "__main__":
    run = dag_obj.test()
    print({(ti.task_id, ti.map_index): (ti.pool, ti.pool_slots) for ti in run.get_task_instances()})
    print(run.state)
```
```text
{('build_mart', 0): ('warehouse_heavy', 1), ('build_mart', 1): ('warehouse_heavy', 1), ('build_mart', 2): ('warehouse_heavy', 1), ('rebuild_fact_sales', -1): ('warehouse_heavy', 3)}
success
```

**Real-World Use Case:**
Twenty teams' DAGs all ran heavy SQL against one shared warehouse around 04:00, and queued queries regularly hit the warehouse's concurrency limit. A `warehouse_heavy` pool with 6 slots made Airflow queue the excess itself. Teams could see their tasks waiting for a slot in the UI, instead of getting cryptic warehouse timeouts.

**Common Mistakes / Interviewer Red Flags:**
- Using pools as a per-DAG throttle when `max_active_tasks` or `max_active_runs` is meant for that.
- Long poke-mode sensors in a small pool, which starve the real work.
- Referencing a pool in code that was never created in production.

**Follow-up Questions Interviewers Ask:**
- How do you see pool pressure? — The Pools page shows running, queued and scheduled slots. The `pool.open_slots` and `pool.starving_tasks` metrics, tagged by pool, expose it too.
- Can pools be team-scoped? — In 3.2+ multi-team deployments, pools can be assigned to a team.

---

### Q548. How do priority weights and `weight_rule` decide what runs first?
**ID:** AF-30
**Topic:** Airflow > Priority Weights
**Level:** Intermediate
**Frequency:** Occasional
**Tags:** `airflow`, `priority-weight`, `scheduling`, `pools`

**Short Answer (2–3 lines):**
When slots are scarce, the scheduler queues task instances with the highest *effective* priority weight first. `weight_rule` decides how the effective weight is computed. `downstream`, the default, adds the weights of every task downstream. `upstream` adds the weights of every task upstream. `absolute` uses the task's own `priority_weight` only.

**Detailed Explanation:**
For a three-task chain `extract >> clean >> load` where `load` has `priority_weight=10` and the others 1 (output below):

| `weight_rule` | extract | clean | load | Effect |
|---|---|---|---|---|
| `downstream` (default) | 12 | 11 | 10 | Starts many new pipelines (breadth first) |
| `upstream` | 1 | 2 | 12 | Finishes pipelines that are already running (depth first) |
| `absolute` | 1 | 1 | 10 | Only explicit weights count; cheapest to compute |

Points to make:
- Weights only matter when something is **full**: a pool, `parallelism` or executor slots. With spare capacity, everything ready runs.
- Weights are compared across DAGs. Use weights to put the CFO dashboard's DAG ahead of an exploratory backfill.
- `upstream` is good for long DAGs, so half-finished runs don't wait behind brand-new ones. That helps especially during catchup or backfills.
- `absolute` is cheap and predictable on very large DAGs.
- A custom `PriorityWeightStrategy`, registered through a plugin, can compute weights from task attributes.
- Weight doesn't pre-empt anything. A running low-priority task keeps its slot.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import DAG, WeightRule
from airflow.providers.standard.operators.empty import EmptyOperator

dags = {}
for rule in (WeightRule.DOWNSTREAM, WeightRule.UPSTREAM, WeightRule.ABSOLUTE):
    with DAG(f"weights_{rule.value}", schedule=None, start_date=datetime(2026, 1, 1),
             default_args={"weight_rule": rule}) as d:
        extract = EmptyOperator(task_id="extract")
        clean = EmptyOperator(task_id="clean")
        load = EmptyOperator(task_id="load", priority_weight=10)   # own weight 10, others 1
        extract >> clean >> load
    dags[rule.value] = d

if __name__ == "__main__":
    for name, d in dags.items():
        run = d.test()
        # the effective weight the scheduler stored on each task instance
        print(f"{name:10}", {ti.task_id: ti.priority_weight for ti in
                             sorted(run.get_task_instances(), key=lambda t: ["extract", "clean", "load"].index(t.task_id))})
```
```text
downstream {'extract': 12, 'clean': 11, 'load': 10}
upstream   {'extract': 1, 'clean': 2, 'load': 12}
absolute   {'extract': 1, 'clean': 1, 'load': 10}
```

**Real-World Use Case:**
During a 90-day backfill, the backfill's runs crowded out the daily production loads in a shared pool. The team gave production DAGs `priority_weight=100` with `weight_rule="absolute"` and left the backfill at 1. Production tasks got the next free slot every time, and the backfill used whatever capacity was left.

**Common Mistakes / Interviewer Red Flags:**
- Expecting priority to speed things up when there is spare capacity.
- Raising weights everywhere ("everything is priority 100"), which changes nothing.
- Not knowing the default rule adds up downstream weights, so long DAGs win over short ones.

**Follow-up Questions Interviewers Ask:**
- Where is weight applied — the scheduler or the executor? — The scheduler orders what it queues. Celery queues and Kubernetes scheduling add their own ordering after that.
- How do you give a whole DAG priority? — `default_args={"priority_weight": ..., "weight_rule": "absolute"}`.

---

### Q549. How do Airflow's concurrency limits stack, from `parallelism` down to a single task?
**ID:** AF-31
**Topic:** Airflow > Concurrency
**Level:** Advanced
**Frequency:** Common
**Tags:** `airflow`, `concurrency`, `max-active-runs`, `scaling`

**Short Answer (2–3 lines):**
A task instance only starts when *every* limit on it has room. The limits are: `[core] parallelism` per scheduler, the executor's or worker's slots, the pool, the DAG's `max_active_tasks` and `max_active_runs`, and the task's `max_active_tis_per_dag` and `max_active_tis_per_dagrun`. The tightest limit wins, so "tasks stuck in scheduled" usually means one of these is full.

**Detailed Explanation:**
| Level | Setting | Default (3.3) | Limits |
|---|---|---|---|
| Scheduler | `[core] parallelism` | 32 | Running + queued task instances per scheduler |
| Executor | Celery `worker_concurrency`, K8s quotas, Local process count | Celery 16 per worker | Physical slots |
| Pool | `slots` per pool | `default_pool` 128 | One resource, across all DAGs |
| DAG | `max_active_tasks` | `[core] max_active_tasks_per_dag` = 16 | Running tasks of one DAG across all its runs |
| DAG | `max_active_runs` | `[core] max_active_runs_per_dag` = 16 | Concurrent runs (catchup, backfill) |
| Task | `max_active_tis_per_dag` | None | Copies of one task across runs |
| Task | `max_active_tis_per_dagrun` | None | Copies of one task within a run (mapping) |
| Backfill | `--max-active-runs` | n/a | That backfill only (see Q536) |

Related settings:
- `max_consecutive_failed_dag_runs` pauses a DAG automatically after N failed runs in a row.
- `[scheduler] max_tis_per_query` controls how many task instances the scheduler examines per loop. It is a throughput knob, not a limit.
- With two or more schedulers, `parallelism` applies **per scheduler**, so the total capacity multiplies (see Q555).

How to diagnose: open a task stuck in `scheduled`. The UI's task-instance details, or `airflow tasks failed-deps <dag_id> <task_id> <run_id>`, list the unmet dependency, such as "pool full" or "max_active_tasks reached". Then raise the limit that is actually the bottleneck. Raising `parallelism` does nothing when the pool is full. See the site's guide on finding why tasks sit in scheduled or queued.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.configuration import conf
from airflow.sdk import dag, task


@dag(
    schedule="@hourly", start_date=datetime(2026, 1, 1), catchup=True,
    max_active_runs=3,                 # at most 3 runs of this DAG at once (catchup can't flood)
    max_active_tasks=8,                # at most 8 running task instances across those runs
    max_consecutive_failed_dag_runs=5, # auto-pause after 5 failed runs in a row
)
def throttled_ingest():
    @task(max_active_tis_per_dagrun=2,     # per run: 2 copies of this task at once
          max_active_tis_per_dag=4,        # across all runs: 4 copies at once
          pool="partner_api")              # across ALL DAGs: pool slots
    def fetch_page(page: int):
        print("page", page)

    fetch_page.expand(page=list(range(20)))


d = throttled_ingest()

if __name__ == "__main__":
    t = d.get_task("fetch_page")
    print("[core] parallelism:", conf.getint("core", "parallelism"),
          "| max_active_tasks_per_dag default:", conf.getint("core", "max_active_tasks_per_dag"),
          "| max_active_runs_per_dag default:", conf.getint("core", "max_active_runs_per_dag"))
    print("DAG:", {"max_active_runs": d.max_active_runs, "max_active_tasks": d.max_active_tasks,
                   "max_consecutive_failed_dag_runs": d.max_consecutive_failed_dag_runs})
    print("task:", {"per_dagrun": t.max_active_tis_per_dagrun, "per_dag": t.max_active_tis_per_dag, "pool": t.pool})
```
```text
[core] parallelism: 32 | max_active_tasks_per_dag default: 16 | max_active_runs_per_dag default: 16
DAG: {'max_active_runs': 3, 'max_active_tasks': 8, 'max_consecutive_failed_dag_runs': 5}
task: {'per_dagrun': 2, 'per_dag': 4, 'pool': 'partner_api'}
```
For this DAG, at most 4 `fetch_page` copies run at once, and only if the `partner_api` pool and the scheduler's `parallelism` both have room.

**Real-World Use Case:**
A platform raised Celery workers from 4 to 12, but throughput didn't change. `failed-deps` showed "Task Instance Slots Available: parallelism reached". `parallelism` was still 32, while the 12 workers offered 192 slots. Raising it to 192, and setting per-DAG `max_active_tasks` so one DAG couldn't take everything, let the new workers do useful work.

**Common Mistakes / Interviewer Red Flags:**
- Turning on `catchup=True` for an hourly DAG with a year-old `start_date` and the default `max_active_runs` of 16.
- Using one huge pool as the only control and ignoring the DAG-level limits.
- Not knowing that `parallelism` is per scheduler.

**Follow-up Questions Interviewers Ask:**
- What happened to `concurrency` and `task_concurrency`? — They were renamed to `max_active_tasks` and `max_active_tis_per_dag`. The old names are gone in Airflow 3.
- How do you protect a downstream API from all DAGs? — With a pool (see Q547).

---

### Q550. How do you configure retries, backoff, timeouts and the 3.3 retry policies?
**ID:** AF-32
**Topic:** Airflow > Retries and Timeouts
**Level:** Intermediate
**Frequency:** Very Common
**Tags:** `airflow`, `retries`, `execution-timeout`, `retry-policy`

**Short Answer (2–3 lines):**
`retries` and `retry_delay` set how many times and how long to wait. `retry_exponential_backoff` grows the delay, with a numeric multiplier allowed since 3.2, and `max_retry_delay` caps it. `execution_timeout` kills a single hung try. Airflow 3.3 adds `retry_policy`, which retries, fails fast or falls back to the defaults depending on the exception. `AirflowFailException` always fails at once.

**Detailed Explanation:**
| Setting | Scope | Notes |
|---|---|---|
| `retries` | Task | Default `[core] default_task_retries` = 0. Set it in `default_args` |
| `retry_delay` | Task | Default 300 s |
| `retry_exponential_backoff` | Task | `True` or a multiplier such as `2.0` (3.2+); adds jitter |
| `max_retry_delay` | Task | Cap for the backoff |
| `execution_timeout` | **One try** | Kills the try and counts as a failure, which may be retried |
| `dagrun_timeout` | DAG run | Marks a run that runs too long as failed |
| `retry_policy` (3.3) | Task | `ExceptionRetryPolicy(rules=[RetryRule(...)])`, or subclass `RetryPolicy` |
| `AirflowFailException` | Raised in code | No retries, ever |

How retry policies work: rules are checked in order against the exception, using `isinstance` by default, and the first match wins. An action of `RETRY` can set its own delay but never goes beyond `retries`. `FAIL` stops now. `DEFAULT` falls back to the normal settings, and so does an exception no rule matches. The policy runs in the worker, and its decision is written to the task log. A subclass can inspect HTTP status codes, `Retry-After` headers or `dag_run.run_type`.

Rules of thumb:
- Retry *transient* errors such as timeouts, 429s, 5xx responses and lost connections. Fail fast on bad credentials, 4xx responses, schema errors and bad input.
- Retries are only safe if the task is idempotent (see Q46 and the site's guide on retries and idempotency).
- Always set `execution_timeout` on tasks that call networks. A hung socket otherwise holds a slot until the heartbeat timeout fires (see Q567).

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3 (retry policies are new in 3.3)
from datetime import datetime, timedelta
from airflow.sdk import ExceptionRetryPolicy, RetryAction, RetryRule, dag, task

CALLS = {"flaky": 0}

# First matching rule wins; anything unmatched falls back to retries/retry_delay.
API_POLICY = ExceptionRetryPolicy(rules=[
    RetryRule(exception=PermissionError, action=RetryAction.FAIL, reason="bad credentials, retrying won't help"),
    RetryRule(exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=1)),
])


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False,
     default_args={
         "retries": 3,
         "retry_delay": timedelta(seconds=1),
         "retry_exponential_backoff": 2.0,          # numeric multiplier since 3.2 (True = 2)
         "max_retry_delay": timedelta(minutes=30),
         "execution_timeout": timedelta(minutes=10),  # per try; kills a hung call
         "retry_policy": API_POLICY,
     })
def retry_demo():
    @task
    def flaky_api():
        CALLS["flaky"] += 1
        if CALLS["flaky"] < 3:
            raise ConnectionError(f"timeout on attempt {CALLS['flaky']}")
        print("succeeded on attempt", CALLS["flaky"])

    @task
    def bad_credentials():
        raise PermissionError("token rejected")

    flaky_api()
    bad_credentials()


dag_obj = retry_demo()

if __name__ == "__main__":
    run = dag_obj.test()
    for ti in sorted(run.get_task_instances(), key=lambda t: t.task_id):
        print(f"{ti.task_id:16} state={ti.state} try_number={ti.try_number} max_tries={ti.max_tries}")
```
```text
succeeded on attempt 3
bad_credentials  state=failed try_number=1 max_tries=3
flaky_api        state=success try_number=3 max_tries=3
```
`bad_credentials` had 3 retries available but failed on its first try, because of the policy's `FAIL` rule.

**Real-World Use Case:**
A marketing-API extractor had `retries=5`. When an expired token caused 401s, it retried for 40 minutes before failing, and the on-call engineer only heard about it after the dashboards were late. A retry policy that fails at once on auth errors, while still retrying 429s and 5xx with backoff, cut the alert time to about a minute.

**Common Mistakes / Interviewer Red Flags:**
- Treating `execution_timeout` as a whole-run timeout. It applies to each try, including time spent deferred.
- Retrying non-idempotent appends, which duplicates data on every retry.
- Very many retries with no backoff against a struggling dependency.

**Follow-up Questions Interviewers Ask:**
- Can a retry policy add tries? — No. It can fail earlier or change the delay, but `retries` stays the maximum.
- How do you retry a whole DAG? — Clear the failed run. For automatic DAG-level retries, the docs show a pattern based on a failure callback.

---

### Q551. SLAs were removed in Airflow 3. How do Deadline Alerts replace them?
**ID:** AF-33
**Topic:** Airflow > Deadline Alerts
**Level:** Intermediate
**Frequency:** Common
**Tags:** `airflow`, `deadline-alerts`, `sla`, `alerting`

**Short Answer (2–3 lines):**
The old `sla` and `sla_miss_callback` were removed in 3.0. Passing them now only gives a warning. Since 3.1, a DAG declares `deadline=DeadlineAlert(reference=..., interval=..., callback=...)`. When a run has not finished by reference + interval, the callback runs. Callbacks are `AsyncCallback` (run in the triggerer) or `SyncCallback` (run by the executor, since 3.2). The feature is still marked experimental.

**Detailed Explanation:**
Why SLAs went away: they were measured from odd anchors, they were evaluated by the DAG processor, and they misfired around catchup. Deadlines are explicit.

References (the point the clock starts from):
| Reference | Deadline = |
|---|---|
| `DAGRUN_LOGICAL_DATE` | Scheduled time + interval. "Done by 04:00 for the 01:00 run", even if the run never started |
| `DAGRUN_QUEUED_AT` | Queue time + interval. Catches capacity problems |
| `FIXED_DATETIME(dt)` | A wall-clock time; a negative interval alerts *before* it |
| `AVERAGE_RUNTIME(max_runs, min_runs)` | Now + historical average + interval. No deadline is set if there aren't enough runs |
| Custom reference | Any function that returns a timestamp |

Callbacks:
- `AsyncCallback(callable_or_notifier, kwargs=...)` runs in the **triggerer**. The callable must be importable there, for example from the plugins folder, and the triggerer needs a restart after you change it.
- `SyncCallback(...)` (3.2+) is sent to the executor and runs as a top-priority task.
- Templated kwargs such as `{{ dag_run.dag_id }}` and `{{ deadline.deadline_time }}` work with notifiers, and a `context` kwarg is passed in.
- Airflow 3.3 adds a Deadlines page under Browse.

How to migrate: map each old `sla=timedelta(...)` to a `DAGRUN_LOGICAL_DATE` deadline at the DAG level, and send it to the same channel. Remember that the old SLAs were per task, while deadlines are per DAG run. *Product behaviour changes; checked October 2026.*

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3 — plugins/deadline_callbacks.py
def page_finance_oncall(**kwargs):
    context = kwargs.get("context", {})
    dag_id = context.get("dag_run", {}).get("dag_id")
    print(f"[{kwargs['severity']}] {dag_id} missed its deadline")
```
```python
# Runtime: Python 3.11, Apache Airflow 3.3 (Deadline Alerts: 3.1+, experimental) — dags/finance_close.py
from datetime import datetime, timedelta
from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback, dag, task
from deadline_callbacks import page_finance_oncall


@dag(
    schedule="0 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # "Must be finished 3 hours after its scheduled time", whether or not it has started.
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=3),
        callback=SyncCallback(page_finance_oncall, kwargs={"severity": "P2"}),
    ),
)
def finance_close():
    @task
    def reconcile():
        print("reconciling ledgers")

    reconcile()


finance_close()
```
```text
# DagBag parse, then calling the callback the way Airflow does on a miss:
errors: {}
reference: DagRunLogicalDateDeadline | interval: 3:00:00 | callback: deadline_callbacks.page_finance_oncall
[P2] finance_close missed its deadline
```

**Real-World Use Case:**
A month-end close DAG had to finish by 04:00 for the controllers. In 2.x its SLA was measured per task from odd anchors and fired inconsistently. A `DAGRUN_LOGICAL_DATE` deadline of 3 hours paged the on-call engineer at 04:00 whenever the 01:00 run wasn't done. That included nights when a scheduler outage meant the run never started, which a duration check alone would have missed.

**Common Mistakes / Interviewer Red Flags:**
- Saying "set `sla=` on the task" for Airflow 3.
- Defining an `AsyncCallback` that the triggerer can't import.
- Using only `AVERAGE_RUNTIME` on a new DAG, which sets no deadline until enough runs exist.

**Follow-up Questions Interviewers Ask:**
- Can a DAG have several deadlines? — Yes. Pass a list of `DeadlineAlert`s, for example a warning at +2 h and a page at +3 h.
- Deadline or `dagrun_timeout`? — `dagrun_timeout` *fails* a run that takes too long. A deadline only *alerts*, and can fire before the run has even started.

---

### Q552. How do callbacks and notifiers work, and where do they run?
**ID:** AF-34
**Topic:** Airflow > Callbacks and Alerting
**Level:** Beginner
**Frequency:** Common
**Tags:** `airflow`, `callbacks`, `notifiers`, `alerting`

**Short Answer (2–3 lines):**
Tasks accept `on_execute_callback`, `on_success_callback`, `on_failure_callback`, `on_retry_callback` and `on_skipped_callback`. DAGs accept success and failure callbacks. Each takes a function, a list of functions, or a notifier: a `BaseNotifier` subclass with templated fields, such as the Slack, SMTP or PagerDuty notifiers from providers. Task callbacks run where the task ran. DAG-level callbacks are run by the DAG processor, so their errors show up in its logs.

**Detailed Explanation:**
| Callback | Fires when | Level |
|---|---|---|
| `on_execute_callback` | Just before the task executes | Task |
| `on_success_callback` | Task or run succeeded | Task, DAG |
| `on_failure_callback` | Task failed for good, or run failed | Task, DAG |
| `on_retry_callback` | Task failed and will retry | Task |
| `on_skipped_callback` | Task raised `AirflowSkipException` while running | Task |

Behaviour to know:
- Callbacks fire only for state changes caused by running tasks. Marking a task failed or successful in the UI or CLI doesn't trigger them.
- In 3.0+ `on_success_callback` no longer fires for tasks that end `skipped`.
- Put task callbacks in `default_args` to cover every task. Put a DAG-level `on_failure_callback` on the DAG for one alert per run instead of one per task.
- **Notifiers** are reusable classes with `template_fields` and `notify(context)`. Prefer provider notifiers, such as `SlackNotifier` or `SmtpNotifier`, over hand-written HTTP calls.
- A callback must be quick and must not fail. An exception in it is only logged, so the alert is lost.
- For "the run didn't finish by a certain time", use Deadline Alerts (see Q551). For alerting patterns overall, see the site's guide on failure handling and alerting.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime, timedelta
from airflow.sdk import BaseNotifier, dag, task


class ChatNotifier(BaseNotifier):
    """Reusable alert: template_fields are rendered with the task's context, like an operator."""
    template_fields = ("message",)

    def __init__(self, message: str, channel: str = "#data-alerts"):
        super().__init__()
        self.message, self.channel = message, channel

    def notify(self, context):
        # real version: post to Slack/Teams via a provider hook or notifier
        print(f"to {self.channel}: {self.message}")


def log_retry(context):                      # plain-function callbacks work too
    ti = context["ti"]
    print(f"retrying {ti.task_id}, try {ti.try_number}")


@dag(
    schedule=None, start_date=datetime(2026, 1, 1), catchup=False,
    on_failure_callback=ChatNotifier("DAG {{ dag.dag_id }} run {{ run_id }} FAILED"),
    default_args={
        "retries": 1, "retry_delay": timedelta(seconds=1),
        "on_retry_callback": log_retry,
        "on_failure_callback": ChatNotifier("task {{ ti.task_id }} failed after {{ ti.try_number }} tries"),
    },
)
def alerting_demo():
    @task
    def load():
        raise RuntimeError("warehouse unavailable")

    load()


dag_obj = alerting_demo()

if __name__ == "__main__":
    print(dag_obj.test().state)
```
```text
retrying load, try 1
to #data-alerts: task load failed after 2 tries
failed
```
In this local `dag.test()` run only the task-level callbacks printed. In a deployment, the DAG-level `on_failure_callback` is run by the DAG processor once the run fails.

**Real-World Use Case:**
A team with 300 tasks had a task-level `on_failure_callback` posting to Slack. A single warehouse outage produced 300 messages, and the channel got muted. They moved the alert to DAG level with a notifier that lists the failed task ids, and kept task-level alerts only for a few critical tasks. One incident became one message.

**Common Mistakes / Interviewer Red Flags:**
- Expecting callbacks to fire after marking a task failed in the UI.
- Doing heavy work, such as queries or retries, inside a callback.
- Hard-coding webhook URLs in DAG code instead of keeping them in a connection.

**Follow-up Questions Interviewers Ask:**
- Where do you find callback errors? — Task callbacks in the task log. DAG callbacks in the DAG processor logs.
- Can you attach several notifiers? — Yes. Pass a list, for example Slack and PagerDuty.

---

### Q553. How do multiple executors work, and how do you route a task to a specific one?
**ID:** AF-35
**Topic:** Airflow > Executors
**Level:** Intermediate
**Frequency:** Common
**Tags:** `airflow`, `executors`, `multiple-executors`, `routing`

**Short Answer (2–3 lines):**
Since 2.10 you can list several executors, for example `[core] executor = CeleryExecutor,KubernetesExecutor`. The first is the default. A task picks another with `executor="KubernetesExecutor"` or an alias, and a whole DAG can do it through `default_args`. Airflow 3 removed the hybrid CeleryKubernetes and LocalKubernetes executors in favour of this.

**Detailed Explanation:**
Q42 compares the executors themselves. This question is about running more than one.

Configuration:
- `executor = LocalExecutor,CeleryExecutor`, or with aliases: `executor = fast:CeleryExecutor,isolated:KubernetesExecutor`.
- Every component (schedulers, API server, workers, DAG processor) must have the **same** list. A task that names an executor not in the list makes the **DAG fail to parse** (output below).
- One class can't be listed twice, except across teams in multi-team mode (3.2+).

Routing:
| Scope | How |
|---|---|
| One task | `@task(executor="KubernetesExecutor")` or `Operator(executor=...)` |
| Whole DAG | `default_args={"executor": "..."}` |
| Per-executor settings | `executor_config={"pod_override": k8s.V1Pod(...)}` for Kubernetes; `queue=` for Celery |

Typical layouts:
- **Celery default + Kubernetes for the heavy work**: low latency for thousands of light tasks, isolation for big ones.
- **Local default + an ECS, Batch or Kubernetes executor**: a small single-machine deployment that can burst.
- **Celery + EdgeExecutor**: run some tasks on remote sites or in other networks (see Q554).

Each executor reports its own metrics, such as `executor.open_slots.<executor class name>`. Watch each one separately. An idle Kubernetes executor doesn't help when the Celery queue is full.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
# airflow.cfg (every component must have the same list):
#   [core] executor = CeleryExecutor,KubernetesExecutor     # first = default
from datetime import datetime
from airflow.sdk import dag, task


@dag(schedule="0 2 * * *", start_date=datetime(2026, 1, 1), catchup=False)
def mixed_executors():
    @task                                              # default executor (first in the list)
    def trigger_warehouse_job():
        print("submitting SQL")

    @task(executor="KubernetesExecutor",               # own pod, own resources
          executor_config={"pod_override": None})      # e.g. a k8s V1Pod with 16Gi memory
    def train_model():
        print("training")

    trigger_warehouse_job() >> train_model()


mixed_executors()
```
```text
# Parsed on a deployment configured with only "executor = LocalExecutor":
dags: []
import error: UnknownExecutorException: Task 'train_model' specifies executor 'KubernetesExecutor', which is not available. Make sure it is listed in your [core] executors co...
```

**Real-World Use Case:**
A company ran Celery for about 5,000 short "submit and poll" tasks a day. A few ML-feature tasks needed 32 GB of RAM and GPU libraries. Routing those to the Kubernetes executor with a `pod_override` let them have their own images and node pools. The Celery workers stayed small and identical.

**Common Mistakes / Interviewer Red Flags:**
- Configuring the second executor on the scheduler but not on other components, which leads to parse errors in some places.
- Proposing CeleryKubernetesExecutor for Airflow 3.
- Confusing `executor=` with `queue=`. A queue routes between Celery worker groups *within* the Celery executor.

**Follow-up Questions Interviewers Ask:**
- How does this relate to KubernetesPodOperator? — KPO runs a pod from any executor. Executor routing decides where the Airflow task process itself runs (see Q556).
- Does the scheduler run the executors? — Yes. Executor logic runs inside each scheduler process; there is no separate executor service.

---

### Q554. What is the Edge Executor, and when would you use it?
**ID:** AF-36
**Topic:** Airflow > Executors
**Level:** Advanced
**Frequency:** Occasional
**Tags:** `airflow`, `edge-executor`, `remote-execution`, `hybrid`

**Short Answer (2–3 lines):**
The `EdgeExecutor`, from the `apache-airflow-providers-edge3` package, sends tasks to lightweight **edge workers** that run anywhere with outbound HTTPS to the API server: a factory, a branch office, another cloud or an on-premises data centre. Workers pull jobs over HTTP, so nothing needs inbound firewall rules, a message broker or database access. It usually runs alongside Celery or Kubernetes as one of several executors.

**Detailed Explanation:**
How it works:
- The central scheduler queues jobs for the `EdgeExecutor`. The API server exposes the edge API (`[edge] api_enabled = True`, `[edge] api_url`).
- An edge worker started with `airflow edge worker -q <queues>` polls for jobs, runs them with the Task SDK, and sends back heartbeats and logs over HTTPS. It authenticates with the shared `[api_auth] jwt_secret`.
- Tasks are routed with `executor="EdgeExecutor"` plus a `queue` that only the right site's workers listen on. If the Edge Executor isn't the default, both are needed.
- On Airflow 3.2+, the provider registers its own database tables automatically. Older versions need `[database] external_db_managers`.
- From 3.2, multi-team mode can give each team its own Edge Executor and workers.

Requirements and limits:
- Each worker needs Airflow, the Task SDK, the edge3 provider and the libraries its tasks use. DAG files must be synced to it, for example by Git sync, because the worker parses them.
- Configuration must match the central site.
- Throughput and latency depend on HTTP polling. It suits tens to hundreds of remote tasks, not tens of thousands of short tasks a minute.

| Use Edge when | Use something else when |
|---|---|
| Data can't leave a site, or the network only allows outbound connections | Everything runs in one cloud network (use Celery or Kubernetes) |
| You need hardware on site (OT systems, GPUs on premises) | You only need a different image (KubernetesPodOperator) |
| A few hybrid tasks in an otherwise central platform | The remote site runs its own full Airflow anyway |

**Example / Code:**
```properties
# Runtime: Apache Airflow 3.3 + apache-airflow-providers-edge3 — airflow.cfg on central site AND workers
[core]
executor = CeleryExecutor,airflow.providers.edge3.executors.EdgeExecutor
[api_auth]
jwt_secret = <same random secret everywhere>
[edge]
api_enabled = True
api_url = https://airflow.example.internal/edge_worker/v1/rpcapi
```
```python
# Runtime: Python 3.11, Apache Airflow 3.3, apache-airflow-providers-edge3
# [core] executor = CeleryExecutor,airflow.providers.edge3.executors.EdgeExecutor
from datetime import datetime
from airflow.sdk import dag, task


@dag(schedule="*/30 * * * *", start_date=datetime(2026, 1, 1), catchup=False)
def plant_sync():
    # Runs on an edge worker inside the factory network, started with:
    #   airflow edge worker -q plant_berlin
    @task(executor="EdgeExecutor", queue="plant_berlin")
    def read_scada_export() -> str:
        return "/data/exports/latest.csv"

    @task                       # default executor in the central cluster
    def load_to_lake(path: str):
        print("uploading", path)

    load_to_lake(read_scada_export())


plant_sync()
```
```text
# DagBag parse with executor = LocalExecutor,airflow.providers.edge3.executors.EdgeExecutor
errors: {}
{'read_scada_export': ('EdgeExecutor', 'plant_berlin'), 'load_to_lake': (None, 'default')}
```

**Real-World Use Case:**
A manufacturer had to read machine exports that lived on a plant network, which allowed no inbound connections. One edge worker per plant, each on its own queue, read the files locally and uploaded summaries. Everything else ran on the central Kubernetes executor, and operators managed all of it in one Airflow UI.

**Common Mistakes / Interviewer Red Flags:**
- Expecting edge workers to work without the DAG files or the tasks' libraries.
- Setting `queue="site_a"` but not `executor="EdgeExecutor"` when Edge isn't the default executor.
- Treating it as a way around security reviews. The worker still runs arbitrary DAG code inside that network.

**Follow-up Questions Interviewers Ask:**
- How do you take an edge worker out of service? — Put it into maintenance mode from the UI or CLI, so it stops taking jobs and drains.
- Edge or a remote Celery worker? — Celery needs the worker to reach the broker, and in practice the metadata/result backends too. Edge only needs outbound HTTPS to the API server.

---

### Q555. How do you scale and make the scheduler highly available?
**ID:** AF-37
**Topic:** Airflow > Scheduler Scaling
**Level:** Advanced
**Frequency:** Common
**Tags:** `airflow`, `scheduler`, `high-availability`, `performance`

**Short Answer (2–3 lines):**
Run two or more schedulers against a Postgres or MySQL 8+ metadata DB. They coordinate through row-level locks (`SELECT … FOR UPDATE SKIP LOCKED`), so all of them are active at once and if one dies the others carry on. Scale the DAG processor and triggerer separately. Tune the loop with `max_dagruns_per_loop_to_schedule`, `max_tis_per_query` and `parallelism`, and look at the database first when things get slow.

**Detailed Explanation:**
In Airflow 3 there are several components to scale, not one:
| Component | Scales by | Bottleneck symptoms |
|---|---|---|
| Scheduler | More replicas (active-active) | Runs late to start, tasks slow to queue |
| DAG processor | `parsing_processes`, more replicas, separate bundles | DAGs update slowly, parse timeouts (see Q568) |
| Triggerer | Replicas × `[triggerer] capacity` (1000) | Deferred tasks resume late |
| API server | Replicas, workers | Slow UI, slow Execution API calls from tasks |
| Executor/workers | Celery workers, K8s nodes | Tasks sit in `queued` |
| Metadata DB | Instance size, connection pooling (PgBouncer), cleanup | Everything is slow |

Scheduler essentials:
- **HA needs** `[scheduler] use_row_level_locking = True` (the default) and a database with `SKIP LOCKED`. SQLite supports neither, so it is for local development only.
- **`parallelism` is per scheduler.** Three schedulers with 32 each can have 96 task instances running or queued.
- **Loop tuning:** `max_dagruns_to_create_per_loop` (10), `max_dagruns_per_loop_to_schedule` (20) and `max_tis_per_query` (16). Raise them carefully; larger batches mean longer locks.
- **Database hygiene:** `airflow db clean --clean-before-timestamp ...` trims old runs, task instances, XComs and logs metadata. Large tables slow every query the scheduler makes.
- **Health checks:** `airflow jobs check --job-type SchedulerJob --allow-multiple`, which can serve as a liveness probe, plus `scheduler_heartbeat` metrics.

Before adding schedulers, check whether the problem is parsing (top-level code), the database, or concurrency limits (see Q549). A second scheduler fixes none of those.

**Example / Code:**
```bash
# Runtime: Apache Airflow 3.3 CLI (run on any host with the deployment's config)
airflow jobs check --job-type SchedulerJob --allow-multiple --limit 5   # exits 0 if a scheduler is alive
airflow jobs check --job-type TriggererJob --allow-multiple
airflow jobs check --job-type DagProcessorJob --allow-multiple
airflow db clean --clean-before-timestamp "$(date -u -d '-90 days' +%Y-%m-%dT%H:%M:%S)" --yes
```
```text
# On a machine where no scheduler is running, the first command prints and exits non-zero:
No alive jobs found.
```
```properties
# airflow.cfg values worth knowing (3.3 defaults shown)
[core]
parallelism = 32
[scheduler]
use_row_level_locking = True
max_dagruns_to_create_per_loop = 10
max_dagruns_per_loop_to_schedule = 20
max_tis_per_query = 16
[dag_processor]
parsing_processes = 2
[triggerer]
capacity = 1000
```

**Real-World Use Case:**
A deployment with about 1,500 DAGs had runs starting up to 10 minutes late. Metrics showed the scheduler loop was fine, but DB CPU sat at 90%. The task-instance table held three years of history. After `airflow db clean` to keep 90 days, PgBouncer in front of Postgres, and a second scheduler for redundancy, start latency dropped below 30 seconds.

**Common Mistakes / Interviewer Red Flags:**
- Thinking only one scheduler can be active, an active-passive model. Since 2.0, all schedulers are active.
- Running HA schedulers on SQLite, or on MySQL 5.x without `SKIP LOCKED`.
- Scaling schedulers to fix slow parsing, which is the DAG processor's job in Airflow 3.

**Follow-up Questions Interviewers Ask:**
- What does `--only-idle` do (3.2+)? — Used with `--num-runs`, it counts only the loops where the scheduler was idle. A short-lived scheduler, for example in CI, then processes every triggered DAG and queued task before it exits.
- How do you see the scheduler loop time? — The `scheduler.scheduler_loop_duration` metric, plus the `dag_processing.*` metrics for parsing.

---

### Q556. KubernetesPodOperator vs KubernetesExecutor vs virtualenv tasks: how do you isolate dependencies?
**ID:** AF-38
**Topic:** Airflow > Task Isolation
**Level:** Intermediate
**Frequency:** Very Common
**Tags:** `airflow`, `kubernetes-pod-operator`, `isolation`, `dependencies`

**Short Answer (2–3 lines):**
`KubernetesPodOperator` runs **any container image** as one task, from any executor. The Airflow task only launches the pod and watches it. `KubernetesExecutor` runs the *Airflow task itself* in a new pod. `@task.virtualenv` and `@task.external_python` isolate Python packages on the worker without building an image. `@task.docker` and `@task.kubernetes` run a Python function inside a container.

**Detailed Explanation:**
| Option | What is isolated | Startup cost | Best for |
|---|---|---|---|
| `@task.virtualenv(requirements=[...])` | Python packages; venv built per run (cacheable) | Seconds to minutes | Occasional conflicting libraries |
| `@task.external_python(python="/opt/venvs/x/bin/python")` | A venv that already exists | Low | Stable, heavy dependencies baked into the worker image |
| `@task.docker` / `DockerOperator` | Container on a Docker host | Medium | Docker-based setups |
| `@task.kubernetes` | A Python function in a pod with a chosen image | Pod start | Python code with its own image |
| `KubernetesPodOperator` | Any image, any language, own CPU/RAM/GPU | Pod start | Spark-submit, dbt, ML, non-Python tools |
| `KubernetesExecutor` (`executor=` routing) | The whole Airflow task process | Pod start | Per-task resources while keeping normal operators |

KubernetesPodOperator settings that matter:
- `image` with a pinned tag, `cmds`/`arguments` (templated), and `container_resources` requests and limits.
- `namespace`, `service_account_name` for cloud IAM through workload identity, and `node_selector`/`tolerations`.
- `on_finish_action="delete_succeeded_pod"` keeps failed pods around for debugging.
- `get_logs=True` streams logs. `do_xcom_push=True` reads `/airflow/xcom/return.json` from a sidecar.
- `deferrable=True` waits in the triggerer instead of keeping a worker busy while the pod runs (see Q543).
- `reattach_on_restart=True`, the default, avoids launching a second pod after a worker restart.

The usual mistake is mixing up KPO and `KubernetesExecutor` (see Q42). KPO works fine on Celery.

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3, apache-airflow-providers-cncf-kubernetes 10.x
from datetime import datetime
from kubernetes.client import models as k8s
from airflow.sdk import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


@dag(schedule="0 3 * * *", start_date=datetime(2026, 1, 1), catchup=False)
def isolated_tasks():
    # 1) Any container image, any language, own CPU/memory; works with ANY executor.
    score = KubernetesPodOperator(
        task_id="score_customers",
        name="score-customers",
        namespace="data-jobs",
        image="registry.internal/ml/scorer:2026.05.1",        # pinned tag, not :latest
        cmds=["python", "-m", "scorer"],
        arguments=["--date", "{{ ds }}"],                       # templated
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "2", "memory": "8Gi"}, limits={"memory": "8Gi"}),
        env_vars={"OUTPUT": "s3://lake/gold/scores/{{ ds }}/"},
        on_finish_action="delete_succeeded_pod",                # keep failed pods for debugging
        get_logs=True,
        deferrable=True,                                        # wait in the triggerer, not a worker
    )

    # 2) Python with conflicting libraries, no image build: a throwaway virtualenv on the worker.
    @task.virtualenv(requirements=["pandas==2.2.3"], system_site_packages=False)
    def legacy_report():
        import pandas as pd
        return pd.__version__

    score >> legacy_report()


isolated_tasks()
```
```text
# DagBag parse (no cluster needed to parse):
errors: {}
{'score_customers': 'KubernetesPodOperator', 'legacy_report': '_PythonVirtualenvDecoratedOperator'}
image: registry.internal/ml/scorer:2026.05.1 | requests: {'cpu': '2', 'memory': '8Gi'} | namespace: data-jobs | deferrable: True
```

**Real-World Use Case:**
A data-science team needed CUDA libraries and a newer scikit-learn than the Airflow image allowed. Their scoring job became a KubernetesPodOperator on a GPU node pool with its own image, built and versioned by the DS team. The Airflow image stayed small and stable, and the platform team no longer had to settle dependency conflicts between teams.

**Common Mistakes / Interviewer Red Flags:**
- Using the `:latest` tag, which makes reruns not reproducible.
- No resource requests or limits, so pods get evicted or starve their neighbours.
- `@task.virtualenv` with a large requirements list on every run, without caching (`venv_cache_path`).

**Follow-up Questions Interviewers Ask:**
- How does a KPO pod return a value? — Write JSON to `/airflow/xcom/return.json` and set `do_xcom_push=True`.
- How does a pod get cloud credentials? — Through a Kubernetes service account mapped to a cloud IAM role, such as IRSA or GKE Workload Identity, not through keys in environment variables.

---

### Q557. What are the Task SDK and the Task Execution API, and why do they matter?
**ID:** AF-39
**Topic:** Airflow > Task SDK
**Level:** Advanced
**Frequency:** Common
**Tags:** `airflow`, `task-sdk`, `execution-api`, `airflow-3`

**Short Answer (2–3 lines):**
In Airflow 3, task code talks to Airflow only through the **Task SDK** (`airflow.sdk`). A supervisor process on the worker sends those calls to the API server's **Task Execution API** over HTTP, using a short-lived JWT for that task instance. Workers no longer need metadata-DB credentials. This brings security, independent upgrades of the SDK, remote execution (Edge), and task SDKs in other languages (Java and Go, experimental in 3.3).

**Detailed Explanation:**
Airflow 2: every worker had the DB connection string. Any task could open an ORM session and read or change anything in the database.

Airflow 3:
1. The executor starts the **supervisor** on the worker. The supervisor starts the task process and relays its requests: Variables, Connections, XCom, state changes, heartbeats and logs.
2. The supervisor calls the Execution API (`/execution/...` on the API server) with a token scoped to that task instance.
3. The task process sees a stable interface: `airflow.sdk` classes, `get_current_context()`, `Variable`, `Connection`, XCom and `outlet_events`.

What this means for DAG authors:
| Old pattern | Airflow 3 replacement |
|---|---|
| `from airflow.settings import Session` in task code | Not supported. Use the SDK, or the REST API / Python client for admin operations |
| `DagRun.find(...)` or `TaskInstance` queries in a task | The context (`dag_run`, `ti`), or the REST API |
| `from airflow.models import Variable` | `from airflow.sdk import Variable` |
| Raising `airflow.exceptions.AirflowSkipException` | `airflow.sdk.exceptions` |

Operational effects: API-server capacity now affects running tasks. Network rules must allow workers to reach the API server. `[core] execution_api_server_url` must be correct on the workers. The Task SDK can be upgraded on a different cadence from airflow-core (since 3.1). Note that the DAG processor and the triggerer still run user code with DB access, so that boundary is not complete.

The 3.3 **Coordinator** layer (AIP-108) lets `@task.stub(queue=...)` tasks run in Java or Go runtimes that use the same Execution API. *Product behaviour changes; checked October 2026.*

**Example / Code:**
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import Variable, dag, get_current_context, task


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def sdk_boundary():
    @task
    def produce(dag_run=None) -> dict:
        # Each call below is a Task SDK call that the worker's supervisor sends to the
        # Execution API (HTTP + a short-lived JWT for this task instance) - no DB session.
        Variable.set("last_seen_run", dag_run.run_id)
        return {"rows": 10}

    @task
    def consume(payload: dict):
        ctx = get_current_context()
        prev = ctx["ti"].xcom_pull(task_ids="produce")
        print("variable ok:", Variable.get("last_seen_run") == ctx["dag_run"].run_id,
              "| xcom:", prev, "| arg:", payload, "| try:", ctx["ti"].try_number)

    consume(produce())


dag_obj = sdk_boundary()

if __name__ == "__main__":
    print(dag_obj.test().state)
```
```text
variable ok: True | xcom: {'rows': 10} | arg: {'rows': 10} | try: 1
success
```
`dag.test()` runs in one process that has the database configured. An ORM query inside a task would therefore "work" locally but fail on real workers, which get no DB credentials. Catch it in code review, and with `ruff --select AIR` in CI (see Q564), not with `dag.test()`.

**Real-World Use Case:**
A security review flagged that every Airflow 2 worker pod held the metadata-DB password. After the move to Airflow 3, worker pods held only the API URL and per-task tokens. Two legacy tasks that queried `TaskInstance` directly were rewritten to use the context and the REST API's Python client.

**Common Mistakes / Interviewer Red Flags:**
- Saying tasks "can't access Variables any more". They can, through the SDK.
- Leaving the API server undersized, even though every running task now calls it.
- Claiming the DAG processor is isolated as well. It still runs user code with DB access.

**Follow-up Questions Interviewers Ask:**
- What is `airflowctl`? — The remote CLI that talks to the REST API. It is separate from the `airflow` CLI that runs components (AIP-81).
- How does a task get a Connection with a secrets backend? — The supervisor gets it from the worker's secrets backend if one is configured, otherwise through the Execution API (see Q559).

---

### Q558. What are DAG bundles and DAG versioning in Airflow 3?
**ID:** AF-40
**Topic:** Airflow > DAG Bundles and Versioning
**Level:** Intermediate
**Frequency:** Common
**Tags:** `airflow`, `dag-bundles`, `dag-versioning`, `airflow-3`

**Short Answer (2–3 lines):**
A DAG bundle is a source of DAG code: a local folder, a Git repository, or an S3 or GCS prefix. Airflow records a new **DAG version** whenever a DAG's structure changes after it has run. Versioned bundles, today `GitDagBundle`, also pin each run to the commit it started with. A run's tasks keep running that code even if `main` moves during the run, and the UI shows each run's own graph.

**Detailed Explanation:**
| Bundle | Versioned? | Notes |
|---|---|---|
| `LocalDagBundle` (default `dags-folder`) | No | Same behaviour as Airflow 2's DAGs folder; always the latest code |
| `GitDagBundle` (git provider) | **Yes** | `tracking_ref` (branch, tag or SHA), `git_conn_id` or `repo_url`, `subdir`, `sparse_dirs` |
| `S3DagBundle` / `GCSDagBundle` | No | Bucket and prefix; latest code |

Configure bundles with `[dag_processor] dag_bundle_config_list`, a JSON list of `{name, classpath, kwargs}`. You can mix bundle types and have several repositories, for example one per team.

DAG versioning behaviour:
- The DAG processor serialises each DAG. If the result differs and the current version already has task instances, a new version number is created. Otherwise the current version is updated in place.
- Each run records `created_dag_version`. The grid and graph views show history *as it was*, so removed tasks no longer vanish from the history.
- **Rerun and clear (3.3).** `rerun_with_latest_version` (DAG argument, `[core]` setting, or a per-request flag) chooses between the original bundle version and the latest one. The default is the original version for clear and rerun, and the latest version for backfills.
- `disable_bundle_versioning=True` on a DAG opts it out of versioning.

Why interviewers care: Airflow 2 always reran with the latest code. A fix deployed in the middle of a run could leave half a run on old code and half on new. Versioned bundles make reruns reproducible and deploys safer.

**Example / Code:**
```properties
# Runtime: Apache Airflow 3.3 + apache-airflow-providers-git — airflow.cfg
[dag_processor]
dag_bundle_config_list = [
    {"name": "analytics-dags",
     "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
     "kwargs": {"git_conn_id": "github_analytics", "tracking_ref": "main", "subdir": "dags"}},
    {"name": "dags-folder",
     "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
     "kwargs": {}}
  ]
[core]
rerun_with_latest_version = False
```
```python
# Runtime: Python 3.11, Apache Airflow 3.3
from datetime import datetime
from airflow.sdk import dag, task


@dag(
    schedule="0 5 * * *", start_date=datetime(2026, 1, 1), catchup=False,
    rerun_with_latest_version=False,   # clears/reruns use the commit the run started with (3.3)
    disable_bundle_versioning=False,   # opt a single DAG out of versioning if it must always run latest
)
def versioned_pipeline():
    @task
    def extract():
        return 1

    extract()


versioned_pipeline()
```
```text
# Parsed with DagBag; the default bundle list printed by "airflow config get-value dag_processor dag_bundle_config_list":
{'rerun_with_latest_version': False, 'disable_bundle_versioning': False}
[
{
"name": "dags-folder",
"classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
"kwargs": {}
}
]
```

**Real-World Use Case:**
An engineer merged a refactor that renamed a task while the nightly run was halfway through. In Airflow 2, the remaining tasks would have picked up the new code and the rename would have broken the run. With a `GitDagBundle`, the run finished on the commit it started with, and the next night's run used the new version. Both graphs stayed visible in the UI history.

**Common Mistakes / Interviewer Red Flags:**
- Assuming versioning pins code when you use the local DAGs folder. Only versioned bundles pin code. The local folder still records DAG *structure* versions.
- Expecting a clear to use new code by default in 3.3. Clears default to the original version; use "run on latest version" when you are deploying a fix.
- Not giving the DAG processor and the workers Git credentials and network access.

**Follow-up Questions Interviewers Ask:**
- Where does a worker get the right code? — It checks out the bundle version recorded on the run, such as a Git commit, before parsing the DAG file.
- How do you clean up old checkouts? — The `[dag_processor] stale_bundle_cleanup_*` settings control how long old bundle versions are kept on disk.

---
