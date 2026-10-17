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
