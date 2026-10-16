---
title: "Python interview questions for Data Engineers"
seoTitle: "Python Interview Questions for Data Engineers"
description: "50 Python interview questions for data engineers with answers: generators, concurrency, pandas, error handling, testing, API ingestion and pipeline code."
hub: "python"
label: "Python"
section: 7
publishedDate: "2026-10-16"
updatedDate: "2026-10-16"
levels: {"Beginner": 15, "Intermediate": 23, "Advanced": 12}
questions: [{"num": 351, "id": "PY-01", "title": "list vs tuple vs set vs dict", "topic": "Data Structures", "level": "Beginner", "frequency": "Very Common"}, {"num": 352, "id": "PY-02", "title": "Mutable vs immutable types, and the mutable default argument trap", "topic": "Language Fundamentals", "level": "Beginner", "frequency": "Very Common"}, {"num": 353, "id": "PY-03", "title": "Shallow copy vs deep copy", "topic": "Language Fundamentals", "level": "Beginner", "frequency": "Common"}, {"num": 354, "id": "PY-04", "title": "List, dict and set comprehensions", "topic": "Language Fundamentals", "level": "Beginner", "frequency": "Common"}, {"num": 355, "id": "PY-05", "title": "Iterables vs iterators vs generators", "topic": "Iteration", "level": "Intermediate", "frequency": "Very Common"}, {"num": 356, "id": "PY-06", "title": "Building generator pipelines with yield from", "topic": "Iteration", "level": "Advanced", "frequency": "Common"}, {"num": 357, "id": "PY-07", "title": "Decorators (with arguments and functools.wraps)", "topic": "Functions", "level": "Intermediate", "frequency": "Very Common"}, {"num": 358, "id": "PY-08", "title": "Context managers and the with statement", "topic": "Resource Management", "level": "Intermediate", "frequency": "Very Common"}, {"num": 359, "id": "PY-09", "title": "*args and **kwargs", "topic": "Functions", "level": "Beginner", "frequency": "Very Common"}, {"num": 360, "id": "PY-10", "title": "OOP: inheritance vs composition", "topic": "Object-Oriented Design", "level": "Beginner", "frequency": "Common"}, {"num": 361, "id": "PY-11", "title": "Instance vs @classmethod vs @staticmethod", "topic": "Object-Oriented Design", "level": "Beginner", "frequency": "Common"}, {"num": 362, "id": "PY-12", "title": "dataclasses vs Pydantic models", "topic": "Data Modelling", "level": "Intermediate", "frequency": "Common"}, {"num": 363, "id": "PY-13", "title": "Useful dunder methods", "topic": "Object-Oriented Design", "level": "Intermediate", "frequency": "Occasional"}, {"num": 364, "id": "PY-14", "title": "Exception handling best practices", "topic": "Error Handling", "level": "Beginner", "frequency": "Very Common"}, {"num": 365, "id": "PY-15", "title": "Custom exceptions and retryable errors", "topic": "Error Handling", "level": "Intermediate", "frequency": "Common"}, {"num": 366, "id": "PY-16", "title": "The GIL, and the free-threaded build in Python 3.13+", "topic": "Concurrency", "level": "Advanced", "frequency": "Very Common"}, {"num": 367, "id": "PY-17", "title": "threading vs multiprocessing vs asyncio", "topic": "Concurrency", "level": "Advanced", "frequency": "Very Common"}, {"num": 368, "id": "PY-18", "title": "concurrent.futures executors", "topic": "Concurrency", "level": "Intermediate", "frequency": "Common"}, {"num": 369, "id": "PY-19", "title": "Concurrent API calls with asyncio", "topic": "Concurrency", "level": "Advanced", "frequency": "Common"}, {"num": 370, "id": "PY-20", "title": "Memory management: reference counting and garbage collection", "topic": "Internals", "level": "Advanced", "frequency": "Common"}, {"num": 371, "id": "PY-21", "title": "Profiling and speeding up Python code", "topic": "Performance", "level": "Advanced", "frequency": "Common"}, {"num": 372, "id": "PY-22", "title": "pandas: loc vs iloc, and vectorisation vs apply", "topic": "pandas", "level": "Intermediate", "frequency": "Very Common"}, {"num": 373, "id": "PY-23", "title": "pandas merge, concat and groupby/agg", "topic": "pandas", "level": "Intermediate", "frequency": "Very Common"}, {"num": 374, "id": "PY-24", "title": "Processing data larger than memory in pandas (chunks, dtypes)", "topic": "pandas", "level": "Intermediate", "frequency": "Very Common"}, {"num": 375, "id": "PY-25", "title": "pandas vs Polars vs PySpark", "topic": "Data Libraries", "level": "Advanced", "frequency": "Common"}, {"num": 376, "id": "PY-26", "title": "Reading and writing CSV, JSON and Parquet", "topic": "File I/O", "level": "Beginner", "frequency": "Very Common"}, {"num": 377, "id": "PY-27", "title": "Read a huge file in chunks with a generator", "topic": "File I/O", "level": "Intermediate", "frequency": "Very Common"}, {"num": 378, "id": "PY-28", "title": "API ingestion with pagination (offset, cursor, next-link)", "topic": "API Ingestion", "level": "Advanced", "frequency": "Very Common"}, {"num": 379, "id": "PY-29", "title": "Retries with exponential backoff and rate limiting", "topic": "Reliability", "level": "Advanced", "frequency": "Common"}, {"num": 380, "id": "PY-30", "title": "Logging in pipelines", "topic": "Observability", "level": "Intermediate", "frequency": "Common"}, {"num": 381, "id": "PY-31", "title": "Configuration and secrets via environment variables", "topic": "Configuration", "level": "Beginner", "frequency": "Common"}, {"num": 382, "id": "PY-32", "title": "pytest fixtures and parametrize", "topic": "Testing", "level": "Intermediate", "frequency": "Common"}, {"num": 383, "id": "PY-33", "title": "Mocking external services in tests", "topic": "Testing", "level": "Advanced", "frequency": "Common"}, {"num": 384, "id": "PY-34", "title": "Packaging and environments: venv, pip, pyproject.toml, uv", "topic": "Tooling", "level": "Intermediate", "frequency": "Common"}, {"num": 385, "id": "PY-35", "title": "Type hints and static checking", "topic": "Code Quality", "level": "Intermediate", "frequency": "Common"}, {"num": 386, "id": "PY-36", "title": "is vs ==", "topic": "Language Fundamentals", "level": "Beginner", "frequency": "Common"}, {"num": 387, "id": "PY-37", "title": "Coding: flatten a nested JSON object", "topic": "Coding Patterns", "level": "Intermediate", "frequency": "Very Common"}, {"num": 388, "id": "PY-38", "title": "Coding: deduplicate records by key, keeping the latest", "topic": "Coding Patterns", "level": "Intermediate", "frequency": "Very Common"}, {"num": 389, "id": "PY-39", "title": "Coding: word count and top-k frequent words", "topic": "Coding Patterns", "level": "Beginner", "frequency": "Very Common"}, {"num": 390, "id": "PY-40", "title": "Coding: merge overlapping intervals", "topic": "Coding Patterns", "level": "Intermediate", "frequency": "Common"}, {"num": 391, "id": "PY-41", "title": "Coding: validate records and split good from bad", "topic": "Data Quality", "level": "Intermediate", "frequency": "Common"}, {"num": 392, "id": "PY-42", "title": "Coding: parse log files with regular expressions", "topic": "Coding Patterns", "level": "Intermediate", "frequency": "Common"}, {"num": 393, "id": "PY-43", "title": "Coding: implement an LRU cache", "topic": "Coding Patterns", "level": "Advanced", "frequency": "Occasional"}, {"num": 394, "id": "PY-44", "title": "Coding: group anagrams / two-sum with dictionaries", "topic": "Coding Patterns", "level": "Beginner", "frequency": "Common"}, {"num": 395, "id": "PY-45", "title": "Coding: batch an iterable into chunks", "topic": "Coding Patterns", "level": "Beginner", "frequency": "Common"}, {"num": 396, "id": "PY-46", "title": "datetime and time zones with zoneinfo", "topic": "Dates & Times", "level": "Intermediate", "frequency": "Common"}, {"num": 397, "id": "PY-47", "title": "Querying databases from Python safely (parameterised queries, SQLAlchemy)", "topic": "Databases", "level": "Intermediate", "frequency": "Common"}, {"num": 398, "id": "PY-48", "title": "Working with S3 using boto3", "topic": "Cloud Storage", "level": "Intermediate", "frequency": "Common"}, {"num": 399, "id": "PY-49", "title": "Structure of an idempotent, restartable ETL script", "topic": "Pipeline Design", "level": "Advanced", "frequency": "Common"}, {"num": 400, "id": "PY-50", "title": "lambda, map, filter and reduce", "topic": "Functional Programming", "level": "Beginner", "frequency": "Common"}]
---

<details class="qb-item" id="q351" data-level="Beginner">
<summary><span class="qb-num">Q351</span><h3 class="qb-title">list vs tuple vs set vs dict</h3><span class="qb-meta">Beginner · Very Common · Data Structures</span></summary>

**Short answer**

A list is an ordered, mutable sequence that allows duplicates. A tuple is ordered and immutable, so it's hashable if its contents are, and works as a dict key or a fixed record. A set holds unordered, unique, hashable items, with O(1) average membership tests. A dict maps unique hashable keys to values, keeps insertion order (Python 3.7+), and has O(1) average lookup.

**Detailed explanation**

| Type | Ordered | Mutable | Duplicates | Lookup by | Typical complexity |
|---|---|---|---|---|---|
| `list` | Yes | Yes | Yes | Index | Index O(1); `in` O(n); append O(1) amortised; insert/delete at front O(n) |
| `tuple` | Yes | No | Yes | Index | Index O(1); `in` O(n) |
| `set` | No (iteration order arbitrary) | Yes (`frozenset` immutable) | No | Membership | `in`, add, remove O(1) average |
| `dict` | Insertion order | Yes | Keys unique | Key | get/set/`in` O(1) average |

**Choosing in data engineering code**
- **list:** ordered batches of records, results to write in order.
- **tuple:** fixed-shape records such as `(customer_id, order_date)`, composite dict keys, function returns. Immutability prevents accidental changes. `NamedTuple` or dataclasses add field names (see [Q362](/interview/python/#q362)).
- **set:** deduplicating IDs, fast "have we seen this?" checks, and set algebra (`a - b` for missing keys, `a & b` for overlap) when reconciling sources.
- **dict:** lookups (code to label), grouping (`defaultdict(list)`), counting (`Counter`), JSON-like records.

**Hashability:** set members and dict keys must be hashable. Lists and dicts aren't, so convert them to tuples or frozensets. Floats like `NaN` behave oddly as keys (`NaN != NaN`).

**Performance trap:** `if x in big_list` inside a loop is O(n²). Convert the list to a set first.

**Memory:** tuples are slightly smaller than lists, and sets and dicts use more memory because of their hash tables. For millions of rows, use columnar structures (pandas, Polars, Arrow) instead of lists of dicts (see [Q374](/interview/python/#q374)).

**Example**

```python
# Runtime: Python 3.12
from collections import Counter, defaultdict

source_ids = [101, 102, 103, 103, 104]
target_ids = {102, 104, 105}

missing_in_target = set(source_ids) - target_ids          # {101, 103}
unexpected_in_target = target_ids - set(source_ids)       # {105}

orders = [("c1", 120.0), ("c2", 40.0), ("c1", 30.0)]      # tuples as records
spend = defaultdict(float)
for customer_id, amount in orders:
    spend[customer_id] += amount                          # {'c1': 150.0, 'c2': 40.0}

status_counts = Counter(["paid", "paid", "refunded"])     # Counter({'paid': 2, 'refunded': 1})
daily_key = {("c1", "2026-03-01"): 120.0}                 # tuple as composite key
print(missing_in_target, unexpected_in_target, dict(spend), status_counts)
```

**Real-world use case**

A reconciliation script checked 2 million source IDs against a target list with `in`, and took 40 minutes. Converting the target IDs to a set made it finish in 2 seconds.

**Common mistakes and red flags**

- Membership tests on large lists.
- Using lists as dict keys.
- Assuming sets preserve order.

**Follow-up questions**

- Why is dict lookup O(1)? — Hash table: key hash → bucket; collisions resolved by probing.
- When is a tuple mutable in practice? — If it contains mutable objects (a tuple holding a list), the inner list can change.

**In depth:** [List vs tuple vs set: when would you use each in a data pipeline?](/interview/python/list-tuple-set/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q352" data-level="Beginner">
<summary><span class="qb-num">Q352</span><h3 class="qb-title">Mutable vs immutable types, and the mutable default argument trap</h3><span class="qb-meta">Beginner · Very Common · Language Fundamentals</span></summary>

**Short answer**

Immutable objects (int, float, str, tuple, frozenset, bytes) can't change in place. "Modifying" one creates a new object. Mutable objects (list, dict, set, most class instances) change in place, and every reference sees the change. Default argument values are evaluated once, when the function is defined, so a mutable default like `def f(x=[])` is shared across calls. Use `None` and create the object inside the function.

**Detailed explanation**

**Names are references.** `a = [1, 2]` then `b = a` means `b.append(3)` changes `a` too. Both names point to the same list. With immutables, `s = "x"` then `t = s` then `t += "y"` creates a new string for `t`, and `s` stays "x".

**Function arguments** are passed by object reference ("call by sharing"):
- Mutating a passed list or dict inside a function affects the caller.
- Rebinding the parameter (`items = []`) doesn't affect the caller.

**The default argument trap**
```python
def add_record(rec, batch=[]):     # evaluated once at def time
    batch.append(rec)
    return batch
add_record(1)  # [1]
add_record(2)  # [1, 2]  <- surprise: same list
```
Fix: `def add_record(rec, batch=None): batch = [] if batch is None else batch`.

The same issue appears with dataclass fields (use `field(default_factory=list)`; see [Q362](/interview/python/#q362)), class attributes shared across instances, and module-level caches reused across Lambda or worker invocations.

**Why it matters in pipelines**
- Shared mutable state leaks data between batches or requests. For example, an accumulator list keeps growing across Airflow task retries in the same worker process.
- Functions that mutate their inputs cause hard-to-trace bugs. Prefer returning new objects, or document the mutation clearly.
- Immutable objects are safe to share across threads and to use as dict keys.

**Example**

```python
# Runtime: Python 3.12
def normalise(records: list[dict], *, seen: set[str] | None = None) -> list[dict]:
    seen = set() if seen is None else seen
    out = []
    for r in records:
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        out.append({**r, "email": r["email"].strip().lower()})   # new dict, input untouched
    return out

batch1 = [{"id": "a", "email": " A@X.COM "}]
batch2 = [{"id": "a", "email": "a@x.com"}]
print(normalise(batch1), normalise(batch2))   # each call gets a fresh 'seen' set
```
Result: both calls return the record. With a mutable default `seen=set()`, the second call would wrongly drop "a".

**Real-world use case**

A Lambda function used `def handler(event, context, buffer=[])` to collect events, and warm invocations kept appending to the same list. Some outputs contained events from previous requests until the default was changed to `None`.

**Common mistakes and red flags**

- Mutable defaults.
- Assuming assignment copies objects.
- Mutating input dicts in shared utility functions.

**Follow-up questions**

- Is a tuple containing a list hashable? — No; hashing fails because the list isn't hashable.
- How do you copy instead of share? — `list(x)`, `x.copy()`, `copy.deepcopy(x)` (see [Q353](/interview/python/#q353)).

**In depth:** [Why are mutable default arguments dangerous in Python?](/interview/python/mutable-default-arguments/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q353" data-level="Beginner">
<summary><span class="qb-num">Q353</span><h3 class="qb-title">Shallow copy vs deep copy</h3><span class="qb-meta">Beginner · Common · Language Fundamentals</span></summary>

**Short answer**

A shallow copy (`list(x)`, `x.copy()`, `dict(x)`, `copy.copy(x)`, slicing) creates a new outer container, but the inner objects are still shared. `copy.deepcopy(x)` recursively copies nested objects, so the result is fully independent. Use a deep copy when you'll modify nested structures, such as JSON-like configs or records, without affecting the original.

**Detailed explanation**

**Illustration**
```text
original = {"id": 1, "tags": ["a", "b"]}
shallow  = original.copy()     -> new dict, SAME "tags" list
deep     = copy.deepcopy(original) -> new dict, NEW "tags" list
shallow["tags"].append("c")    -> original["tags"] is now ["a","b","c"]
deep["tags"].append("d")       -> original unaffected
```

**Ways to copy**
| Method | Depth |
|---|---|
| Assignment `b = a` | No copy (same object) |
| `a.copy()`, `list(a)`, `dict(a)`, `a[:]`, `{**a}`, `copy.copy(a)` | Shallow |
| `copy.deepcopy(a)` | Deep (handles cycles via memo) |
| `json.loads(json.dumps(a))` | Deep for JSON-safe data only (loses types like datetime, tuples→lists) |

**Performance:** `deepcopy` is slow for big structures, because it visits every object. In data pipelines, it's usually better to:
- **Build new structures** with comprehensions (`{**r, "x": new}` creates a new top-level dict, sharing unchanged nested values deliberately).
- Use **immutable records** (frozen dataclasses, tuples), so copying is unnecessary.
- Use **DataFrames**, whose `copy()` semantics are explicit. pandas 2.x copy-on-write mode avoids many accidental-mutation issues, and pandas 3.0 makes copy-on-write the default (see [Q372](/interview/python/#q372)).

**Common places this bites**
- Reusing a base config dict for several jobs and mutating nested sections.
- Building "template" records and appending to their nested lists.
- `[[0] * 3] * 3` creates three references to the same inner list. Use `[[0] * 3 for _ in range(3)]`.

**Example**

```python
# Runtime: Python 3.12
import copy

base_config = {"source": "s3://lake/raw/", "options": {"format": "parquet", "columns": ["id"]}}

job_a = copy.deepcopy(base_config)
job_a["options"]["columns"].append("amount")

job_b = base_config.copy()                 # shallow
job_b["options"]["columns"].append("ts")   # mutates base_config too!

print(base_config["options"]["columns"])   # ['id', 'ts']
print(job_a["options"]["columns"])         # ['id', 'amount']

grid_bad = [[0] * 2] * 2; grid_bad[0][0] = 9           # [[9, 0], [9, 0]]
grid_ok = [[0] * 2 for _ in range(2)]; grid_ok[0][0] = 9  # [[9, 0], [0, 0]]
```

**Real-world use case**

An ingestion framework generated per-table configs from a shared template with `.copy()`. Adding a column filter for one table silently added it to all of them. Switching to `deepcopy`, and later to frozen dataclass configs, fixed it.

**Common mistakes and red flags**

- Believing `.copy()` is deep.
- Using `deepcopy` on huge data in hot loops.
- The `[[x]*n]*m` trap.

**Follow-up questions**

- How does deepcopy handle cycles? — A memo dict tracks already-copied objects.
- How do you customise copying? — Implement `__copy__`/`__deepcopy__`.

**In depth:** [Explain shallow copy vs deep copy.](/interview/python/shallow-vs-deep-copy/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q354" data-level="Beginner">
<summary><span class="qb-num">Q354</span><h3 class="qb-title">List, dict and set comprehensions</h3><span class="qb-meta">Beginner · Common · Language Fundamentals</span></summary>

**Short answer**

Comprehensions build collections in one expression. `[f(x) for x in xs if cond]` builds a list, `{k: v for ...}` a dict, `{x for ...}` a set, and `(f(x) for x in xs)` a lazy generator. They're more readable and usually faster than equivalent loops that call `append`. Keep them simple, and switch to a regular loop or a helper function when the logic gets nested or complicated.

**Detailed explanation**

**Forms**
| Form | Result |
|---|---|
| `[expr for x in it if cond]` | list |
| `{key: val for x in it}` | dict |
| `{expr for x in it}` | set |
| `(expr for x in it)` | generator (lazy, one pass, low memory; see [Q355](/interview/python/#q355)) |
| Nested: `[y for row in rows for y in row]` | flatten one level (outer loop first) |
| Conditional expression: `[a if c else b for x in it]` | map with branching |

**Why they're faster:** the loop runs in optimised bytecode with a specialised append, and there's no repeated attribute lookup of `.append`. Python 3.12 inlined comprehensions (PEP 709), which speeds them up further.

**Readability rules of thumb**
- One `for` and one `if` is fine. Two `for`s is the limit. Beyond that, write a loop.
- Don't use comprehensions for side effects (`[print(x) for x in xs]`). Use a loop.
- Use the **walrus operator** to avoid computing twice: `[y for x in xs if (y := parse(x)) is not None]`.
- Use **generator expressions** for aggregates over large data: `sum(r["amount"] for r in records)` avoids building a list.

**Scope:** comprehension variables don't leak into the enclosing scope in Python 3.

**Example**

```python
# Runtime: Python 3.12
from datetime import date

rows = [
    {"id": "1", "amount": "10.50", "status": "paid", "dt": "2026-03-01"},
    {"id": "2", "amount": "",      "status": "void", "dt": "2026-03-01"},
    {"id": "3", "amount": "7.25",  "status": "paid", "dt": "2026-03-02"},
]

paid_amounts = [float(r["amount"]) for r in rows if r["status"] == "paid" and r["amount"]]
by_id = {r["id"]: r for r in rows}
statuses = {r["status"] for r in rows}
dates = sorted({date.fromisoformat(r["dt"]) for r in rows})
total = sum(float(r["amount"]) for r in rows if r["amount"])      # generator, no list

print(paid_amounts, list(by_id), statuses, dates, total)
# [10.5, 7.25] ['1', '2', '3'] {'paid', 'void'} [datetime.date(2026, 3, 1), datetime.date(2026, 3, 2)] 17.75
```

**Real-world use case**

A code review replaced a 25-line nested loop that built a lookup dict, with flag variables, by a two-line dict comprehension plus a helper function. It was easier to read, easier to test, and about 30% faster.

**Common mistakes and red flags**

- Triple-nested comprehensions.
- Comprehensions for side effects.
- Building big lists just to `sum` them.

**Follow-up questions**

- Comprehension vs `map`/`filter`? — Comprehensions are usually clearer; `map` with built-ins can be marginally faster (see [Q400](/interview/python/#q400)).
- How do you flatten a list of lists? — `[x for sub in nested for x in sub]` or `itertools.chain.from_iterable`.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q355" data-level="Intermediate">
<summary><span class="qb-num">Q355</span><h3 class="qb-title">Iterables vs iterators vs generators</h3><span class="qb-meta">Intermediate · Very Common · Iteration</span></summary>

**Short answer**

An iterable is anything you can loop over: it has `__iter__`, as lists, dicts, files and ranges do. An iterator is the object that produces items one at a time with `__next__`, and raises `StopIteration` when exhausted. Most iterators can only be consumed once. A generator is an easy way to write an iterator: a function using `yield`, or a generator expression, which produces values lazily without holding them all in memory.

**Detailed explanation**

**Protocol**
- `iter(iterable)` returns an iterator. `next(iterator)` returns the next item, or raises `StopIteration`.
- A `for` loop calls `iter()` once, then `next()` repeatedly.
- Iterators are themselves iterable: `iter(it) is it`.

**Iterable vs iterator**
| | Iterable (e.g. list) | Iterator (e.g. `iter(list)`, file object, generator) |
|---|---|---|
| Can loop multiple times | Yes (new iterator each time) | No — exhausted after one pass |
| Holds all data | Often | Usually produces on demand |
| `len()` | Often | Usually no |

**Generators**
- A function with `yield` returns a generator object when called. Its code runs only when you iterate it, pausing at each `yield` and keeping its local state.
- Generator expressions: `(line.strip() for line in f)`.
- Benefits: **constant memory** for large or infinite streams, composable **pipelines** (see [Q356](/interview/python/#q356)), and lazy evaluation (stop early without computing the rest).
- `return` inside a generator ends iteration. The value is attached to `StopIteration.value`.
- Extras: `send()`, `throw()` and `close()` for coroutine-style use. Rarely needed in DE code.

**Data engineering uses**
- Streaming large files line by line or in chunks (see [Q377](/interview/python/#q377)).
- Paginating APIs: yield one page or record at a time (see [Q378](/interview/python/#q378)).
- Batching (see [Q395](/interview/python/#q395)), transforms and filters chained without loading everything into memory.

**Gotchas**
- Exhaustion: iterating a generator twice gives nothing the second time. Materialise it with `list()` if you need several passes, and accept the memory cost.
- Lazy errors: exceptions surface when items are consumed, not when the generator is created.
- Open resources: a generator reading a file should use `with` inside, so the file closes when the generator finishes or is closed.

**Example**

```python
# Runtime: Python 3.12
from typing import Iterator

def read_valid_lines(path: str) -> Iterator[str]:
    with open(path, encoding="utf-8") as f:
        for line in f:                      # file object is an iterator
            line = line.strip()
            if line and not line.startswith("#"):
                yield line

lines = read_valid_lines("events.log")      # nothing read yet
first_10 = [next(lines) for _ in range(10)] # reads only as much as needed

nums = iter([1, 2, 3])
print(list(nums), list(nums))               # [1, 2, 3] []  <- exhausted
```
If the file has fewer than 10 valid lines, `next()` raises `StopIteration` inside the comprehension. Use `itertools.islice(lines, 10)` to take up to 10 safely.

**Real-world use case**

A script loaded a 30 GB log file with `f.readlines()` and was killed by the OOM killer. Rewriting it as a generator pipeline (read, filter, parse, batch-write) ran in under 200 MB of memory.

**Common mistakes and red flags**

- Iterating a generator twice.
- Calling `list()` on huge generators "to debug".
- Confusing generators with lists.

**Follow-up questions**

- How do you take the first N items lazily? — `itertools.islice(gen, N)`.
- Can a class be an iterator? — Yes: implement `__iter__` returning self and `__next__`.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q356" data-level="Advanced">
<summary><span class="qb-num">Q356</span><h3 class="qb-title">Building generator pipelines with yield from</h3><span class="qb-meta">Advanced · Common · Iteration</span></summary>

**Short answer**

Chain small generator functions, such as read → parse → filter → transform → batch, so that each step pulls items from the previous one lazily. Memory stays flat no matter how big the input is. `yield from sub_generator` delegates to another generator, yielding all its items (and passing through `send` and `throw`). It's useful for flattening, for combining several sources, and for recursive generators.

**Detailed explanation**

**Pipeline design**
- Each stage takes an iterable and yields outputs: `def parse(lines): for l in lines: yield json.loads(l)`.
- Compose them: `write(batch(transform(filter(parse(read(files))))))`.
- Only one item (or one batch) flows through at a time, which is the same idea as Unix pipes, or Spark's lazy evaluation on a single machine.

**`yield from`**
- `yield from iterable` is equivalent to `for x in iterable: yield x`, and also forwards `send` and `throw`, and captures the sub-generator's return value.
- Uses:
  - **Multiple files:** `for p in paths: yield from read_file(p)`.
  - **Recursion:** walking nested JSON or directories (see [Q387](/interview/python/#q387)).
  - **Composition:** building a generator from smaller ones.

**Strengths**
- Constant memory and early termination (`islice`).
- Each stage is a small, pure function, which is easy to unit test (see [Q382](/interview/python/#q382)).
- Readable data flow.

**Limits**
- **Single-threaded** and CPU-bound. For heavy parsing, consider multiprocessing (see [Q367](/interview/python/#q367)), Polars or DuckDB.
- **Error handling:** an exception in any stage stops the pipeline. Decide which stage catches and quarantines bad records (see [Q391](/interview/python/#q391)).
- **Ordering and side effects:** writes happen as items flow. Combine with idempotent sinks and checkpoints for restartability (see [Q399](/interview/python/#q399)).
- **Debugging:** lazy behaviour means errors appear far from the creation point. Log the record context.

**Example**

```python
# Runtime: Python 3.12
import gzip
import json
from itertools import batched          # Python 3.12+
from pathlib import Path
from typing import Iterable, Iterator

def read_files(paths: Iterable[Path]) -> Iterator[str]:
    for p in paths:
        opener = gzip.open if p.suffix == ".gz" else open
        with opener(p, "rt", encoding="utf-8") as f:
            yield from f

def parse(lines: Iterable[str]) -> Iterator[dict]:
    for line in lines:
        if line.strip():
            yield json.loads(line)

def only_purchases(events: Iterable[dict]) -> Iterator[dict]:
    yield from (e for e in events if e.get("type") == "purchase")

def to_rows(events: Iterable[dict]) -> Iterator[tuple]:
    for e in events:
        yield (e["event_id"], e["user_id"], float(e["amount"]), e["ts"])

paths = sorted(Path("landing/2026-03-01").glob("*.jsonl*"))
for chunk in batched(to_rows(only_purchases(parse(read_files(paths)))), 5_000):
    print(f"writing {len(chunk)} rows")          # e.g. executemany / write Parquet part
```

**Real-world use case**

A nightly job processed 400 gzipped JSONL files (about 60 GB uncompressed) on a 4 GB container. The generator pipeline with 5,000-row batches kept memory under 300 MB, and malformed lines were diverted to a quarantine file in the parse stage.

**Common mistakes and red flags**

- Materialising lists between stages.
- One giant generator doing everything.
- No error strategy for bad records mid-stream.

**Follow-up questions**

- What is `itertools.batched`? — Python 3.12+ helper yielding tuples of size n (see [Q395](/interview/python/#q395)).
- How would you parallelise this? — Split by file across processes, each running the pipeline.

**In depth:** [What is a generator and why can it help with large datasets?](/interview/python/generators-large-datasets/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q357" data-level="Intermediate">
<summary><span class="qb-num">Q357</span><h3 class="qb-title">Decorators (with arguments and functools.wraps)</h3><span class="qb-meta">Intermediate · Very Common · Functions</span></summary>

**Short answer**

A decorator is a function that takes a function and returns a new function with added behaviour, such as timing, logging, retries, caching or auth checks. It's applied with `@decorator`. A decorator with arguments, like `@retry(times=3)`, is a factory: it returns the actual decorator. Always use `functools.wraps` so the wrapped function keeps its name, docstring and signature metadata.

**Detailed explanation**

**Mechanics**
- `@timed` above `def f(): ...` is the same as `f = timed(f)`.
- The wrapper usually accepts `*args, **kwargs` (see [Q359](/interview/python/#q359)), calls the original, and returns its result.
- **Closures:** the wrapper "remembers" variables from the enclosing scope (the decorator's arguments, the wrapped function).

**With arguments (three levels)**
```text
def retry(times):            # factory, receives decorator args
    def decorator(func):     # actual decorator
        @wraps(func)
        def wrapper(*a, **kw):   # replaces func
            ...
        return wrapper
    return decorator
```

**`functools.wraps`:** copies `__name__`, `__doc__`, `__module__` and `__qualname__`, and sets `__wrapped__`. Without it, logs, stack traces, Airflow task names and documentation tools show "wrapper" everywhere, and `inspect.signature` misreports the signature.

**Useful built-in decorators:** `@functools.lru_cache` / `@cache` (memoisation; see [Q393](/interview/python/#q393)), `@property`, `@classmethod`, `@staticmethod` (see [Q361](/interview/python/#q361)), `@dataclass` (see [Q362](/interview/python/#q362)), `@contextlib.contextmanager` (see [Q358](/interview/python/#q358)).

**DE uses:** retrying flaky API or DB calls (see [Q379](/interview/python/#q379)), timing and logging task steps (see [Q380](/interview/python/#q380)), validating inputs, registering transforms in a plugin registry, and Airflow's TaskFlow `@task` / `@dag`.

**Cautions**
- Stacking order matters. `@a @b def f` means `a(b(f))`.
- Decorators on methods receive `self` in `args`.
- Async functions need async wrappers (`async def wrapper` with `await func(...)`).

**Example**

```python
# Runtime: Python 3.12
import functools
import logging
import time

log = logging.getLogger(__name__)

def timed(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            log.info("%s took %.3fs", func.__name__, time.perf_counter() - start)
    return wrapper

def retry(times: int = 3, delay_s: float = 1.0, exceptions: tuple[type[Exception], ...] = (ConnectionError,)):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, times + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    if attempt == times:
                        raise
                    time.sleep(delay_s * attempt)
        return wrapper
    return decorator

@timed
@retry(times=4, delay_s=0.5)
def fetch_rates(day: str) -> dict:
    """Fetch FX rates for a day."""
    return {"USD": 1.0, "EUR": 0.92}

print(fetch_rates.__name__, fetch_rates.__doc__)   # fetch_rates  Fetch FX rates for a day.
```

**Real-world use case**

A team wrapped all external calls with a shared `@retry` decorator that only retries transient error types, plus `@timed` logging. Retry behaviour became consistent across 30 connectors, and the slow endpoints showed up clearly in the logs.

**Common mistakes and red flags**

- Forgetting `wraps`.
- Retrying all exceptions, including bugs.
- Not returning the wrapped function's result.

**Follow-up questions**

- Can classes be decorators? — Yes, via `__call__`; and decorators can decorate classes.
- How do you access the original function? — `decorated.__wrapped__` (set by `wraps`).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q358" data-level="Intermediate">
<summary><span class="qb-num">Q358</span><h3 class="qb-title">Context managers and the with statement</h3><span class="qb-meta">Intermediate · Very Common · Resource Management</span></summary>

**Short answer**

A context manager sets something up on entry and guarantees cleanup on exit, even when an exception occurs: closing files, releasing connections, committing or rolling back transactions, releasing locks, deleting temp files. Use it with `with`. Write one as a class with `__enter__`/`__exit__`, or more simply with `@contextlib.contextmanager` and a `try/finally` around `yield`.

**Detailed explanation**

**Protocol**
- `__enter__(self)` runs at the start of the `with` block. Its return value is bound by `as`.
- `__exit__(self, exc_type, exc, tb)` always runs. If it returns True, it suppresses the exception (rarely wanted).
- Async versions are `__aenter__`/`__aexit__` with `async with`, for example async HTTP sessions (see [Q369](/interview/python/#q369)).

**Common context managers**
| Use | Example |
|---|---|
| Files | `with open(path) as f:` |
| DB transactions | `with conn:` (sqlite3/psycopg commit on success, rollback on error), SQLAlchemy `with engine.begin() as conn:` (see [Q397](/interview/python/#q397)) |
| Locks | `with threading.Lock():` |
| Temp resources | `tempfile.TemporaryDirectory()` |
| Suppress specific errors | `contextlib.suppress(FileNotFoundError)` |
| Multiple resources | `contextlib.ExitStack` (dynamic number of resources) |
| Timing/tracing spans | custom (OpenTelemetry spans) |

**`@contextmanager`**
```text
@contextmanager
def resource():
    setup()
    try:
        yield value
    finally:
        cleanup()
```
Without `try/finally`, cleanup doesn't run on errors.

**Why it matters in pipelines**
- Leaked connections exhaust database pools. Leaked file handles hit OS limits in long-running workers.
- Transaction scoping ensures partial writes are rolled back on failure, which is part of idempotency (see [Q399](/interview/python/#q399)).
- Cleanup of temp staging files and locks prevents stuck reruns.

**Example**

```python
# Runtime: Python 3.12
import logging
import shutil
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

log = logging.getLogger(__name__)

@contextmanager
def staging_dir(prefix: str = "etl_"):
    path = Path(tempfile.mkdtemp(prefix=prefix))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)

@contextmanager
def step(name: str):
    start = time.perf_counter()
    log.info("start %s", name)
    try:
        yield
        log.info("done %s in %.2fs", name, time.perf_counter() - start)
    except Exception:
        log.exception("failed %s after %.2fs", name, time.perf_counter() - start)
        raise

with step("export orders"), staging_dir() as tmp:
    out = tmp / "orders.csv"
    out.write_text("order_id,amount\n1,10.5\n")
    print(out.exists())        # True; directory removed after block, even on error
```

**Real-world use case**

A long-running ingestion service opened database connections without closing them on exceptions, and the pool was exhausted after a few hours of upstream errors. Wrapping connection use in context managers removed the leaks.

**Common mistakes and red flags**

- `@contextmanager` without try/finally.
- Swallowing exceptions in `__exit__`.
- Manual open/close without exception safety.

**Follow-up questions**

- What does `ExitStack` solve? — Managing a variable number of context managers (e.g. opening N files) safely.
- Does `with conn:` close a sqlite3 connection? — No — it commits/rolls back the transaction; close separately.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q359" data-level="Beginner">
<summary><span class="qb-num">Q359</span><h3 class="qb-title">*args and **kwargs</h3><span class="qb-meta">Beginner · Very Common · Functions</span></summary>

**Short answer**

In a function definition, `*args` collects extra positional arguments into a tuple, and `**kwargs` collects extra keyword arguments into a dict. In a call, `*seq` unpacks a sequence into positional arguments and `**mapping` unpacks a dict into keyword arguments. They're used in wrappers and decorators, flexible APIs and forwarding arguments. A bare `*` forces the arguments after it to be keyword-only, and `/` forces those before it to be positional-only.

**Detailed explanation**

**Parameter order:** `def f(pos_only, /, normal, *args, kw_only, **kwargs)`.

| Syntax | Meaning |
|---|---|
| `*args` in def | Extra positionals → tuple |
| `**kwargs` in def | Extra keywords → dict |
| `*` alone in def | Following params are keyword-only |
| `/` in def (3.8+) | Preceding params are positional-only |
| `f(*items)` | Unpack sequence |
| `f(**options)` | Unpack mapping |
| `[*a, *b]`, `{**d1, **d2}` | Merge sequences/dicts (later keys win) |

**Good uses**
- **Decorators and wrappers** that forward any arguments (see [Q357](/interview/python/#q357)).
- **Thin adapters** around library calls, passing configuration dicts: `client.get(url, **request_options)`.
- **Keyword-only flags** for clarity: `def load(path, *, overwrite=False)`. Callers must write `overwrite=True`, which prevents positional mistakes.

**Overuse warnings**
- `def run(**kwargs)` everywhere hides the real interface. IDEs and type checkers can't help, and typos in keys pass silently. Prefer explicit parameters, or a typed config object (dataclass or Pydantic; see [Q362](/interview/python/#q362)).
- Typing: `*args: int` means each positional is int, and `**kwargs: str` means each value is str. For precise kwargs typing, use `Unpack[TypedDict]` (PEP 692).

**Rule of thumb:** use `*args`/`**kwargs` when you're genuinely forwarding arguments you don't need to understand (wrappers, adapters). When you're the final consumer, spell the parameters out.

**Example**

```python
# Runtime: Python 3.12
from typing import Any

def build_query(table: str, /, *columns: str, where: str | None = None, limit: int | None = None) -> str:
    cols = ", ".join(columns) or "*"
    sql = f"SELECT {cols} FROM {table}"
    if where:
        sql += f" WHERE {where}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return sql

def log_call(func, *args: Any, **kwargs: Any):
    print(f"calling {func.__name__} args={args} kwargs={kwargs}")
    return func(*args, **kwargs)

defaults = {"limit": 100}
overrides = {"where": "status = 'paid'"}
print(log_call(build_query, "orders", "order_id", "amount", **{**defaults, **overrides}))
# SELECT order_id, amount FROM orders WHERE status = 'paid' LIMIT 100
```
This builder is for trusted, code-defined identifiers and conditions only. Values that come from users must go through parameterised queries (see [Q397](/interview/python/#q397)).

**Real-world use case**

A loader function had the signature `load(df, table, True, False, 500)`, and positional booleans caused a production overwrite when the arguments were swapped. Changing it to `load(df, table, *, overwrite=False, create=False, batch_size=500)` made calls self-documenting.

**Common mistakes and red flags**

- `**kwargs` hiding typos.
- Positional boolean flags.
- Forgetting that dict-merge order determines which value wins.

**Follow-up questions**

- What type is `args`? — tuple; `kwargs` is dict.
- Why positional-only params? — Freedom to rename them later without breaking callers; matches C-implemented built-ins.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q360" data-level="Beginner">
<summary><span class="qb-num">Q360</span><h3 class="qb-title">OOP: inheritance vs composition</h3><span class="qb-meta">Beginner · Common · Object-Oriented Design</span></summary>

**Short answer**

Inheritance ("is-a") makes a subclass reuse and extend a base class's behaviour, for example `PostgresSource(BaseSource)`. Composition ("has-a") builds objects from other objects passed in or held as attributes: a `Pipeline` has a source, a transformer and a sink. Prefer composition for flexibility and testability. Use inheritance for real type hierarchies and shared interfaces, ideally through abstract base classes or Protocols.

**Detailed explanation**

| Aspect | Inheritance | Composition |
|---|---|---|
| Relationship | is-a | has-a / uses-a |
| Coupling | Tight (subclass depends on base internals) | Loose (depends on interfaces) |
| Changing behaviour | Override methods; deep hierarchies get fragile | Swap components at runtime |
| Testing | Need to construct whole hierarchy | Inject fakes/mocks easily (see [Q383](/interview/python/#q383)) |
| Typical use | Shared interface + small variations (connectors implementing `read()`) | Pipelines, services, strategies, clients |

**Interfaces in Python**
- **ABCs** (`abc.ABC`, `@abstractmethod`): enforce that subclasses implement methods, failing at instantiation time.
- **`typing.Protocol`:** structural typing ("anything with `read()` and `close()`"). No inheritance needed, and type checkers verify it (see [Q385](/interview/python/#q385)).
- **Duck typing:** just call the method. Simple, but errors surface at runtime.

**Design guidance for DE code**
- Keep hierarchies **shallow** (one level of abstract base plus concrete classes).
- **Inject dependencies** (clients, configs) through constructors rather than creating them inside, which helps testing.
- Avoid "god" base classes with dozens of hooks.
- Multiple inheritance and mixins: use them sparingly, and understand the MRO (method resolution order).
- Often a few plain functions plus dataclasses beat classes entirely. Don't use OOP for its own sake.

**Example**

```python
# Runtime: Python 3.12
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Iterator, Protocol

class Source(ABC):                                  # inheritance for a shared interface
    @abstractmethod
    def read(self) -> Iterator[dict]: ...

class CsvSource(Source):
    def __init__(self, rows: list[dict]):
        self._rows = rows
    def read(self) -> Iterator[dict]:
        yield from self._rows

class Sink(Protocol):                               # structural interface
    def write(self, records: Iterable[dict]) -> int: ...

class MemorySink:
    def __init__(self):
        self.records: list[dict] = []
    def write(self, records: Iterable[dict]) -> int:
        batch = list(records); self.records.extend(batch); return len(batch)

@dataclass
class Pipeline:                                     # composition: has a source and a sink
    source: Source
    sink: Sink
    def run(self) -> int:
        cleaned = ({**r, "email": r["email"].lower()} for r in self.source.read())
        return self.sink.write(cleaned)

sink = MemorySink()
print(Pipeline(CsvSource([{"email": "A@X.COM"}]), sink).run(), sink.records)  # 1 [{'email': 'a@x.com'}]
```

**Real-world use case**

A connector framework had a 5-level inheritance tree, and changing a base method broke 20 connectors. Refactoring to composition (a Pipeline with injected Source, Transform and Sink objects behind Protocols) let teams add connectors without touching shared base classes, and unit tests used in-memory sinks.

**Common mistakes and red flags**

- Deep inheritance for code reuse.
- Creating clients inside classes, which makes them untestable.
- Classes where functions would do.

**Follow-up questions**

- What's MRO? — The order Python searches classes for attributes (C3 linearization); `Class.__mro__`.
- ABC vs Protocol? — ABC requires explicit inheritance and runtime enforcement; Protocol is structural and checked statically.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q361" data-level="Beginner">
<summary><span class="qb-num">Q361</span><h3 class="qb-title">Instance vs @classmethod vs @staticmethod</h3><span class="qb-meta">Beginner · Common · Object-Oriented Design</span></summary>

**Short answer**

Instance methods receive `self` and work with one object's state. Class methods (`@classmethod`) receive `cls` and work at class level. They're mostly used as alternative constructors (`Config.from_env()`, `Record.from_json()`) and respect subclassing. Static methods (`@staticmethod`) receive neither. They're plain functions grouped in the class namespace for organisation.

**Detailed explanation**

| Type | First arg | Access instance state | Access class | Typical use |
|---|---|---|---|---|
| Instance method | `self` | Yes | Via `type(self)` | Behaviour of an object |
| `@classmethod` | `cls` | No | Yes | Factories/alternative constructors, class-level registries |
| `@staticmethod` | — | No | No | Utility tied conceptually to class |

**Why classmethod factories**
- They return `cls(...)`, so **subclasses get instances of themselves**. `SnowflakeConfig.from_env()` returns a `SnowflakeConfig`, not the base class.
- They keep construction logic (parsing env vars, files, dicts) out of `__init__`, so `__init__` stays simple and testable with explicit values.

**Static methods: use sparingly**
- Fine for small helpers tied to the class's domain (`Money.round_half_even(x)`).
- Often a module-level function is clearer and easier to import and test. Python isn't Java, and not everything needs a class.

**Related:** `@property` gives computed attributes accessed without parentheses (`order.net_amount`), and `functools.cached_property` computes once per instance.

**Interview angle:** expect a follow-up asking which method type fits a scenario. For example, "parse a config from a YAML file" is a classmethod factory, "validate an identifier format" is a static helper or module function, and "compute this record's net amount" is an instance method or property. Explaining *why*, especially how subclassing works with `cls`, shows real understanding rather than memorised definitions.

**Example**

```python
# Runtime: Python 3.12
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class WarehouseConfig:
    account: str
    warehouse: str
    database: str
    role: str = "TRANSFORMER"

    @classmethod
    def from_env(cls, prefix: str = "SF_") -> "WarehouseConfig":
        return cls(
            account=os.environ[f"{prefix}ACCOUNT"],
            warehouse=os.environ.get(f"{prefix}WAREHOUSE", "TRANSFORM_XS"),
            database=os.environ[f"{prefix}DATABASE"],
        )

    @staticmethod
    def normalise_identifier(name: str) -> str:
        return name.strip().upper()

    def fully_qualified(self, schema: str, table: str) -> str:          # instance method
        n = self.normalise_identifier
        return f"{n(self.database)}.{n(schema)}.{n(table)}"

os.environ.update({"SF_ACCOUNT": "acme-xy123", "SF_DATABASE": "analytics"})
cfg = WarehouseConfig.from_env()
print(cfg.fully_qualified("marts", "fct_orders"))      # ANALYTICS.MARTS.FCT_ORDERS
```

**Real-world use case**

A config class's `__init__` read environment variables directly, so tests had to set dozens of env vars. Moving that logic to `from_env()` let tests construct configs explicitly, while production code kept calling `from_env()`.

**Common mistakes and red flags**

- Static methods that need class info.
- `__init__` doing I/O.
- Classes full of static methods that should be a module.

**Follow-up questions**

- Why not just use a module function as factory? — Fine too; classmethods integrate with subclassing.
- What does `@property` add? — Attribute-style access with computed/validated values.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q362" data-level="Intermediate">
<summary><span class="qb-num">Q362</span><h3 class="qb-title">dataclasses vs Pydantic models</h3><span class="qb-meta">Intermediate · Common · Data Modelling</span></summary>

**Short answer**

`dataclasses` (standard library) generate `__init__`, `__repr__` and `__eq__` for plain data containers. They're fast and dependency-free, but don't validate or convert types at runtime. Pydantic (v2, with a Rust core) validates and coerces input data, parses nested JSON, produces clear error messages and generates JSON Schema. Use dataclasses for internal structures and Pydantic at system boundaries: API payloads, configs, record validation.

**Detailed explanation**

| Aspect | `dataclasses` | Pydantic v2 `BaseModel` |
|---|---|---|
| Type enforcement | None at runtime (hints only) | Validates/coerces on creation |
| Nested parsing | Manual | Automatic from dicts/JSON |
| Errors | Your own checks | Structured `ValidationError` with field paths |
| Serialization | `asdict()` | `model_dump()`, `model_dump_json()`, JSON Schema |
| Performance | Very fast creation | Fast (Rust core) but heavier than dataclasses |
| Immutability | `frozen=True` | `model_config = ConfigDict(frozen=True)` |
| Dependencies | stdlib | third-party |
| Extras | `slots=True`, `kw_only=True`, `field(default_factory=...)` | Validators, `Field` constraints, aliases, settings (`pydantic-settings`) |

**When to use which**
- **Pydantic:**
  - Validating API responses before loading, so you fail fast on contract changes (see [Q38](/interview/data-engineering/#q38)).
  - Config from env and files (`pydantic-settings`; see [Q381](/interview/python/#q381)).
  - Request and response models in FastAPI.
  - Record-level validation that quarantines bad rows (see [Q391](/interview/python/#q391)).
- **dataclasses:** internal structures passed between your own functions, hot loops creating millions of objects, and libraries avoiding dependencies.
- **Neither, for bulk data:** millions of rows should live in DataFrames or Arrow, validated with columnar tools such as pandera, Great Expectations or warehouse tests. Per-row Python objects are slow.

**Gotchas**
- Dataclass mutable defaults need `field(default_factory=list)` (see [Q352](/interview/python/#q352)).
- Pydantic **coercion** may hide problems (`"123"` becomes 123). Use strict mode (`ConfigDict(strict=True)`) where exact types matter.
- Pydantic v1 → v2 changed the APIs (`dict()` became `model_dump()`, validators became `field_validator`).

**Example**

```python
# Runtime: Python 3.12 with pydantic 2.x
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, Field, ValidationError, field_validator

@dataclass(slots=True)
class BatchStats:                       # internal, no validation needed
    source: str
    rows: int = 0
    errors: list[str] = field(default_factory=list)

class OrderEvent(BaseModel):            # boundary: validate external payloads
    order_id: str = Field(min_length=1)
    amount: Decimal = Field(ge=0, max_digits=12, decimal_places=2)
    currency: str
    created_at: datetime

    @field_validator("currency")
    @classmethod
    def iso_currency(cls, v: str) -> str:
        v = v.upper()
        if len(v) != 3:
            raise ValueError("currency must be a 3-letter ISO code")
        return v

stats = BatchStats(source="orders_api")
for payload in [{"order_id": "o1", "amount": "19.99", "currency": "usd", "created_at": "2026-03-01T10:00:00Z"},
                {"order_id": "", "amount": "-5", "currency": "EURO", "created_at": "bad"}]:
    try:
        OrderEvent.model_validate(payload); stats.rows += 1
    except ValidationError as e:
        stats.errors.append(f"{payload.get('order_id')!r}: {e.error_count()} errors")
print(stats)   # BatchStats(source='orders_api', rows=1, errors=["'': 4 errors"])
```

**Real-world use case**

A partner API silently started returning `amount` as a formatted string ("1,234.50"). Pydantic validation at ingestion rejected those records with a clear field-level error, instead of loading NULLs or crashing deep inside a transform.

**Common mistakes and red flags**

- Believing dataclass type hints are enforced.
- Pydantic objects for millions of rows.
- Lax coercion hiding contract breaks.

**Follow-up questions**

- What's `slots=True`? — Uses `__slots__` for lower memory and faster attribute access.
- Alternatives? — `attrs`, `msgspec` (very fast validation/serialization), TypedDict for typing dicts.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q363" data-level="Intermediate">
<summary><span class="qb-num">Q363</span><h3 class="qb-title">Useful dunder methods</h3><span class="qb-meta">Intermediate · Occasional · Object-Oriented Design</span></summary>

**Short answer**

Dunder ("double underscore") methods plug your classes into Python's built-in behaviour. Common ones: `__init__` (initialise), `__repr__`/`__str__` (display), `__eq__`/`__hash__` (equality and use in sets and dicts), `__lt__` (sorting), `__len__`, `__iter__`, `__getitem__` and `__contains__` (container behaviour), `__enter__`/`__exit__` (context managers), and `__call__` (callable objects).

**Detailed explanation**

| Method | Enables | Notes |
|---|---|---|
| `__init__` / `__new__` | Construction | `__new__` rarely needed (immutables, singletons) |
| `__repr__` | Unambiguous debug display | Should ideally look like a constructor call; used in logs |
| `__str__` | User-friendly `str()` | Falls back to `__repr__` |
| `__eq__` / `__hash__` | `==`, set/dict membership | If you define `__eq__`, define `__hash__` consistently (or object becomes unhashable); hash only immutable fields |
| `__lt__` etc. | Sorting/comparison | `functools.total_ordering` fills the rest |
| `__len__`, `__iter__`, `__getitem__`, `__contains__` | Container protocols | Makes objects work with `len`, `for`, indexing, `in` |
| `__enter__` / `__exit__` | `with` statement (see [Q358](/interview/python/#q358)) | Resource cleanup |
| `__call__` | Instances callable like functions | Configurable transforms/strategies |
| `__bool__` | Truthiness | e.g. empty batch is falsy |
| `__add__`, `__radd__` | Operators | Value objects (Money) |

**Practical guidance**
- **Dataclasses** auto-generate `__init__`, `__repr__` and `__eq__`. With `frozen=True` (and the default `eq=True`) they also get `__hash__`, and with `order=True` comparisons. That's usually better than hand-writing them (see [Q362](/interview/python/#q362)).
- A **good `__repr__`** pays off in debugging, logs and test failure messages.
- Avoid clever operator overloading that surprises readers.
- Consistency: objects that are equal must have equal hashes.

**Example**

```python
# Runtime: Python 3.12
from functools import total_ordering
from typing import Iterator

@total_ordering
class Version:
    def __init__(self, text: str):
        self.parts = tuple(int(p) for p in text.split("."))
    def __repr__(self) -> str:
        return f"Version('{'.'.join(map(str, self.parts))}')"
    def __eq__(self, other: object) -> bool:
        return isinstance(other, Version) and self.parts == other.parts
    def __hash__(self) -> int:
        return hash(self.parts)
    def __lt__(self, other: "Version") -> bool:
        return self.parts < other.parts

class Batch:
    def __init__(self, records: list[dict]):
        self._records = records
    def __len__(self) -> int: return len(self._records)
    def __iter__(self) -> Iterator[dict]: return iter(self._records)
    def __bool__(self) -> bool: return bool(self._records)
    def __repr__(self) -> str: return f"Batch(n={len(self)})"

print(sorted({Version("1.10.0"), Version("1.9.3"), Version("1.9.3")}))  # [Version('1.9.3'), Version('1.10.0')]
b = Batch([{"id": 1}])
print(b, len(b), bool(Batch([])))                                       # Batch(n=1) 1 False
```
`sorted` compares the integer tuples, so 1.10.0 correctly sorts after 1.9.3. A string sort would get this wrong.

**Real-world use case**

A schema-migration tool compared version strings as text, so "1.10" sorted before "1.9" and migrations ran out of order. A small `Version` class with `__lt__` and `__eq__` fixed the ordering.

**Common mistakes and red flags**

- `__eq__` without a consistent `__hash__`.
- Hashing mutable fields.
- Useless `__repr__` that shows only object addresses.

**Follow-up questions**

- What happens to `__hash__` when you define `__eq__`? — It's set to None (unhashable) unless you define it.
- `__repr__` vs `__str__`? — Developer/debug vs end-user display.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q364" data-level="Beginner">
<summary><span class="qb-num">Q364</span><h3 class="qb-title">Exception handling best practices</h3><span class="qb-meta">Beginner · Very Common · Error Handling</span></summary>

**Short answer**

Catch specific exceptions, never a bare `except:`. Keep `try` blocks small. Handle errors where you can actually do something (retry, skip and quarantine, add context), and let everything else propagate so the job fails visibly. Use `finally` or context managers for cleanup, `raise ... from e` to keep the cause, and `log.exception` to record stack traces. Don't swallow errors silently.

**Detailed explanation**

**Structure**
```text
try:            # only the risky call
    ...
except SpecificError as e:   # handle what you understand
    ...
else:           # runs if no exception (keep happy path out of try)
    ...
finally:        # always runs (cleanup)
    ...
```

**Principles**
1. **Specific over broad.** `except (TimeoutError, ConnectionError)` rather than `except Exception`. A bare `except:` even catches `KeyboardInterrupt` and `SystemExit`.
2. **Fail loudly by default.** A pipeline that "succeeds" after silently skipping errors produces wrong data, which is worse than failing.
3. **Add context and chain.** `raise LoadError(f"failed loading {table}") from e` keeps the original traceback (see [Q365](/interview/python/#q365)).
4. **Log once, at the right level.** Don't log and re-raise at every layer, which duplicates noise. Use `log.exception()` inside `except` to include the traceback.
5. **Classify errors.** Transient errors (timeouts, 429, 5xx) can be retried (see [Q379](/interview/python/#q379)). Data errors (bad record): quarantine and continue, if the business allows (see [Q391](/interview/python/#q391)). Programming errors (KeyError from a bug) must crash.
6. **Clean up** with `with` or `finally`: connections, temp files, locks (see [Q358](/interview/python/#q358)).
7. **Exception groups** (3.11+, `except*`) handle several concurrent failures, for example from `asyncio.TaskGroup` (see [Q369](/interview/python/#q369)).

**Anti-patterns**
| Anti-pattern | Problem |
|---|---|
| `except: pass` | Hides all failures |
| `except Exception as e: print(e)` | Loses traceback, continues with bad state |
| Huge `try` around whole function | Catches unrelated errors |
| Returning None on error silently | Callers can't distinguish "no data" from failure |
| Using exceptions for normal control flow in hot loops | Slow and confusing (but EAFP for dict keys is idiomatic) |

**Example**

```python
# Runtime: Python 3.12
import json
import logging

log = logging.getLogger(__name__)

class SourceDataError(Exception):
    """Raised when an input file cannot be interpreted."""

def load_manifest(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            raw = f.read()
    except FileNotFoundError as e:
        raise SourceDataError(f"manifest missing: {path}") from e

    try:
        manifest = json.loads(raw)
    except json.JSONDecodeError as e:
        raise SourceDataError(f"manifest is not valid JSON: {path} (line {e.lineno})") from e
    else:
        if "files" not in manifest:
            raise SourceDataError(f"manifest has no 'files' key: {path}")
        return manifest

try:
    load_manifest("landing/2026-03-01/_manifest.json")
except SourceDataError:
    log.exception("cannot start load")      # logs traceback with chained cause
    raise SystemExit(2)
```

**Real-world use case**

An ingestion job wrapped each table load in `try/except Exception: continue`. For two weeks, 3 of 40 tables failed silently because a credential had expired, while the job reported success. Replacing that with specific handling plus a final failure status when any table failed restored visibility.

**Common mistakes and red flags**

- Bare except.
- Swallowed errors.
- Losing the original traceback.

**Follow-up questions**

- EAFP vs LBYL? — "Easier to ask forgiveness" (try/except) vs "look before you leap" (checks); EAFP is idiomatic for races like file existence.
- What does `raise` alone do in except? — Re-raises the current exception with original traceback.

**In depth:** [How should exceptions be handled in production data pipelines?](/interview/python/exceptions-in-pipelines/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q365" data-level="Intermediate">
<summary><span class="qb-num">Q365</span><h3 class="qb-title">Custom exceptions and retryable errors</h3><span class="qb-meta">Intermediate · Common · Error Handling</span></summary>

**Short answer**

Define a small exception hierarchy for your pipeline. Typically a base `PipelineError`, with subclasses such as `RetryableError` (timeouts, 429, 5xx, deadlocks) and `PermanentError` / `DataValidationError` (bad credentials, schema mismatch, invalid data). Retry logic and orchestrators can then decide automatically whether to retry, quarantine or fail fast. Attach context (source, batch ID, HTTP status) as attributes.

**Detailed explanation**

**Why classify**
- Retrying permanent errors (401 Unauthorized, 400 Bad Request, schema errors) wastes time and can trigger lockouts or rate limits.
- Not retrying transient errors causes needless failures and pages.
- Clear types make alerts actionable: "schema contract broken" and "provider timeout" call for different responses.

**Typical hierarchy**
```text
PipelineError
 ├─ RetryableError        (network timeouts, 429, 500/502/503/504, DB deadlock/serialization)
 │    └─ RateLimitedError (carries retry_after seconds)
 └─ PermanentError
      ├─ AuthError        (401/403)
      ├─ ConfigError
      └─ DataValidationError (bad payloads; carries record ids/count)
```

**Design tips**
- Inherit from `Exception` (not `BaseException`).
- Add **attributes** (`status_code`, `retry_after`, `source`, `batch_id`) and a helpful message.
- **Translate** library exceptions at boundaries (`requests.HTTPError` → `RetryableError` or `AuthError`), so the rest of the code doesn't depend on library specifics.
- Chain the original with `raise ... from e`.
- **Orchestrator integration:** in Airflow, raising `AirflowFailException` fails a task immediately without retries (for permanent errors), while ordinary exceptions use the configured retries. Similar patterns exist elsewhere (see [Q44](/interview/data-engineering/#q44)).
- Tenacity and similar retry libraries can take `retry_if_exception_type(RetryableError)` (see [Q379](/interview/python/#q379)).

**Alerting tie-in:** map exception types to alert routing. `AuthError` pages the platform team to rotate credentials, `DataValidationError` notifies the data owner with sample record IDs, and exhausted `RetryableError`s open an incident with the provider. Typed errors make that routing a simple lookup instead of fragile log parsing.

**Example**

```python
# Runtime: Python 3.12 with requests 2.x
import requests

class PipelineError(Exception):
    """Base error for the ingestion pipeline."""

class RetryableError(PipelineError):
    def __init__(self, message: str, *, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after

class PermanentError(PipelineError):
    pass

class AuthError(PermanentError):
    pass

RETRYABLE_STATUS = {429, 500, 502, 503, 504}

def get_json(session: requests.Session, url: str, **params) -> dict:
    try:
        resp = session.get(url, params=params, timeout=30)
    except (requests.Timeout, requests.ConnectionError) as e:
        raise RetryableError(f"network error calling {url}") from e

    if resp.status_code in RETRYABLE_STATUS:
        retry_after = resp.headers.get("Retry-After")
        raise RetryableError(f"{resp.status_code} from {url}",
                             retry_after=float(retry_after) if retry_after and retry_after.isdigit() else None)
    if resp.status_code in (401, 403):
        raise AuthError(f"{resp.status_code} from {url}: check credentials/scopes")
    if resp.status_code >= 400:
        raise PermanentError(f"{resp.status_code} from {url}: {resp.text[:200]}")
    return resp.json()
```

**Real-world use case**

An API connector retried 401 errors 10 times with backoff, which locked the service account after too many failed authentications. Classifying 401/403 as `AuthError` (no retry, page immediately) and 429/5xx as retryable fixed both the lockouts and the noisy alerts.

**Common mistakes and red flags**

- Retrying everything.
- String-matching error messages instead of using types.
- Custom exceptions with no context.

**Follow-up questions**

- Why translate library exceptions? — Decouples business logic from HTTP/DB library details and centralises classification.
- How to fail an Airflow task without retry? — Raise `AirflowFailException`.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q366" data-level="Advanced">
<summary><span class="qb-num">Q366</span><h3 class="qb-title">The GIL, and the free-threaded build in Python 3.13+</h3><span class="qb-meta">Advanced · Very Common · Concurrency</span></summary>

**Short answer**

CPython's Global Interpreter Lock (GIL) lets only one thread execute Python bytecode at a time. Threads therefore help with I/O-bound work, because the GIL is released while waiting, but not with CPU-bound pure-Python work, which needs multiprocessing or native extensions. Python 3.13 introduced an optional free-threaded build (PEP 703, `python3.13t`) without a GIL, which was experimental. Python 3.14 made it officially supported but still optional. The default build still has the GIL.

**Detailed explanation**

**Why the GIL exists:** it simplifies memory management (reference counting; see [Q370](/interview/python/#q370)) and C-extension safety by protecting interpreter state.

**Practical effects (standard build)**
| Workload | Threads | Processes | asyncio |
|---|---|---|---|
| I/O-bound (HTTP, DB, S3) | Good (GIL released during I/O) | Works but heavier | Excellent for many concurrent connections |
| CPU-bound pure Python | No speed-up (often slower) | Good (separate interpreters/GILs) | No help |
| CPU-bound in NumPy/pandas/Polars/Arrow | Often good — many C/Rust routines release the GIL | Good | No help |

**Free-threaded CPython**
- Built with `--disable-gil`. Installers provide `python3.13t` / `python3.14t` builds.
- Threads can run Python code in parallel across cores.
- **Trade-offs:** single-thread performance overhead (reduced in 3.14), and C extensions must be rebuilt and declare free-threading support, or the GIL may be re-enabled at runtime for safety. Ecosystem support (NumPy, pandas and others) is growing but not universal.
- **Status:** experimental in 3.13. In 3.14 it moved to "supported but optional" (PEP 779). The default build still has the GIL, and making free-threading the default is a future, staged decision.
- Check with `sys._is_gil_enabled()` (3.13+).

**Other parallelism options**
- `multiprocessing` / `ProcessPoolExecutor` (see [Q367](/interview/python/#q367), [Q368](/interview/python/#q368)).
- **Subinterpreters** (PEP 734, `concurrent.interpreters` in 3.14) give isolated interpreters, each with its own GIL.
- Push heavy computation into vectorised libraries (Polars, NumPy, DuckDB) or distributed engines (Spark).

**Example**

```python
# Runtime: Python 3.12+ (prints GIL status on 3.13+)
import sys
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

def cpu_task(n: int) -> int:
    return sum(i * i for i in range(n))

def bench(executor_cls, workers=4, n=3_000_000):
    start = time.perf_counter()
    with executor_cls(max_workers=workers) as ex:
        list(ex.map(cpu_task, [n] * workers))
    return time.perf_counter() - start

if __name__ == "__main__":
    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    print(f"GIL enabled: {gil}")
    print(f"threads:   {bench(ThreadPoolExecutor):.2f}s")    # ~serial time with GIL
    print(f"processes: {bench(ProcessPoolExecutor):.2f}s")   # ~1/4 on 4+ cores
```

**Real-world use case**

A team parallelised CSV parsing with 16 threads and saw no speed-up, because pure-Python parsing is CPU-bound under the GIL. Switching to a process pool, then to Polars' native multi-threaded CSV reader, cut processing time by about 10 times.

**Common mistakes and red flags**

- "Python can't do concurrency."
- Threads for CPU-bound pure Python.
- Claiming the GIL is removed in all Python 3.13+ installs.

**Follow-up questions**

- Why do NumPy ops scale with threads? — They release the GIL in C loops.
- What breaks in free-threaded builds? — Extensions not marked thread-safe; code relying on implicit GIL atomicity.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q367" data-level="Advanced">
<summary><span class="qb-num">Q367</span><h3 class="qb-title">threading vs multiprocessing vs asyncio</h3><span class="qb-meta">Advanced · Very Common · Concurrency</span></summary>

**Short answer**

Use `threading` (or `ThreadPoolExecutor`) for moderate I/O-bound concurrency with ordinary blocking libraries such as requests, boto3 and DB drivers. Use `multiprocessing` (or `ProcessPoolExecutor`) for CPU-bound work, which sidesteps the GIL with separate processes. Use `asyncio` for very high I/O concurrency (hundreds to thousands of connections) with async libraries (httpx/aiohttp, asyncpg, aioboto3), in a single thread with an event loop.

**Detailed explanation**

| Aspect | threading | multiprocessing | asyncio |
|---|---|---|---|
| Parallelism | Concurrency; CPU parallel only in GIL-releasing code (or free-threaded build) | True parallelism (separate processes) | Concurrency in one thread (cooperative) |
| Best for | Blocking I/O, modest concurrency (tens) | CPU-bound pure Python | Massive I/O concurrency |
| Memory | Shared (cheap) | Separate (data pickled between processes) | Shared, very lightweight tasks |
| Overhead | Thread creation/context switches | Process start + serialization | Minimal per task |
| Shared state | Needs locks; race conditions | Explicit (queues, shared memory, managers) | No preemption mid-statement; still need care across `await` |
| Library requirement | Any | Picklable functions/args | Async-native libraries (blocking calls stall the loop) |
| Failure isolation | Thread crash can affect process | Process crash isolated | One blocked coroutine blocks all |

**Decision guide**
1. **CPU-bound?** Vectorise (pandas, Polars, NumPy) or use processes. Bigger data: Spark or DuckDB.
2. **I/O-bound with existing blocking SDKs** (boto3, Snowflake connector)? A thread pool of 8–32 workers, rate-limited (see [Q368](/interview/python/#q368), [Q379](/interview/python/#q379)).
3. **Thousands of HTTP calls**, or a long-running async service? asyncio with a semaphore for concurrency limits (see [Q369](/interview/python/#q369)).
4. **Mixed:** asyncio with `asyncio.to_thread()` for occasional blocking calls, or processes that each run a thread pool.

**Gotchas**
- **Processes:** on macOS and Windows, the default start method is `spawn`, so guard the entry point with `if __name__ == "__main__":`. Large arguments are pickled, which is expensive. Pass file paths, not DataFrames. Linux's default start method changed from `fork` to `forkserver` in Python 3.14.
- **Threads:** shared mutable state needs locks. Many clients (requests Session, boto3 clients) are thread-safe in specific ways only. Usually one client per thread, or documented thread-safe objects.
- **asyncio:** a single blocking call (`time.sleep`, `requests.get`) freezes the whole event loop.
- **Rate limits:** concurrency must respect API quotas regardless of model.

**Example**

```python
# Runtime: Python 3.12 with boto3 — threads for blocking I/O, processes for CPU work
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import hashlib
import boto3

s3 = boto3.client("s3")    # boto3 low-level clients are generally thread-safe; sessions are not

def object_size(key: str) -> tuple[str, int]:
    head = s3.head_object(Bucket="acme-landing", Key=key)
    return key, head["ContentLength"]

def sha256_of_file(path: str) -> tuple[str, str]:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return path, h.hexdigest()

if __name__ == "__main__":
    keys = [f"orders/2026-03-01/part-{i:04d}.parquet" for i in range(200)]
    with ThreadPoolExecutor(max_workers=16) as pool:                  # I/O-bound
        sizes = dict(pool.map(object_size, keys))

    files = ["/data/export_1.bin", "/data/export_2.bin"]
    with ProcessPoolExecutor() as pool:                               # CPU-bound hashing
        digests = dict(pool.map(sha256_of_file, files))
```
`hashlib` releases the GIL for large updates, so threads would also scale here. Processes are shown to illustrate CPU-bound offloading.

**Real-world use case**

Checking metadata for 50,000 S3 objects sequentially took 70 minutes. A 32-thread pool cut it to 3 minutes. A later rewrite with aioboto3 and a 200-connection semaphore did it in 40 seconds, within S3 request limits.

**Common mistakes and red flags**

- Using asyncio with blocking libraries.
- Processes for I/O-bound tasks with huge pickled payloads.
- Unbounded concurrency hammering APIs.

**Follow-up questions**

- How do you share data between processes? — Queues, pipes, `multiprocessing.shared_memory`, or files/object storage.
- What's `asyncio.to_thread`? — Runs a blocking function in a thread without blocking the event loop.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q368" data-level="Intermediate">
<summary><span class="qb-num">Q368</span><h3 class="qb-title">concurrent.futures executors</h3><span class="qb-meta">Intermediate · Common · Concurrency</span></summary>

**Short answer**

`concurrent.futures` gives one high-level API for thread and process pools. `ThreadPoolExecutor` and `ProcessPoolExecutor` have `submit(fn, *args)`, which returns a `Future`, and `map(fn, iterable)`, which returns results in input order. `as_completed(futures)` yields results as they finish. Futures carry results or exceptions (`future.result()` re-raises), so you can collect per-item errors cleanly.

**Detailed explanation**

**Key APIs**
| API | Behaviour |
|---|---|
| `Executor(max_workers=n)` as context manager | Pool shuts down and waits on exit |
| `submit(fn, *args, **kwargs)` | Schedule one call → `Future` |
| `map(fn, *iterables, timeout=None, chunksize=1)` | Results in input order; raises first exception when iterated; `chunksize` matters for processes |
| `as_completed(fs, timeout)` | Iterate futures as they complete (fastest first) |
| `wait(fs, return_when=FIRST_EXCEPTION/ALL_COMPLETED)` | Block until condition |
| `future.result(timeout)` / `.exception()` / `.cancel()` | Get result/exception, attempt cancel |
| `executor.shutdown(wait=True, cancel_futures=True)` | Stop accepting work, optionally cancel pending |

**Patterns**
- **Per-item error handling:** `submit` plus `as_completed`, then `try: f.result() except ...`. Record failures without stopping other work, and fail the job at the end if any failed (see [Q391](/interview/python/#q391)).
- **Bounded memory with huge inputs:** don't `submit` a million tasks at once. Submit in windows (keep at most N futures in flight), or batch items (see [Q395](/interview/python/#q395)).
- **Choosing max_workers:** threads for I/O, roughly 4–32 depending on latency and API limits. Processes for CPU, about `os.cpu_count()`.
- **Timeouts:** use per-call timeouts inside functions (HTTP timeouts). Executor timeouts don't kill running threads.

**Cautions**
- Exceptions in futures are silent until you call `result()` or `exception()`. Always check them.
- Process pools need picklable functions (top-level defs, not lambdas) and a `__main__` guard.
- Thread pools inside web servers or Lambda: watch the lifecycle and resource limits.

**Example**

```python
# Runtime: Python 3.12
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

log = logging.getLogger(__name__)

def load_table(table: str) -> int:
    if table == "legacy_events":
        raise RuntimeError("source view missing")
    return {"orders": 1200, "customers": 300}.get(table, 0)

tables = ["orders", "customers", "legacy_events", "refunds"]
results, failures = {}, {}

with ThreadPoolExecutor(max_workers=4) as pool:
    futures = {pool.submit(load_table, t): t for t in tables}
    for fut in as_completed(futures):
        table = futures[fut]
        try:
            results[table] = fut.result()
        except Exception as exc:               # record and continue with other tables
            failures[table] = repr(exc)
            log.error("load failed for %s: %s", table, exc)

print(results, failures)
if failures:
    raise SystemExit(f"{len(failures)} table(s) failed: {sorted(failures)}")
```
The broad `except Exception` is deliberate here. Each table's failure is recorded with full detail, and the job still fails overall at the end, so nothing is swallowed.

**Real-world use case**

A loader for 120 tables ran sequentially for 2 hours, and one failure stopped all the remaining tables. With an 8-worker pool and per-table error collection, it finished in 20 minutes and reported every failure in one run.

**Common mistakes and red flags**

- Never calling `result()`, so errors are lost.
- Submitting millions of futures.
- Lambdas with process pools.

**Follow-up questions**

- `map` vs `as_completed`? — Ordered results vs fastest-first handling and per-item errors.
- Can you cancel running tasks? — Only pending ones; running threads can't be killed safely.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q369" data-level="Advanced">
<summary><span class="qb-num">Q369</span><h3 class="qb-title">Concurrent API calls with asyncio</h3><span class="qb-meta">Advanced · Common · Concurrency</span></summary>

**Short answer**

Use an async HTTP client (httpx.AsyncClient or aiohttp) with one shared session, and limit concurrency with an `asyncio.Semaphore`. Run the tasks with `asyncio.TaskGroup` (3.11+) or `asyncio.gather`, add timeouts, and retry transient errors with backoff. Make sure nothing in the coroutines blocks the event loop, and offload unavoidable blocking calls with `asyncio.to_thread`.

**Detailed explanation**

**Building blocks**
- `async def` defines coroutines. `await` yields control while waiting on I/O.
- `asyncio.run(main())` is the entry point.
- **TaskGroup** (`async with asyncio.TaskGroup() as tg: tg.create_task(...)`) gives structured concurrency. If one task fails, the others are cancelled, and errors are raised as an `ExceptionGroup` (handled with `except*`).
- **gather(..., return_exceptions=True)** collects results and exceptions without cancelling the other tasks.
- **Semaphore** caps in-flight requests, to respect rate limits and avoid exhausting connections.
- **Timeouts:** the client timeout plus `asyncio.timeout()` (3.11+) around operations.
- **Connection pooling:** reuse one client (`httpx.AsyncClient(limits=httpx.Limits(max_connections=50))`).

**Rate limiting**
- A semaphore limits concurrency, not requests per second. For RPS limits, add a token bucket or a library such as `aiolimiter` (see [Q379](/interview/python/#q379)).
- Honour `Retry-After` on 429 responses.

**When asyncio is worth it**
- Thousands of small HTTP calls (enrichment, webhooks, paginated fan-out).
- Services handling many concurrent connections.
- Not for CPU-bound work, and not if your main SDKs are blocking-only (use threads instead).

**Pitfalls**
- Calling `requests.get` or `time.sleep` inside coroutines blocks everything.
- Creating a new client per request (no pooling, slow TLS handshakes).
- Unbounded `gather` over 100,000 tasks, which uses lots of memory and overwhelms servers. Process in chunks.
- Running asyncio inside environments that already have a loop (Jupyter). Use `await main()` directly there.

**Example**

```python
# Runtime: Python 3.12 with httpx 0.27+
import asyncio
import random
import httpx

API = "https://api.partner.example/v1/customers/{id}"
MAX_CONCURRENCY = 20

async def fetch_customer(client: httpx.AsyncClient, sem: asyncio.Semaphore, cid: int) -> dict:
    for attempt in range(1, 5):
        async with sem:
            try:
                resp = await client.get(API.format(id=cid))
            except (httpx.TimeoutException, httpx.TransportError):
                resp = None
        if resp is not None and resp.status_code == 200:
            return resp.json()
        if resp is not None and resp.status_code not in (429, 500, 502, 503, 504):
            resp.raise_for_status()
        delay = float(resp.headers.get("Retry-After", 0)) if resp is not None else 0
        await asyncio.sleep(max(delay, (2 ** attempt) * 0.25 + random.random() * 0.25))
    raise RuntimeError(f"customer {cid}: retries exhausted")

async def main(ids: list[int]) -> list[dict]:
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0),
                                 limits=httpx.Limits(max_connections=MAX_CONCURRENCY)) as client:
        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(fetch_customer(client, sem, i)) for i in ids]
    return [t.result() for t in tasks]

if __name__ == "__main__":
    customers = asyncio.run(main(list(range(1, 501))))
    print(len(customers))
```
The sleep happens outside the semaphore, so a waiting retry doesn't hold a concurrency slot. Non-numeric `Retry-After` values (HTTP dates) would need parsing, which is omitted here for brevity.

**Real-world use case**

Enriching 200,000 records via a partner API took 9 hours sequentially. With asyncio, a 25-request concurrency cap matching the partner's limit, and backoff on 429, it took 25 minutes, with zero rate-limit violations after tuning.

**Common mistakes and red flags**

- Blocking calls in coroutines.
- No concurrency cap.
- New client per request.

**Follow-up questions**

- TaskGroup vs gather? — TaskGroup cancels siblings on failure and raises ExceptionGroup; gather can return exceptions as results.
- How do you call a blocking SDK from asyncio? — `await asyncio.to_thread(fn, *args)`.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q370" data-level="Advanced">
<summary><span class="qb-num">Q370</span><h3 class="qb-title">Memory management: reference counting and garbage collection</h3><span class="qb-meta">Advanced · Common · Internals</span></summary>

**Short answer**

CPython frees most objects immediately through reference counting: when an object's refcount drops to zero, its memory is released. A generational cyclic garbage collector (`gc` module) periodically finds reference cycles that refcounting can't free. Memory "leaks" in Python are usually lingering references (global caches, growing lists, closures) rather than GC failures. Measure with `tracemalloc` and similar tools.

**Detailed explanation**

**Reference counting**
- Every object tracks how many references point to it. `del x`, reassigning names, or leaving a scope decrements the count.
- At zero, the object is freed right away, which gives deterministic cleanup, for example closing files when the last reference goes. Still use `with` for clarity (see [Q358](/interview/python/#q358)).
- `sys.getrefcount(obj)` shows the count (+1 for the call argument).

**Cyclic GC**
- Cycles (`a.ref = b; b.ref = a`) never reach zero on their own.
- The generational GC (generations 0, 1 and 2) tracks container objects and collects unreachable cycles. Thresholds come from `gc.get_threshold()`. Python 3.14 changed to an incremental collector design.
- `gc.collect()` forces a collection. `gc.disable()` is sometimes used in short-lived, allocation-heavy batch jobs for speed, which is risky if cycles exist.
- `weakref` lets you reference objects without keeping them alive, which is useful for caches.

**Memory returned to the OS:** CPython's allocator (pymalloc) reuses freed memory internally. Process RSS may not shrink after large objects are freed, especially with fragmentation. For bulk data jobs, process data in chunks (see [Q374](/interview/python/#q374), [Q377](/interview/python/#q377)), or run work in subprocesses that exit.

**Common sources of growth in DE code**
| Cause | Fix |
|---|---|
| Accumulating all records in a list | Stream with generators; write in batches |
| Unbounded caches (`@lru_cache(maxsize=None)` on high-cardinality args) | Bound size; TTL caches |
| pandas intermediate copies | Chain operations carefully, `del` big frames, use appropriate dtypes |
| Global module-level state in long-running workers | Reset per task; avoid globals |
| Exceptions holding tracebacks referencing large frames | Don't store exceptions long-term |

**Tools:** `tracemalloc` (snapshots and diffs by line), `memray` (allocation profiler), `objgraph` (reference graphs), `pympler`, and `psutil` for process RSS.

**Example**

```python
# Runtime: Python 3.12
import gc
import tracemalloc

tracemalloc.start()

cache = []                                   # simulated leak: grows forever
def process_batch(n: int) -> int:
    rows = [{"id": i, "payload": "x" * 100} for i in range(n)]
    cache.extend(rows[:100])                 # keeps references alive
    return len(rows)

snap1 = tracemalloc.take_snapshot()
for _ in range(50):
    process_batch(10_000)
snap2 = tracemalloc.take_snapshot()

for stat in snap2.compare_to(snap1, "lineno")[:3]:
    print(stat)                              # points at the line building rows kept in 'cache'

class Node:
    def __init__(self): self.other = None
a, b = Node(), Node(); a.other, b.other = b, a
del a, b
print("unreachable collected:", gc.collect())
```

**Real-world use case**

A long-running Kafka consumer's memory grew 200 MB per hour until the container was OOM-killed. A `tracemalloc` diff pointed to a dedupe set of every message ID ever seen. Replacing it with a bounded, time-windowed structure stabilised memory.

**Common mistakes and red flags**

- "Python has no memory leaks."
- Calling `gc.collect()` everywhere as a fix.
- Loading entire datasets into Python objects.

**Follow-up questions**

- Why doesn't RSS drop after deleting a big list? — Allocator keeps freed arenas for reuse; fragmentation.
- What does `__slots__` save? — Per-instance `__dict__` overhead (see [Q362](/interview/python/#q362)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q371" data-level="Advanced">
<summary><span class="qb-num">Q371</span><h3 class="qb-title">Profiling and speeding up Python code</h3><span class="qb-meta">Advanced · Common · Performance</span></summary>

**Short answer**

Measure before optimising. Use `cProfile` or `py-spy` for function-level hotspots, `line_profiler` for line-level detail, `timeit` for micro-benchmarks, and `tracemalloc` or `memray` for memory. Then fix the biggest cost first: better algorithms and data structures (sets and dicts over list scans), vectorisation (pandas, Polars, NumPy), fewer I/O round-trips (batching), concurrency for I/O, and moving heavy work into the database or engine.

**Detailed explanation**

**Profiling tools**
| Tool | What it shows | Notes |
|---|---|---|
| `cProfile` + `pstats` / snakeviz | Calls, cumulative time per function | Built-in; some overhead |
| `py-spy` | Sampling profiler; attach to running process, flame graphs | Low overhead, production-friendly |
| `scalene` | CPU, memory, GPU per line; Python vs native time | Great for data code |
| `line_profiler` | Time per line in decorated functions | Focused deep-dives |
| `timeit` | Micro-benchmarks | Compare small alternatives |
| `tracemalloc`, `memray` | Allocation hotspots (see [Q370](/interview/python/#q370)) | Memory issues |

**Typical big wins in DE code**
1. **Algorithmic:** O(n²) membership checks become set lookups (see [Q351](/interview/python/#q351)). Repeated sorting becomes a single sort or a heap.
2. **Vectorise:** replace row loops and `df.apply` with column operations, or move to Polars or DuckDB (see [Q372](/interview/python/#q372), [Q375](/interview/python/#q375)).
3. **Batch I/O:** `executemany` or COPY instead of per-row inserts. Bulk API endpoints. Fewer, larger S3 requests (see [Q397](/interview/python/#q397), [Q398](/interview/python/#q398)).
4. **Push down:** let the warehouse do joins and aggregations instead of pulling raw data into Python.
5. **Concurrency** for I/O waits (see [Q367](/interview/python/#q367)).
6. **Avoid repeated work:** caching (`lru_cache`), compiling regexes once (`re.compile`), and hoisting invariant computations out of loops.
7. **Efficient formats:** Parquet over CSV, and Arrow for interchange (see [Q376](/interview/python/#q376)).
8. **Faster runtimes:** upgrading Python (3.11+ is significantly faster than 3.10, and 3.13+ adds an experimental JIT), PyPy for pure-Python workloads, Cython, Numba or Rust extensions for hot kernels.

**Process**
- Reproduce with representative data. Profile the whole job, find the top functions by cumulative time, optimise one thing, then re-measure.
- Watch for **I/O-wait-dominated** profiles (time in socket or SSL reads). Those need concurrency or batching, not CPU optimisation.

**Example**

```python
# Runtime: Python 3.12
import cProfile
import pstats
import re
from io import StringIO

LINES = [f"2026-03-01 10:{i % 60:02d}:00 user={i % 500} action=view" for i in range(200_000)]

def slow_parse(lines):
    users = []
    for line in lines:
        m = re.search(r"user=(\d+)", line)              # recompiles/looks up each call
        if m and m.group(1) not in users:               # O(n) list membership
            users.append(m.group(1))
    return users

USER_RE = re.compile(r"user=(\d+)")
def fast_parse(lines):
    return {m.group(1) for line in lines if (m := USER_RE.search(line))}

for fn in (slow_parse, fast_parse):
    pr = cProfile.Profile(); pr.enable(); fn(LINES); pr.disable()
    s = StringIO(); pstats.Stats(pr, stream=s).sort_stats("cumulative").print_stats(3)
    print(fn.__name__, s.getvalue().splitlines()[0])   # shows total calls and time
```
`re` caches compiled patterns internally, so the main win here is replacing the O(n) list membership test with a set. Pre-compiling saves a smaller cache-lookup cost.

**Real-world use case**

A transformation script took 3 hours. `py-spy` showed 80% of the time in a function checking `if key in seen_list`, and 15% in per-row database inserts. A set lookup and `executemany` batching brought it to 6 minutes.

**Common mistakes and red flags**

- Micro-optimising without profiling.
- Optimising CPU when the job is I/O-bound.
- Rewriting in another language before trying vectorisation.

**Follow-up questions**

- How do you profile a running production job? — `py-spy dump/record --pid` with appropriate permissions.
- What's the 3.13 JIT? — Experimental copy-and-patch JIT, opt-in at build/run time; modest gains so far.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q372" data-level="Intermediate">
<summary><span class="qb-num">Q372</span><h3 class="qb-title">pandas: loc vs iloc, and vectorisation vs apply</h3><span class="qb-meta">Intermediate · Very Common · pandas</span></summary>

**Short answer**

`df.loc[rows, cols]` selects by label (index labels, boolean masks, column names), and label slices include the end. `df.iloc[rows, cols]` selects by integer position, and slices exclude the end, like Python. Prefer vectorised column operations (`df["a"] * df["b"]`, `np.where`, `.str` and `.dt` accessors, `merge`) over `apply` with Python functions and `iterrows`, which run row by row in Python and are often 10–100 times slower.

**Detailed explanation**

**loc vs iloc**
| | `loc` | `iloc` |
|---|---|---|
| Selects by | Labels / boolean masks | Integer positions |
| Slice end | Inclusive (`'a':'c'` includes `'c'`) | Exclusive (`0:3` → 0,1,2) |
| Columns | Names | Positions |
| Typical use | Filtering: `df.loc[df.status == "paid", ["id", "amount"]]` | Positional: first N rows `df.iloc[:5]` |
| Setting values | `df.loc[mask, "flag"] = True` (safe assignment) | Positional updates |

**Assignment pitfalls**
- Chained indexing (`df[df.a > 0]["b"] = 1`) may modify a temporary copy, which raises `SettingWithCopyWarning` in older modes. Use `df.loc[mask, "b"] = 1`.
- **Copy-on-Write** (opt-in in pandas 2.x and the default in pandas 3.0) makes chained assignment never modify the original, and makes copies lazy. Write code that's correct under CoW.

**Vectorisation ladder (fastest to slowest)**
1. Built-in vectorised ops: arithmetic, comparisons, `.str.*`, `.dt.*`, `np.where`, `np.select`, `.map(dict)`, `clip`, `cut`.
2. `groupby().agg/transform` with built-in aggregations (see [Q373](/interview/python/#q373)).
3. `apply` on columns with vectorised inner functions.
4. `df.apply(func, axis=1)`, `itertuples` (faster than `iterrows`), plain Python loops.
5. `iterrows` (slowest, and it loses dtypes).

**Types matter:** keep numeric columns numeric (not `object`). Use `category` for low-cardinality strings, and nullable dtypes (`Int64`, `string`, `boolean`) or `pd.ArrowDtype` for NULL-aware, efficient columns (see [Q374](/interview/python/#q374)).

**Example**

```python
# Runtime: Python 3.12 with pandas 2.2, numpy 2.x
import numpy as np
import pandas as pd

df = pd.DataFrame({
    "order_id": [1, 2, 3, 4],
    "amount": [120.0, 45.0, 600.0, 15.0],
    "country": ["IN", "US", "US", "UK"],
    "ordered_at": pd.to_datetime(["2026-03-01 10:00", "2026-03-01 23:30", "2026-03-02 08:15", "2026-03-02 12:00"]),
}).set_index("order_id")

print(df.loc[2:3, ["amount", "country"]])     # labels 2 AND 3 (inclusive)
print(df.iloc[0:2, 0])                         # positions 0,1 of first column

# Slow: row-wise apply
df["tier_slow"] = df.apply(lambda r: "high" if r["amount"] >= 500 else "mid" if r["amount"] >= 100 else "low", axis=1)

# Fast: vectorised
df["tier"] = np.select([df["amount"] >= 500, df["amount"] >= 100], ["high", "mid"], default="low")
df["order_date"] = df["ordered_at"].dt.date
df.loc[df["country"] == "US", "amount_with_tax"] = df["amount"] * 1.08
print(df[["tier", "tier_slow", "amount_with_tax"]])
```
Both tier columns are identical: high for 600, mid for 120, low for 45 and 15. Non-US rows get NaN for `amount_with_tax`.

**Real-world use case**

A feature-engineering script used `df.apply(axis=1)` on 8 million rows to compute a tiering rule, which took 14 minutes. `np.select` did it in 0.4 seconds.

**Common mistakes and red flags**

- `iterrows` loops for transformations.
- Chained assignment.
- Forgetting that `loc` slices are inclusive.

**Follow-up questions**

- When is `apply` acceptable? — Small data, or complex logic with no vectorised alternative; consider Polars expressions or Numba otherwise.
- `at`/`iat`? — Fast scalar access by label/position.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q373" data-level="Intermediate">
<summary><span class="qb-num">Q373</span><h3 class="qb-title">pandas merge, concat and groupby/agg</h3><span class="qb-meta">Intermediate · Very Common · pandas</span></summary>

**Short answer**

`pd.merge(left, right, on=..., how="inner"|"left"|"right"|"outer"|"cross", validate=...)` joins on columns, like SQL joins. `pd.concat([df1, df2])` stacks frames vertically (or side by side with `axis=1`). `df.groupby(keys).agg(...)` aggregates, and named aggregation gives clean column names. `transform` returns group results aligned to the original rows. Use `validate="one_to_one"` / `"many_to_one"` and `indicator=True` to catch fan-out and unmatched rows.

**Detailed explanation**

**merge**
- `on`, `left_on`/`right_on`, `left_index`/`right_index`, `suffixes=("_l", "_r")`.
- `validate="many_to_one"` raises `MergeError` if the right side has duplicate keys. This prevents silent row multiplication, the pandas equivalent of the SQL fan-out bug (see [Q117](/interview/sql/#q117)).
- `indicator=True` adds a `_merge` column (`left_only`, `right_only`, `both`), which is great for reconciliation.
- Key dtypes must match (int vs str keys don't join). NaN keys match each other in pandas merges, unlike SQL NULLs (see [Q107](/interview/sql/#q107)).

**concat**
- Vertical: `pd.concat(frames, ignore_index=True)`. Columns are aligned by name, and missing ones are filled with NaN.
- Performance: collect frames in a list and concat once. Don't concat inside a loop, which is quadratic.
- `keys=` adds a hierarchical index identifying each source.

**groupby**
- Named aggregation: `df.groupby("country", as_index=False).agg(revenue=("amount", "sum"), orders=("order_id", "nunique"))`.
- `transform("sum")` broadcasts group totals back to rows (share of total, group-wise normalisation).
- `dropna=False` keeps NaN groups (dropped by default!).
- `observed=True` for categorical keys avoids empty category combinations (the default becomes True in newer pandas).
- `sort=False` for speed when order doesn't matter.
- Custom lambdas in `agg` are slow, so prefer the built-in names.

**Example**

```python
# Runtime: Python 3.12 with pandas 2.2
import pandas as pd

orders = pd.DataFrame({"order_id": [1, 2, 3, 4], "customer_id": [10, 11, 10, 12],
                       "amount": [100.0, 50.0, 25.0, 80.0]})
customers = pd.DataFrame({"customer_id": [10, 11, 13], "segment": ["SMB", "ENT", "SMB"]})

joined = pd.merge(orders, customers, on="customer_id", how="left",
                  validate="many_to_one", indicator=True)
unmatched = joined.loc[joined["_merge"] == "left_only", "order_id"].tolist()     # [4]

summary = (joined.groupby("segment", dropna=False, as_index=False)
                 .agg(revenue=("amount", "sum"), orders=("order_id", "nunique")))
joined["share_of_customer_spend"] = joined["amount"] / joined.groupby("customer_id")["amount"].transform("sum")

march = pd.concat([orders.assign(month="2026-02"), orders.assign(month="2026-03")], ignore_index=True)
print(unmatched, summary, joined[["order_id", "share_of_customer_spend"]], len(march), sep="\n")
```
The summary has SMB revenue 125.0 (2 orders), ENT 50.0 (1 order), and a NaN segment with 80.0 (1 order), which is kept by `dropna=False`. Customer 10's order shares are 0.8 and 0.2.

**Real-world use case**

A revenue report doubled one region's numbers after a dimension file gained duplicate customer rows. Adding `validate="many_to_one"` to the merge made the job fail immediately with a clear error the next time the file had duplicates.

**Common mistakes and red flags**

- Merging without `validate`.
- Concat inside a loop.
- Not realising groupby drops NaN keys by default.

**Follow-up questions**

- `join` vs `merge`? — `df.join` merges on index by default; `merge` is more general.
- How do you do an anti-join in pandas? — Merge with `indicator=True` and keep `left_only`.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q374" data-level="Intermediate">
<summary><span class="qb-num">Q374</span><h3 class="qb-title">Processing data larger than memory in pandas (chunks, dtypes)</h3><span class="qb-meta">Intermediate · Very Common · pandas</span></summary>

**Short answer**

Read only the columns you need (`usecols`), with compact dtypes (category for low-cardinality strings, downcast integers and floats, Arrow-backed strings), and process the data in chunks (`read_csv(chunksize=...)`, or Parquet row groups and batches via pyarrow), aggregating incrementally. If it's still too big, switch to Polars lazy or streaming mode, DuckDB, or Spark instead of forcing pandas.

**Detailed explanation**

**Reduce what you load**
- `usecols=[...]` and `columns=[...]` for Parquet.
- **Filter at read time:** Parquet `filters=[("event_date", "=", "2026-03-01")]` with pyarrow, for partition and row-group pruning (see [Q53](/interview/data-engineering/#q53)).
- `nrows` for sampling during development.

**Shrink dtypes**
| Data | Default | Better |
|---|---|---|
| Low-cardinality strings (country, status) | `object` | `category` |
| Strings generally | `object` | `string[pyarrow]` / `pd.ArrowDtype(pa.string())` |
| Small ints | `int64` | `int8/16/32` or nullable `Int32` |
| Floats | `float64` | `float32` (if precision allows) |
| Timestamps as strings | `object` | `datetime64[ns]` via `parse_dates` |
| Booleans with nulls | `object` | `boolean` |

`df.memory_usage(deep=True)` shows the real usage. Object string columns are often 5–10 times larger than they need to be.

**Chunked processing**
- `pd.read_csv(path, chunksize=500_000, dtype=..., usecols=...)` returns an iterator of DataFrames.
- Aggregate each chunk, then combine the partial results (sums and counts combine easily; for means, keep sum and count).
- For per-key operations that need all the rows of a key, sort or partition files by key first, or use a database or engine.
- For Parquet: `pyarrow.parquet.ParquetFile(path).iter_batches(batch_size=..., columns=...)`.

**When to switch tools**
- Many GBs, joins between large datasets, or repeated jobs: **Polars** lazy and streaming (`scan_parquet(...).collect(engine="streaming")`) or **DuckDB** SQL on files, both of which spill to disk and use all cores.
- Bigger still, or a shared platform: Spark or the warehouse (see [Q300](/interview/pyspark/#q300), [Q375](/interview/python/#q375)).

**Example**

```python
# Runtime: Python 3.12 with pandas 2.2
import pandas as pd

dtypes = {"store_id": "int32", "country": "category", "status": "category", "amount": "float64"}
totals: dict[tuple[str, str], list[float]] = {}

for chunk in pd.read_csv("sales_2026_q1.csv", usecols=list(dtypes) + ["sold_at"],
                         dtype=dtypes, parse_dates=["sold_at"], chunksize=1_000_000):
    chunk = chunk[chunk["status"] == "completed"]
    chunk["month"] = chunk["sold_at"].dt.strftime("%Y-%m")
    part = chunk.groupby(["country", "month"], observed=True)["amount"].agg(["sum", "count"])
    for key, row in part.iterrows():                     # small partial result
        acc = totals.setdefault(key, [0.0, 0])
        acc[0] += row["sum"]; acc[1] += int(row["count"])

result = (pd.DataFrame([(c, m, s, n, s / n) for (c, m), (s, n) in totals.items()],
                       columns=["country", "month", "revenue", "orders", "avg_order"])
            .sort_values(["country", "month"]))
print(result.head())
```
`iterrows` is acceptable here because it loops over the tiny per-chunk aggregate (a few hundred rows), not the raw data.

**Real-world use case**

A 25 GB CSV crashed a 16 GB container on `read_csv`. With `usecols`, category dtypes and 1-million-row chunks, the job ran in 3 GB of RAM. It was later migrated to DuckDB, which runs the whole aggregation as one SQL query in under 2 minutes.

**Common mistakes and red flags**

- Loading all columns as object dtype.
- Averaging chunk averages.
- Insisting on pandas for data far bigger than RAM.

**Follow-up questions**

- How do you measure real memory? — `df.memory_usage(deep=True).sum()` and process RSS.
- Why category dtype? — Stores integer codes plus a small dictionary of unique values.

**In depth:** [How would you process a 50 GB CSV file on a machine with 8 GB of RAM?](/interview/python/process-large-csv-limited-memory/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q375" data-level="Advanced">
<summary><span class="qb-num">Q375</span><h3 class="qb-title">pandas vs Polars vs PySpark</h3><span class="qb-meta">Advanced · Common · Data Libraries</span></summary>

**Short answer**

pandas is the ubiquitous single-node DataFrame library: eager, mostly single-threaded, and with the biggest ecosystem. Polars is a Rust-based single-node engine with a lazy query optimiser, multi-threaded execution and streaming for larger-than-memory data, and is often several times faster than pandas. PySpark is a distributed engine for multi-node scale, lakehouse integration and streaming. DuckDB is the SQL counterpart to Polars. Choose by data size, latency, team skills and platform. See [Q300](/interview/pyspark/#q300) for the Spark-centric view.

**Detailed explanation**

| Aspect | pandas 2.x | Polars 1.x | PySpark |
|---|---|---|---|
| Execution | Eager | Lazy (query optimizer) or eager | Lazy, distributed |
| Parallelism | Mostly single core | All cores | Cluster |
| Memory model | NumPy / optional Arrow | Arrow | JVM Tungsten / Arrow for Python interop |
| Larger than RAM | Chunking manually | Streaming engine | Yes (spill, distribution) |
| API style | Index-based, many ways to do things | Expression-based, consistent, no index | DataFrame/SQL |
| Null handling | NaN/None mix (nullable dtypes help) | Proper nulls everywhere | SQL nulls |
| Ecosystem | Largest (sklearn, plotting, many readers) | Growing; converts to/from pandas/Arrow cheaply | Lakehouse, connectors, MLlib |
| Startup | Instant | Instant | Seconds–minutes (cluster/session) |
| Typical scale | MBs–few GB | GBs–100s GB on one machine | 100s GB–PB |

**Polars strengths**
- **Lazy optimisation:** predicate and projection pushdown into Parquet scans, common subexpression elimination, and join reordering (simple).
- **Expressions:** `pl.col("amount").sum().over("customer_id")`, `when/then/otherwise`, all composable and parallel.
- **Streaming engine:** processes data in batches for larger-than-memory workloads.
- Reads Parquet, Delta (via `deltalake`) and Iceberg (via `pyiceberg`) directly.

**When pandas still makes sense:** small data, library compatibility (many ML and visualisation tools expect pandas), and existing codebases. pandas 2's Arrow dtypes and 3.0's copy-on-write and string improvements narrow some gaps.

**When PySpark:** data or compute beyond one machine, streaming, a governed lakehouse platform, or many concurrent jobs. Serverless Spark has lowered the start-up overhead.

**Migration notes:** Polars has no index, so `loc` and `iloc` patterns change. Use `filter`, `select` and `with_columns`. Explicit null semantics may change results where pandas treated NaN loosely.

**Example**

```python
# Runtime: Python 3.12 with polars 1.x, pandas 2.2, duckdb 1.x
import duckdb
import pandas as pd
import polars as pl

# pandas
pdf = pd.read_parquet("orders.parquet", columns=["customer_id", "amount", "status"])
pd_res = (pdf[pdf["status"] == "paid"].groupby("customer_id", as_index=False)["amount"].sum()
          .nlargest(5, "amount"))

# Polars (lazy: filter + column selection pushed into the Parquet scan)
pl_res = (pl.scan_parquet("orders.parquet")
            .filter(pl.col("status") == "paid")
            .group_by("customer_id").agg(pl.col("amount").sum())
            .sort("amount", descending=True).head(5)
            .collect())

# DuckDB SQL over the same file
duck_res = duckdb.sql("""
    SELECT customer_id, SUM(amount) AS amount
    FROM 'orders.parquet' WHERE status = 'paid'
    GROUP BY customer_id ORDER BY amount DESC LIMIT 5
""").pl()
```

**Real-world use case**

A daily 40 GB Parquet aggregation needed a 128 GB VM with pandas and took 25 minutes. Polars lazy with streaming ran it on a 16 GB VM in 3 minutes. The team kept pandas only for the final small DataFrame passed to plotting.

**Common mistakes and red flags**

- Assuming Polars is a drop-in for pandas.
- Spark for 50 MB files.
- Ignoring DuckDB and Polars for medium data.

**Follow-up questions**

- How do you convert between them? — `pl.from_pandas`, `df.to_pandas()`, Arrow tables (`pl.from_arrow`), often near zero-copy.
- What is pushdown in Polars? — Lazy plans push filters/columns into scans so less data is read.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q376" data-level="Beginner">
<summary><span class="qb-num">Q376</span><h3 class="qb-title">Reading and writing CSV, JSON and Parquet</h3><span class="qb-meta">Beginner · Very Common · File I/O</span></summary>

**Short answer**

For small files, use the standard library: `csv.DictReader`/`DictWriter` with `newline=""` and an explicit encoding, and `json.load`/`json.dump`, or line-by-line `json.loads` for JSONL. For analytics, use pandas, Polars or pyarrow: `read_csv` with explicit dtypes, `read_json(lines=True)`, and `read_parquet`/`write_parquet` (typed, columnar, compressed). Prefer Parquet for anything stored and re-read. Use CSV and JSON for interchange.

**Detailed explanation**

**CSV with the standard library**
- `open(path, newline="", encoding="utf-8")`. `newline=""` prevents blank lines on Windows and handles embedded newlines correctly.
- `csv.DictReader` gives dicts keyed by header. Everything is a string, so convert explicitly.
- Dialects: `delimiter`, `quotechar`, `quoting=csv.QUOTE_MINIMAL`.
- **BOM** from Excel exports: read with `encoding="utf-8-sig"`.

**JSON and JSONL**
- `json.load(f)` for whole documents, which loads everything into memory.
- **JSON Lines** (one object per line) streams well: `for line in f: json.loads(line)`.
- `json.dumps(obj, default=str)` handles datetimes and decimals by converting them to strings. Better to serialise deliberately (isoformat, str for Decimal).
- Large single JSON arrays: use streaming parsers (`ijson`), or convert upstream to JSONL.

**Parquet (pyarrow, pandas, Polars)**
- `pd.read_parquet(path, columns=[...], filters=[...])` and `df.to_parquet(path, index=False, compression="zstd")`.
- `pl.read_parquet`/`scan_parquet`, `df.write_parquet`.
- `pyarrow.dataset` for partitioned directories (`dataset("s3://...", partitioning="hive")`).
- Preserves types (timestamps, decimals, nested), compresses well, and supports column pruning (see [Q52](/interview/data-engineering/#q52)).

**Encoding and correctness checklist**
- Always set `encoding` explicitly (UTF-8).
- Make timezone handling explicit for timestamps (see [Q396](/interview/python/#q396)).
- Money: Decimal or integer cents, never float for exact amounts in finance outputs.
- Atomic writes: write to a temp file or prefix, then rename or move (see [Q399](/interview/python/#q399)).

**Example**

```python
# Runtime: Python 3.12 with pandas 2.2, pyarrow 17+
import csv
import json
from decimal import Decimal
from pathlib import Path
import pandas as pd

src = Path("orders.csv")
src.write_text("order_id,amount,currency\n1,19.99,USD\n2,5.00,EUR\n", encoding="utf-8")

with src.open(newline="", encoding="utf-8-sig") as f:
    rows = [{"order_id": int(r["order_id"]), "amount": Decimal(r["amount"]), "currency": r["currency"]}
            for r in csv.DictReader(f)]

with Path("orders.jsonl").open("w", encoding="utf-8") as f:
    for r in rows:
        f.write(json.dumps({**r, "amount": str(r["amount"])}) + "\n")

df = pd.read_json("orders.jsonl", lines=True, dtype={"amount": "string"})
df["amount"] = df["amount"].map(Decimal)
pd.DataFrame(rows).astype({"amount": "float64"}).to_parquet("orders.parquet", index=False, compression="zstd")
print(pd.read_parquet("orders.parquet", columns=["order_id", "amount"]))
```
Converting to float64 for Parquet is fine for analytics. For exact money, store integer cents or a pyarrow decimal type instead.

**Real-world use case**

A partner's CSVs were exported from Excel with a BOM, so the first column became `\ufefforder_id` and every lookup failed. Reading with `utf-8-sig`, plus a header validation step, fixed it.

**Common mistakes and red flags**

- No encoding specified.
- Floats for money in exports.
- `json.load` on multi-GB files.

**Follow-up questions**

- Why JSONL over JSON arrays? — Streamable, appendable, splittable by line.
- How do you write a partitioned Parquet dataset? — `pyarrow.dataset.write_dataset(..., partitioning=["event_date"])` or Polars/pandas `partition_cols`.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q377" data-level="Intermediate">
<summary><span class="qb-num">Q377</span><h3 class="qb-title">Read a huge file in chunks with a generator</h3><span class="qb-meta">Intermediate · Very Common · File I/O</span></summary>

**Short answer**

Never load a huge file at once. Iterate the file object line by line (it's a lazy iterator), or read fixed-size byte blocks with `f.read(size)`, inside a generator that yields lines, records or batches. Use `with` so the file closes, and an explicit encoding. For compressed files, `gzip.open(path, "rt")` streams too. Downstream steps then process one batch at a time with constant memory.

**Detailed explanation**

**Options by data shape**
| Data | Approach |
|---|---|
| Text, line-oriented (logs, CSV, JSONL) | `for line in f` — buffered, lazy |
| Records in batches | Generator yielding lists of N parsed lines (or `itertools.batched`) |
| Binary or fixed-size blocks | `iter(lambda: f.read(1 << 20), b"")` |
| CSV with quoted newlines | `csv.reader(f)` over the file iterator (handles multi-line fields) — don't split lines yourself |
| Compressed | `gzip.open`, `bz2.open`, `lzma.open` in text mode; `zstandard` stream readers |
| Remote objects | Streaming body (`boto3` `StreamingBody.iter_lines()`; see [Q398](/interview/python/#q398)) |

**Design tips**
- Yield **batches** (thousands of records) for downstream efficiency: bulk inserts, Parquet row groups.
- Track **line numbers or offsets** for error messages and restartability (resume from offset N).
- Handle **bad lines** without stopping: yield errors to a quarantine path (see [Q391](/interview/python/#q391)).
- Avoid `f.readlines()` and `f.read()` on big files, both of which load everything.
- For random access or parallelism, split the file into byte ranges aligned to newlines, and process the ranges in separate processes.

**Memory check:** peak memory should depend on the batch size, not the file size. A quick test is to run the job on a 10× larger file and confirm RSS stays roughly flat.

**Example**

```python
# Runtime: Python 3.12
import gzip
import json
from pathlib import Path
from typing import Iterator

def read_jsonl_batches(path: str | Path, batch_size: int = 10_000) -> Iterator[tuple[list[dict], list[tuple[int, str]]]]:
    """Yield (good_records, bad_lines) batches with constant memory."""
    path = Path(path)
    opener = gzip.open if path.suffix == ".gz" else open
    good: list[dict] = []
    bad: list[tuple[int, str]] = []
    with opener(path, "rt", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                good.append(json.loads(line))
            except json.JSONDecodeError:
                bad.append((lineno, line[:500]))
            if len(good) >= batch_size:
                yield good, bad
                good, bad = [], []
    if good or bad:
        yield good, bad

total = 0
for records, errors in read_jsonl_batches("events-2026-03-01.jsonl.gz", batch_size=5_000):
    total += len(records)                 # e.g. write batch to Parquet / bulk insert
    for lineno, snippet in errors:
        print(f"bad line {lineno}: {snippet[:80]!r}")
print("records:", total)
```

**Real-world use case**

A support tool parsed a 12 GB gzipped audit log on a 2 GB container by streaming it with batches of 5,000. Bad lines with line numbers went to a quarantine file, so the vendor could fix their exporter.

**Common mistakes and red flags**

- `readlines()` on large files.
- Splitting CSV by newline manually.
- Losing line numbers for errors.

**Follow-up questions**

- How do you resume after failure? — Persist the last processed line/offset or batch number and skip to it on restart.
- Is `for line in f` buffered? — Yes; Python reads in buffered blocks and splits lines.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q378" data-level="Advanced">
<summary><span class="qb-num">Q378</span><h3 class="qb-title">API ingestion with pagination (offset, cursor, next-link)</h3><span class="qb-meta">Advanced · Very Common · API Ingestion</span></summary>

**Short answer**

Write a generator that requests a page, yields its records, and follows the API's pagination scheme until no more pages remain. Schemes include offset/limit, page numbers, an opaque cursor or token, a `next` link in the body, or a `Link` header. Prefer cursor or keyset pagination for consistency under concurrent changes. Combine it with an incremental watermark (`updated_since`), retries, rate limiting and checkpointing of the last cursor, so runs can resume.

**Detailed explanation**

| Scheme | How | Pros | Cons |
|---|---|---|---|
| Offset/limit (`?offset=200&limit=100`) | Skip N rows | Simple, random access | Duplicates/missed rows when data changes during paging; slow deep offsets |
| Page number | `?page=3` | Simple | Same issues as offset |
| Cursor/token (`?cursor=abc`) | Server returns next cursor | Stable, efficient | Sequential only; cursor expiry |
| Keyset (`?after_id=12345` / `updated_since`) | Filter by last seen key | Stable and efficient | Needs sortable unique key |
| Next link (`"next": "<next page link>"` or `Link` header) | Follow URL | Server controls params | Must validate URL host |

**Robust ingestion checklist**
1. **Incremental window:** `updated_since = last_watermark - overlap` (for late updates), and dedupe by ID downstream (see [Q388](/interview/python/#q388)).
2. **Stable ordering:** request results sorted by `updated_at, id` where the API supports it.
3. **Termination conditions:** empty page, missing next cursor, or `has_more = false`. Guard against infinite loops (repeating cursor, a maximum page count).
4. **Retries and rate limits:** 429 with `Retry-After`, 5xx backoff, and timeouts on every request (see [Q379](/interview/python/#q379)).
5. **Checkpointing:** persist the cursor or watermark after each page is safely written, so a crash resumes there (see [Q399](/interview/python/#q399)).
6. **Raw landing:** store the raw JSON pages (with request metadata) before transforming, for replay and debugging.
7. **Security:** keep tokens in a secret store (see [Q381](/interview/python/#q381)). When following next links, check that the host matches the expected API host, to avoid sending credentials elsewhere.

**Example**

```python
# Runtime: Python 3.12 with requests 2.x
from typing import Iterator
from urllib.parse import urlparse
import requests

BASE = "https://api.crm.example"

def iter_contacts(session: requests.Session, updated_since: str, page_size: int = 200,
                  max_pages: int = 10_000) -> Iterator[dict]:
    url = f"{BASE}/v2/contacts"
    params: dict | None = {"updated_since": updated_since, "limit": page_size, "sort": "updated_at,id"}
    seen_cursors: set[str] = set()
    for _ in range(max_pages):
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        yield from body.get("data", [])

        next_url = body.get("paging", {}).get("next")
        if not next_url:
            return
        if urlparse(next_url).netloc != urlparse(BASE).netloc:
            raise ValueError(f"unexpected pagination host: {next_url}")
        if next_url in seen_cursors:
            raise RuntimeError("pagination loop detected")
        seen_cursors.add(next_url)
        url, params = next_url, None            # next link already contains query params
    raise RuntimeError("max_pages exceeded; check pagination logic")

session = requests.Session()
session.headers["Authorization"] = "Bearer <token from secret store>"
for contact in iter_contacts(session, updated_since="2026-03-01T00:00:00Z"):
    pass   # write to landing in batches; checkpoint last updated_at
```
This uses plain `raise_for_status`. In production, wrap the request with the retry and classification logic from Q365 and Q379.

**Real-world use case**

An offset-paginated export missed about 1% of contacts each run, because records updated mid-run shifted between pages. Switching to the API's cursor pagination, with an `updated_since` overlap of 10 minutes and dedupe by ID, made the loads complete.

**Common mistakes and red flags**

- No termination guard.
- Offset paging on changing data.
- No checkpoint, so a crash on page 900 restarts from page 1.

**Follow-up questions**

- How do you parallelise paging? — Partition by time windows or ID ranges (if API supports filters), each paginated separately.
- How do you handle cursor expiry? — Restart from last checkpointed watermark with overlap.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q379" data-level="Advanced">
<summary><span class="qb-num">Q379</span><h3 class="qb-title">Retries with exponential backoff and rate limiting</h3><span class="qb-meta">Advanced · Common · Reliability</span></summary>

**Short answer**

Retry only transient failures (timeouts, connection errors, 429, 502/503/504, deadlocks), with exponential backoff plus jitter (`base * 2**attempt`, randomised), a maximum attempt count and a total time budget. Honour `Retry-After`. Separately, limit request rate (a token bucket) and concurrency, so you don't cause the 429s in the first place. Use well-tested libraries such as tenacity, or urllib3 `Retry` adapters for requests.

**Detailed explanation**

**Why jitter:** without randomness, many clients retry in sync ("thundering herd"), causing repeated overload. "Full jitter" sleeps a random time between 0 and the backoff ceiling.

**Retry policy components**
| Component | Typical value |
|---|---|
| Retryable conditions | Network errors, timeouts, HTTP 429/500/502/503/504, DB serialization/deadlock errors |
| Non-retryable | 400, 401, 403, 404 (usually), validation errors (see [Q365](/interview/python/#q365)) |
| Attempts | 3–6 |
| Backoff | 0.5–1 s base, ×2 each attempt, cap 30–60 s |
| Jitter | Full or equal jitter |
| Budget | Stop after total elapsed > N minutes |
| Retry-After | Use server value when provided |

**Idempotency:** retrying a POST that already succeeded server-side can create duplicates. Use idempotency keys (where the API supports them), or design writes as upserts (see [Q47](/interview/data-engineering/#q47)).

**Rate limiting**
- **Token bucket:** allow R requests per second with burst B. Sleep when the bucket is empty.
- Respect documented quotas and adapt to `X-RateLimit-Remaining` headers.
- In distributed workers, coordinate through a shared limiter (Redis) or partition the quota among workers.

**Libraries**
- `tenacity`: decorators with `stop_after_attempt`, `wait_random_exponential`, `retry_if_exception_type`, and `before_sleep` logging.
- requests plus `urllib3.util.Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504], respect_retry_after_header=True)` mounted on an `HTTPAdapter`.
- Cloud SDKs (boto3, the Snowflake connector) have built-in retry configs, so tune those rather than wrapping them twice.

**Example**

```python
# Runtime: Python 3.12 with tenacity 8.x / 9.x
import logging
import threading
import time
from tenacity import (retry, retry_if_exception_type, stop_after_attempt, stop_after_delay,
                      wait_random_exponential, before_sleep_log)

log = logging.getLogger(__name__)

class RetryableError(Exception):
    pass

class TokenBucket:
    def __init__(self, rate_per_s: float, burst: int):
        self.rate, self.capacity = rate_per_s, burst
        self.tokens, self.updated = float(burst), time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / self.rate
            time.sleep(wait)

limiter = TokenBucket(rate_per_s=10, burst=20)

@retry(retry=retry_if_exception_type(RetryableError),
       wait=wait_random_exponential(multiplier=0.5, max=30),
       stop=(stop_after_attempt(6) | stop_after_delay(300)),
       before_sleep=before_sleep_log(log, logging.WARNING),
       reraise=True)
def call_partner(order_id: str) -> dict:
    limiter.acquire()
    # resp = session.get(...); raise RetryableError on 429/5xx/timeouts (see Q365)
    return {"order_id": order_id, "status": "ok"}

print(call_partner("o-1001"))
```

**Real-world use case**

Fifty workers retried a failing endpoint every second without jitter, which turned a brief outage into a 30-minute overload. Adding full-jitter exponential backoff, a shared rate limit of 20 requests per second, and honouring `Retry-After` let the partner recover, and the pipeline completed within its SLA.

**Common mistakes and red flags**

- Retrying 4xx errors.
- No jitter.
- Infinite retries without a time budget.
- Retrying non-idempotent writes blindly.

**Follow-up questions**

- What's a circuit breaker? — Stops calls to a failing dependency for a cool-down period after repeated failures.
- Where should retries live? — At one layer (SDK, client wrapper, or orchestrator), not stacked multiplicatively.

**In depth:** [How would you add retries with backoff to API ingestion in Python?](/interview/python/api-retries-backoff/)

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q380" data-level="Intermediate">
<summary><span class="qb-num">Q380</span><h3 class="qb-title">Logging in pipelines</h3><span class="qb-meta">Intermediate · Common · Observability</span></summary>

**Short answer**

Use the `logging` module, never `print`. Create module-level loggers with `logging.getLogger(__name__)`, and configure handlers and format once at the entry point. Use appropriate levels and lazy `%s` formatting. Emit structured context (run ID, task, source, batch, row counts, durations), ideally as JSON for log platforms. Log exceptions with `log.exception`, and never log secrets or raw PII.

**Detailed explanation**

**Levels**
| Level | Use |
|---|---|
| DEBUG | Detailed diagnostics (off in prod by default) |
| INFO | Milestones: start/end of steps, row counts, durations |
| WARNING | Unexpected but handled (retries, skipped bad records) |
| ERROR | A step failed |
| CRITICAL | Pipeline cannot continue |

**Setup**
- Libraries and modules: only `log = logging.getLogger(__name__)`, with no `basicConfig` inside libraries.
- Entry point: `logging.basicConfig(level=..., format=...)` or `logging.config.dictConfig(...)`.
- Orchestrators (Airflow, Databricks, Lambda) capture stdout and stderr and the root logger. Follow their conventions.

**Structured logging**
- JSON lines with fields like `timestamp, level, logger, message, run_id, task, source, rows, duration_ms` can be searched and aggregated in Datadog, CloudWatch, Splunk or Elastic.
- Use `extra={...}`, a `LoggerAdapter`, or libraries like `structlog` or `python-json-logger`.
- Correlate across systems with a **run or trace ID** (OpenTelemetry for traces and metrics; see [Q91](/interview/data-engineering/#q91)).

**What to log per step:** inputs (source, partition, watermark), outputs (rows read, written, rejected), duration, retries, and the final status. These are the numbers you need during an incident (see [Q91](/interview/data-engineering/#q91), [Q92](/interview/data-engineering/#q92)).

**Hygiene**
- Lazy formatting: `log.info("loaded %s rows", n)`, not f-strings. It avoids formatting cost when the level is disabled, and keeps messages groupable.
- **No secrets** (tokens, passwords, connection strings) and minimal PII. Mask emails and IDs if needed.
- Avoid logging every row. Log aggregates and samples.
- Make sure stack traces appear once (log at the boundary that handles the error).

**Example**

```python
# Runtime: Python 3.12
import json
import logging
import sys
import time
import uuid

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {"ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"), "level": record.levelname,
                   "logger": record.name, "msg": record.getMessage()}
        for key in ("run_id", "step", "rows", "duration_ms", "source"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])

run_id = str(uuid.uuid4())
log = logging.LoggerAdapter(logging.getLogger("orders_etl"), {"run_id": run_id})

start = time.perf_counter()
rows = 12_345
log.info("loaded orders", extra={"step": "load", "rows": rows, "source": "s3://acme-landing/orders/2026-03-01/",
                                  "duration_ms": round((time.perf_counter() - start) * 1000)})
```
`LoggerAdapter` merges its own `extra` and, by default, ignores a call's `extra` on Python versions before 3.13. On 3.13+, set `merge_extra=True` on the adapter so per-call fields like `rows` are kept. On older versions, pass the per-call fields through a custom adapter.

**Real-world use case**

During an incident, logs only said "done" for each step, so nobody could tell which partition had loaded zero rows. Structured logs with `run_id`, `step`, `rows` and `source` let the team find it with one query, and they added an alert on `rows == 0`.

**Common mistakes and red flags**

- `print` in production.
- f-strings everywhere in log calls.
- Logging credentials.
- No row counts.

**Follow-up questions**

- How do you change log level without code changes? — Env var/config read at startup (`LOG_LEVEL`).
- Logs vs metrics? — Logs for detail and debugging; metrics for aggregated time series and alerting.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q381" data-level="Beginner">
<summary><span class="qb-num">Q381</span><h3 class="qb-title">Configuration and secrets via environment variables</h3><span class="qb-meta">Beginner · Common · Configuration</span></summary>

**Short answer**

Keep configuration (URLs, bucket names, batch sizes, environment names) and secrets (passwords, keys, tokens) out of code. Read them from environment variables, with `os.environ["KEY"]` for required values and `os.getenv("KEY", default)` for optional ones. Validate them at startup, ideally through a typed settings object (pydantic-settings). Inject secrets from a secrets manager, never from Git or `.env` files committed to the repo.

**Detailed explanation**

**Why environment variables:** the same code runs in dev, CI and prod, with only the environment differing (the twelve-factor app principle). They work across containers, Lambda, Airflow, Kubernetes and Databricks jobs.

**Patterns**
| Need | Approach |
|---|---|
| Required setting | `os.environ["DB_HOST"]` → KeyError early (fail fast) |
| Optional with default | `os.getenv("BATCH_SIZE", "5000")` then convert/validate |
| Typed, validated config | `pydantic-settings` `BaseSettings` (env prefix, types, `.env` for local dev) |
| Secrets | Secrets manager (AWS Secrets Manager, Azure Key Vault, GCP Secret Manager, Vault, Databricks secret scopes, Airflow connections) injected as env vars or fetched at runtime |
| Local development | `.env` file (git-ignored) loaded by settings library or `python-dotenv` |
| Feature flags | Env var or config service |

**Security practices**
- **Never commit secrets.** Add `.env` to `.gitignore`, and use pre-commit secret scanners (gitleaks, detect-secrets).
- Prefer **short-lived credentials and identity** over static keys: IAM roles, workload identity, OIDC from CI, key-pair auth with rotation (see [Q86](/interview/data-engineering/#q86)).
- Don't print env vars or full configs in logs. Mask secret fields (`SecretStr` in Pydantic shows `**********`).
- Restrict who can read secrets in the orchestrator and cloud.
- Environment variables can leak through process listings or crash dumps on some systems. For highly sensitive secrets, fetch them from the manager at runtime into memory.

**Validate at startup:** fail immediately with a clear message if a required setting is missing or malformed, rather than crashing halfway through a load.

**Example**

```python
# Runtime: Python 3.12 with pydantic-settings 2.x
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

class EtlSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ETL_", env_file=".env", extra="ignore")

    env: str = Field(pattern="^(dev|ci|prod)$")
    source_bucket: str
    batch_size: int = Field(default=5000, ge=100, le=100_000)
    snowflake_account: str
    snowflake_user: str
    snowflake_private_key: SecretStr          # injected by secrets manager in prod

settings = EtlSettings()                      # raises ValidationError listing all problems
print(settings.env, settings.batch_size, settings.snowflake_private_key)   # key prints as **********
key_pem = settings.snowflake_private_key.get_secret_value()                # use only where needed
```

```bash
export ETL_ENV=dev ETL_SOURCE_BUCKET=acme-dev-landing \
       ETL_SNOWFLAKE_ACCOUNT=acme-xy123 ETL_SNOWFLAKE_USER=SVC_ETL_DEV
export ETL_SNOWFLAKE_PRIVATE_KEY="$(aws secretsmanager get-secret-value --secret-id etl/dev/sf-key --query SecretString --output text)"
```

**Real-world use case**

A database password committed in a config file was found by a public repository scanner within hours of a repo being made public by mistake. The team rotated it, moved all secrets to the cloud secrets manager injected at runtime, and added gitleaks to pre-commit and CI.

**Common mistakes and red flags**

- Hard-coded credentials.
- Committing `.env` files.
- Logging the config dict.
- Converting env strings without validation (`"false"` is truthy).

**Follow-up questions**

- Why is `bool(os.getenv("FLAG"))` wrong? — Any non-empty string, including "false", is True; parse explicitly.
- How do Airflow tasks get secrets? — Connections/Variables backed by a secrets backend (see [Q86](/interview/data-engineering/#q86)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q382" data-level="Intermediate">
<summary><span class="qb-num">Q382</span><h3 class="qb-title">pytest fixtures and parametrize</h3><span class="qb-meta">Intermediate · Common · Testing</span></summary>

**Short answer**

pytest discovers `test_*.py` files and `test_*` functions, and uses plain `assert` with detailed failure output. Fixtures (`@pytest.fixture`) provide reusable setup and teardown (sample DataFrames, temp directories, a local SparkSession, fake clients), injected by parameter name, with scopes (function, module, session). `@pytest.mark.parametrize` runs one test over many input and expected pairs, which is ideal for edge cases in transformation logic.

**Detailed explanation**

**Fixtures**
- Defined in test files or a shared `conftest.py`.
- **Scopes:** `function` (default, fresh each test), `class`, `module`, `session` (for expensive objects like a SparkSession).
- **Teardown** through `yield`: code after `yield` runs after the test.
- Built-ins: `tmp_path` (a per-test temp directory), `monkeypatch` (set env vars and attributes), `caplog` (captured logs), `capsys`.
- Fixtures can depend on other fixtures.

**Parametrize**
- `@pytest.mark.parametrize("raw,expected", [("A@X.COM ", "a@x.com"), ...])`.
- `ids=` for readable test names. `pytest.param(..., marks=pytest.mark.xfail)` for known issues.
- Stack decorators for the cartesian product of parameter sets.

**What to test in data engineering**
- **Pure transformation functions:** parsing, cleaning, dedupe, business rules. Make them pure, so they're easy to test (see [Q356](/interview/python/#q356), [Q360](/interview/python/#q360)).
- **Edge cases:** NULLs and empties, duplicates, timezone boundaries (DST), late data, unicode, huge values.
- **Contracts:** output schema (columns and dtypes), uniqueness of keys.
- **Integration points:** with mocks or fakes (see [Q383](/interview/python/#q383)), or containers (testcontainers for Postgres).
- **Spark:** a session-scoped local SparkSession fixture, small DataFrames, and `chispa` or `assertDataFrameEqual` (PySpark 3.5+) for comparisons.

**Organisation:** `tests/unit/`, `tests/integration/`, and markers (`@pytest.mark.integration`) to select test types in CI. Use `pytest -x -q` locally and the `--cov` coverage plugin.

**Example**

```python
# Runtime: Python 3.12 with pytest 8.x, pandas 2.2
# src/etl/clean.py
import pandas as pd

def normalise_email(raw: str | None) -> str | None:
    if raw is None or not raw.strip():
        return None
    return raw.strip().lower()

def dedupe_latest(df: pd.DataFrame, key: str, ts: str) -> pd.DataFrame:
    return df.sort_values([key, ts]).drop_duplicates(subset=[key], keep="last").reset_index(drop=True)

# tests/unit/test_clean.py
import pandas as pd
import pytest
from etl.clean import dedupe_latest, normalise_email

@pytest.fixture
def customer_updates() -> pd.DataFrame:
    return pd.DataFrame({"customer_id": [1, 1, 2],
                         "updated_at": pd.to_datetime(["2026-03-01", "2026-03-02", "2026-03-01"]),
                         "segment": ["SMB", "MID", "ENT"]})

@pytest.mark.parametrize("raw,expected", [
    (" A@Example.COM ", "a@example.com"),
    ("", None),
    ("   ", None),
    (None, None),
], ids=["mixed-case-whitespace", "empty", "blank", "none"])
def test_normalise_email(raw, expected):
    assert normalise_email(raw) == expected

def test_dedupe_latest_keeps_newest(customer_updates):
    out = dedupe_latest(customer_updates, "customer_id", "updated_at")
    assert out.set_index("customer_id")["segment"].to_dict() == {1: "MID", 2: "ENT"}
```

**Real-world use case**

A timezone conversion bug only appeared on DST transition days. A parametrized test covering the spring-forward and fall-back dates for three time zones caught it before release, and the cases became part of the regression suite.

**Common mistakes and red flags**

- Tests that hit real production systems.
- One giant test per module.
- Session-scoped fixtures with mutable shared state.

**Follow-up questions**

- How do you share fixtures across files? — `conftest.py`.
- How do you test Spark code fast? — Session-scoped local SparkSession with minimal configs and tiny DataFrames.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q383" data-level="Advanced">
<summary><span class="qb-num">Q383</span><h3 class="qb-title">Mocking external services in tests</h3><span class="qb-meta">Advanced · Common · Testing</span></summary>

**Short answer**

Isolate your logic from APIs, cloud services and databases in unit tests. Use `unittest.mock.patch` / `MagicMock` or pytest's `monkeypatch` for small seams, HTTP mocking libraries (`responses`, `respx`, `requests-mock`) for HTTP calls, `moto` for AWS services, and fakes or test containers for databases. Patch where the name is looked up, assert on the interactions that matter, and keep a few real integration tests too.

**Detailed explanation**

**Options**
| Technique | Use for | Notes |
|---|---|---|
| Dependency injection + fake objects | Your own interfaces (Source/Sink) | Cleanest; no patching (see [Q360](/interview/python/#q360)) |
| `unittest.mock.patch("module.name")` | Replace functions/classes at lookup location | Patch where it's used, not where defined |
| `MagicMock(spec=RealClass)` / `create_autospec` | Mocks that respect real signatures | Catches wrong method names/args |
| `monkeypatch` (pytest) | Env vars, attributes, dict entries | Auto-undo after test |
| `responses` / `requests-mock` (requests), `respx` (httpx) | HTTP APIs: status codes, pagination, errors | Simulate 429/500 for retry tests |
| `moto` | S3, SQS, DynamoDB, etc. in-memory | `@mock_aws` decorator (moto 5+) |
| testcontainers / docker-compose | Real Postgres/Kafka in tests | Integration tests |
| VCR-style recording (`vcrpy`) | Replay recorded HTTP | Watch for secrets in cassettes |

**What to verify**
- **Behaviour**, not implementation detail: the correct records written, and the correct retries on 429.
- **Error paths:** timeouts, 5xx and malformed payloads lead to the correct exceptions and quarantine (see [Q365](/interview/python/#q365), [Q391](/interview/python/#q391)).
- **Interactions** that matter: `mock.assert_called_once_with(...)` for idempotency keys and pagination params.

**Pitfalls**
- **Over-mocking:** tests pass while the real integration is broken. Keep contract and integration tests against sandboxes.
- **Patching the wrong path:** `patch("requests.get")` doesn't affect `from requests import get` already imported in your module. Patch `"mymodule.get"`.
- Mocks without `spec` accept any attribute, so typos pass silently.

**Example**

```python
# Runtime: Python 3.12 with pytest 8.x, responses 0.25+, moto 5.x, boto3
import boto3
import pytest
import requests
import responses
from moto import mock_aws

# --- code under test (src/etl/ingest.py) ---
def fetch_orders(session: requests.Session, since: str) -> list[dict]:
    resp = session.get("https://api.shop.example/orders", params={"since": since}, timeout=10)
    resp.raise_for_status()
    return resp.json()["data"]

def land_raw(s3, bucket: str, key: str, body: bytes) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=body)

# --- tests ---
@responses.activate
def test_fetch_orders_parses_data():
    responses.get("https://api.shop.example/orders",
                  json={"data": [{"id": "o1"}]}, status=200,
                  match=[responses.matchers.query_param_matcher({"since": "2026-03-01"})])
    assert fetch_orders(requests.Session(), "2026-03-01") == [{"id": "o1"}]

@responses.activate
def test_fetch_orders_raises_on_server_error():
    responses.get("https://api.shop.example/orders", status=503)
    with pytest.raises(requests.HTTPError):
        fetch_orders(requests.Session(), "2026-03-01")

@mock_aws
def test_land_raw_writes_object():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="acme-landing")
    land_raw(s3, "acme-landing", "orders/2026-03-01/page-1.json", b'{"data": []}')
    obj = s3.get_object(Bucket="acme-landing", Key="orders/2026-03-01/page-1.json")
    assert obj["Body"].read() == b'{"data": []}'
```

**Real-world use case**

Retry logic was never tested and turned out to retry 401s. Adding `responses`-based tests for 401 (no retry), 429 with `Retry-After` (retry), and 503 (backoff) caught the bug and prevented future regressions.

**Common mistakes and red flags**

- Mocking everything, so there are no integration tests.
- Patching the wrong import path.
- Real network calls in unit tests.

**Follow-up questions**

- How do you mock time? — `freezegun`/`time-machine`, or inject a clock function.
- How do you test Snowflake code? — Unit test SQL generation; integration tests against a dev account/schema or zero-copy clones.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q384" data-level="Intermediate">
<summary><span class="qb-num">Q384</span><h3 class="qb-title">Packaging and environments: venv, pip, pyproject.toml, uv</h3><span class="qb-meta">Intermediate · Common · Tooling</span></summary>

**Short answer**

Isolate each project in a virtual environment (`python -m venv .venv`, or `uv venv`). Declare metadata and dependencies in `pyproject.toml` (PEP 621), pin exact versions with a lock file (`uv.lock`, `poetry.lock`, or `pip-compile` requirements) for reproducible deploys, and build wheels for distribution. `uv` (from Astral) is a fast all-in-one tool that has become popular for creating environments, resolving, locking and running.

**Detailed explanation**

**Concepts**
| Concept | Purpose |
|---|---|
| Virtual environment | Per-project interpreter + site-packages; avoids conflicts with system Python |
| `pyproject.toml` | Standard project metadata, dependencies, build backend, tool configs (ruff, pytest, mypy) |
| Lock file | Exact versions (including transitive) for reproducibility |
| Wheel (`.whl`) | Built distribution, installable without building; used for Databricks/Spark jobs, Airflow plugins |
| Build backend | hatchling, setuptools, poetry-core, uv_build |

**Tooling landscape**
- **pip + venv:** built in. `pip-tools` (`pip-compile`) to generate pinned requirements.
- **uv:** very fast installs and resolution. Commands such as `uv init`, `uv add`, `uv lock`, `uv sync` and `uv run`, plus Python version management (`uv python install`). Also `uv pip ...` for pip-compatible flows.
- **Poetry:** dependency management, locking and publishing with its own CLI.
- **conda / mamba:** useful for non-Python binary dependencies (GDAL, CUDA). Common in data science.
- **pipx / uv tool:** isolated installs of CLI tools (dbt, sqlfluff).

**Practices for data platforms**
- **Pin** dependencies for deployable jobs (Airflow images, Lambda layers, Databricks wheels). Use **ranges** for libraries published to others.
- Match **target runtime versions**: Python version, and libraries preinstalled in Databricks Runtime or Snowpark (Anaconda channel) to avoid conflicts (see [Q234](/interview/databricks/#q234)).
- Separate **dev dependencies** (pytest, ruff, mypy) with dependency groups.
- Build in CI, and run vulnerability scans (`pip-audit`, Dependabot or Renovate).
- Containers: multi-stage builds that install from the lock file for small, reproducible images.

**Example**

```toml
# pyproject.toml
[project]
name = "acme-etl"
version = "0.4.0"
requires-python = ">=3.11"
dependencies = [
  "pandas>=2.2,<3",
  "pyarrow>=17",
  "requests>=2.32,<3",
  "pydantic-settings>=2.4,<3",
]

[dependency-groups]
dev = ["pytest>=8", "ruff>=0.6", "mypy>=1.11", "responses>=0.25", "moto[s3]>=5"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.ruff]
line-length = 110
```

```bash
uv sync                      # create .venv and install from uv.lock (creates lock if missing)
uv run pytest -q             # run inside the project environment
uv add "duckdb>=1.1"         # add dependency and update lock
uv build                     # build sdist + wheel into dist/
```

**Real-world use case**

Airflow tasks failed after a rebuild because an unpinned transitive dependency released a breaking version. Moving to `pyproject.toml` with `uv.lock`, and building images from the lock, made rebuilds reproducible, and upgrades now happen through reviewed Renovate PRs.

**Common mistakes and red flags**

- Installing into system Python.
- Unpinned production dependencies.
- Committing `.venv`.

**Follow-up questions**

- Why wheels for Spark jobs? — Packaged code + metadata installable on clusters; consistent across nodes.
- `requirements.txt` vs `pyproject.toml`? — Requirements is a flat install list; pyproject defines the project and its dependency specs (locks capture exact versions).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q385" data-level="Intermediate">
<summary><span class="qb-num">Q385</span><h3 class="qb-title">Type hints and static checking</h3><span class="qb-meta">Intermediate · Common · Code Quality</span></summary>

**Short answer**

Type hints (`def f(x: int) -> str`, `list[dict[str, Any]]`, `str | None`) document intent and let static checkers such as mypy, pyright or pyrefly catch bugs before runtime: None-handling errors, wrong argument types, missing attributes. They aren't enforced at runtime by default. Add them to public functions and data structures, run a checker in CI, and use runtime validation (Pydantic) at boundaries.

**Detailed explanation**

**Modern syntax (3.10+ / 3.12+)**
- Built-in generics: `list[int]`, `dict[str, float]`, `tuple[int, ...]`.
- Unions: `int | None` (instead of `Optional[int]`).
- `typing` helpers: `Any`, `Literal["dev", "prod"]`, `TypedDict` (typed dict shapes), `Protocol` (structural interfaces; see [Q360](/interview/python/#q360)), `Callable[[int], str]`, `Iterator[T]`, `Final`, `TypeAlias`.
- **Python 3.12 generics syntax:** `def first[T](xs: list[T]) -> T: ...` and `type JSON = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None`.
- `from __future__ import annotations` for postponed evaluation (forward references) on older versions.

**Checkers**
| Tool | Notes |
|---|---|
| mypy | Reference checker; gradual typing; strict mode options |
| pyright / basedpyright | Fast, powers Pylance in VS Code |
| pyrefly, ty | Newer fast Rust-based checkers (evolving) |

**Adoption strategy**
- Start with **public functions** and **data structures** (dataclasses, TypedDicts).
- Enable checks gradually (`--strict` per module), and use `# type: ignore[code]` sparingly with reasons.
- Run the checker in **CI and pre-commit**, alongside a linter (ruff).
- Use **stubs** for untyped libraries (`types-requests`, `pandas-stubs`, `boto3-stubs`).

**Where types help most in DE code**
- None-handling (`str | None` forces checks before `.lower()`).
- Config objects and records passed between stages.
- Refactors across large codebases.
- Clear signatures for shared libraries used by many pipelines.

**Limits:** DataFrame column types aren't captured by standard hints. Use pandera or Pydantic for schema validation, and Polars or pandas typed schemas where available.

**Example**

```python
# Runtime: Python 3.12 (check with: mypy --strict etl_types.py)
from dataclasses import dataclass
from datetime import date
from typing import Iterable, Iterator, Literal, TypedDict

type Env = Literal["dev", "ci", "prod"]

class RawOrder(TypedDict):
    order_id: str
    amount: str
    coupon: str | None

@dataclass(frozen=True, slots=True)
class Order:
    order_id: str
    amount_cents: int
    coupon: str | None
    load_date: date

def parse_orders(rows: Iterable[RawOrder], load_date: date) -> Iterator[Order]:
    for r in rows:
        coupon = r["coupon"].strip().upper() if r["coupon"] else None   # None-safe; mypy enforces
        yield Order(r["order_id"], round(float(r["amount"]) * 100), coupon, load_date)

def target_schema(env: Env) -> str:
    return {"dev": "dbt_dev", "ci": "dbt_ci", "prod": "analytics"}[env]

orders = list(parse_orders([{"order_id": "o1", "amount": "12.30", "coupon": None}], date(2026, 3, 1)))
print(orders, target_schema("prod"))
```
Calling `target_schema("staging")` is flagged by the type checker before it can fail at runtime with a KeyError. In real money pipelines, use `Decimal` rather than float to compute cents.

**Real-world use case**

A refactor changed a function to return `str | None`, and three call sites crashed in production on `.upper()` of None. After mypy was added to CI, a similar change was flagged at PR time with exact line numbers.

**Common mistakes and red flags**

- `Any` everywhere.
- Believing hints validate at runtime.
- Not running a checker at all.

**Follow-up questions**

- TypedDict vs dataclass? — TypedDict types plain dicts (e.g. JSON); dataclass creates real objects.
- How do you type a decorator? — `ParamSpec` and `TypeVar` (`Callable[P, R]`).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q386" data-level="Beginner">
<summary><span class="qb-num">Q386</span><h3 class="qb-title">is vs ==</h3><span class="qb-meta">Beginner · Common · Language Fundamentals</span></summary>

**Short answer**

`==` compares values (it calls `__eq__`). `is` compares identity: whether two names point to the same object in memory. Use `is` only for singletons such as `None` (`x is None`), `True`/`False` in rare explicit cases, and sentinel objects. Never use `is` to compare numbers or strings. Small-integer caching and string interning make it seem to work, sometimes.

**Detailed explanation**

| Comparison | Meaning | Use for |
|---|---|---|
| `a == b` | Equal value | Numbers, strings, lists, dicts, dataclasses |
| `a is b` | Same object | `None`, sentinels, checking aliasing |
| `a is not None` | Idiomatic None check | Optional values |

**Why `is` "works" sometimes**
- CPython caches small ints (-5 to 256), so `a = 256; b = 256; a is b` is True, but with 257 it may be False.
- Some strings are interned (identifier-like literals), so `is` can be True for literals and False for computed strings.
- These are implementation details. Relying on them gives bugs that appear only with certain data. Python warns: `SyntaxWarning: "is" with a literal`.

**None checks**
- `x is None` is correct and fast, and immune to custom `__eq__`.
- `x == None` can be overridden. pandas and NumPy return element-wise arrays, for example, and linters flag it.
- In pandas, use `isna()` / `notna()`. `NaN` is never equal to itself (`NaN == NaN` is False), and `None` in object columns behaves differently from `NaN` in float columns.

**Truthiness vs None:** `if not x:` treats `0`, `""`, `[]` and `None` the same. Use `if x is None:` when 0 or empty are valid values, such as an amount of 0 or an empty string from the source.

**Sentinels:** `_MISSING = object()` distinguishes "not provided" from None in function arguments: `def get(key, default=_MISSING)`, then `if default is _MISSING:`.

**Example**

```python
# Runtime: Python 3.12
import math

a = int("1000"); b = int("1000")
print(a == b, a is b)                 # True False  (value equal, different objects)

def describe(discount: float | None) -> str:
    if discount is None:              # 0.0 is a valid discount, so don't use 'if not discount'
        return "no discount info"
    return f"discount={discount}"

print(describe(0.0), describe(None))  # discount=0.0  no discount info

nan = float("nan")
print(nan == nan, math.isnan(nan))    # False True

_MISSING = object()
def get_setting(cfg: dict, key: str, default=_MISSING):
    if key in cfg:
        return cfg[key]
    if default is _MISSING:
        raise KeyError(key)
    return default
print(get_setting({"a": None}, "a"), get_setting({}, "b", default=None))   # None None
```

**Real-world use case**

A filter used `if not row["discount"]: row["discount"] = DEFAULT`, which overwrote legitimate 0% discounts with the default 5%, and reported revenue was overstated. Changing it to `is None` fixed it.

**Common mistakes and red flags**

- `x is 5` or `name is "abc"`.
- `== None`.
- Truthiness checks where 0 or empty are valid.

**Follow-up questions**

- What does `id()` return? — An integer identity (memory address in CPython) unique for the object's lifetime.
- Why `NaN != NaN`? — IEEE 754 defines NaN as unequal to everything, including itself.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q387" data-level="Intermediate">
<summary><span class="qb-num">Q387</span><h3 class="qb-title">Coding: flatten a nested JSON object</h3><span class="qb-meta">Intermediate · Very Common · Coding Patterns</span></summary>

**Short answer**

Recursively walk the dict. For nested dicts, extend the key path (`parent.child`). For lists, either index them (`items.0.sku`), keep them as JSON strings, or explode them into multiple rows, depending on the requirement. Leaf values are emitted with their full path as the key. Clarify list handling, the separator, key collisions and depth limits with the interviewer before coding. For tabular output, `pandas.json_normalize` handles common cases.

**Detailed explanation**

**Clarify first**
1. **Lists:** index positions, keep as JSON or arrays, or explode into child rows (one row per item)? For analytics, exploding into a child table is usually right (see [Q283](/interview/pyspark/#q283)).
2. **Separator:** `.` or `_`. What if keys already contain the separator?
3. **Empty dicts and lists:** drop them, or keep them as empty values?
4. **Types:** keep native types, or stringify everything?
5. **Depth:** very deep nesting can hit the recursion limit (default about 1000). An iterative stack avoids that.

**Algorithm (dict and index flattening)**
- `flatten(obj, prefix)`:
  - If `obj` is a dict: for each `k, v`, recurse with `prefix + sep + k`.
  - If a list (with indexing): for each `i, v`, recurse with `prefix + sep + str(i)`.
  - Otherwise: `out[prefix] = obj`.
- Time is O(total number of nodes), and output size equals the number of leaves.

**Explode variant:** produce several rows, a parent row of scalar fields plus a list of child rows with a parent key. This matches relational modelling.

**Tools:** `pandas.json_normalize(data, record_path=[...], meta=[...], sep=".")` for record lists with metadata. Spark `explode` and struct selection at scale (see [Q283](/interview/pyspark/#q283)).

**Example**

```python
# Runtime: Python 3.12
from typing import Any

def flatten(obj: Any, sep: str = ".", index_lists: bool = True) -> dict[str, Any]:
    """Iteratively flatten nested dicts/lists into a single-level dict."""
    out: dict[str, Any] = {}
    stack: list[tuple[str, Any]] = [("", obj)]
    while stack:
        prefix, value = stack.pop()
        if isinstance(value, dict) and value:
            for k, v in value.items():
                stack.append((f"{prefix}{sep}{k}" if prefix else str(k), v))
        elif isinstance(value, list) and value and index_lists:
            for i, v in enumerate(value):
                stack.append((f"{prefix}{sep}{i}" if prefix else str(i), v))
        else:
            if prefix in out:
                raise ValueError(f"key collision on '{prefix}'")
            out[prefix] = value
    return dict(sorted(out.items()))

event = {
    "event_id": "e1",
    "user": {"id": 42, "geo": {"country": "IN", "city": "Pune"}},
    "items": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 1}],
    "tags": [],
}
print(flatten(event))
# {'event_id': 'e1', 'items.0.qty': 2, 'items.0.sku': 'A', 'items.1.qty': 1, 'items.1.sku': 'B',
#  'tags': [], 'user.geo.city': 'Pune', 'user.geo.country': 'IN', 'user.id': 42}
```
Empty containers are kept as leaves. The collision check catches inputs like `{"a.b": 1, "a": {"b": 2}}`, which would otherwise overwrite silently.

**Real-world use case**

A webhook ingestion stored flattened event payloads for a schemaless audit table, with arrays kept as JSON strings, and exploded `items` into a separate child table for analytics. The collision check caught a vendor that sent both `meta.source` and a nested `meta` object.

**Common mistakes and red flags**

- Not asking how lists should be handled.
- Recursion without considering depth.
- Silent key collisions.

**Follow-up questions**

- How would you unflatten? — Split keys by separator and build nested dicts (numeric segments → lists if required).
- How would you do this for 100 GB of JSON? — Spark/Polars with explicit schemas and explode, not Python loops.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q388" data-level="Intermediate">
<summary><span class="qb-num">Q388</span><h3 class="qb-title">Coding: deduplicate records by key, keeping the latest</h3><span class="qb-meta">Intermediate · Very Common · Coding Patterns</span></summary>

**Short answer**

Do a single pass with a dict keyed by the record's key. Keep a record if its key is new or its timestamp is newer (with a tiebreaker such as ingestion sequence). That's O(n) time and O(unique keys) memory. In pandas, use `sort_values([key, ts]).drop_duplicates(key, keep="last")`. In SQL or Spark, use `ROW_NUMBER()` (see [Q115](/interview/sql/#q115), [Q295](/interview/pyspark/#q295)).

**Detailed explanation**

**Clarify**
- What defines "latest": an event timestamp, an update timestamp, or a sequence number? How are **ties** broken?
- NULL or missing timestamps: treat them as oldest, or reject?
- Is the input sorted? If so, you can stream with constant memory per key group.
- Do deleted records (tombstones) remove the key, or stay as the latest state?
- Memory constraints: what if there are too many unique keys to hold in memory?

**Algorithm (dict)**
- `best = {}`. For each record `r`: `k = r[key]`. If `k` isn't in `best` or `(r.ts, r.seq) > (best[k].ts, best[k].seq)`, set `best[k] = r`.
- Comparing tuples gives deterministic tiebreaking.
- Output: `best.values()`, optionally sorted.

**Variants**
- **Sorted input (by key):** use `itertools.groupby`, and take the max per group, with O(1) memory per group.
- **Too many keys for memory:** external sort by key (split, sort and merge), a database, DuckDB, or Spark.
- **Streaming dedupe of exact duplicates by event ID:** a bounded time-window set or a Bloom filter (accepting false positives), like watermark-based dedupe in streaming (see [Q290](/interview/pyspark/#q290)).

**Complexity:** O(n) time, O(u) space for u unique keys. A sort-based approach is O(n log n).

**Example**

```python
# Runtime: Python 3.12
from datetime import datetime
from operator import itemgetter
from typing import Iterable

def latest_by_key(records: Iterable[dict], key: str = "customer_id",
                  ts: str = "updated_at", seq: str = "ingest_seq") -> list[dict]:
    best: dict = {}
    for r in records:
        if r.get(ts) is None:
            continue                                   # policy: skip records without timestamp
        k = r[key]
        rank = (r[ts], r.get(seq, 0))
        current = best.get(k)
        if current is None or rank > (current[ts], current.get(seq, 0)):
            best[k] = r
    return sorted(best.values(), key=itemgetter(key))

updates = [
    {"customer_id": 1, "segment": "SMB", "updated_at": datetime(2026, 3, 1, 9), "ingest_seq": 10},
    {"customer_id": 1, "segment": "MID", "updated_at": datetime(2026, 3, 2, 9), "ingest_seq": 11},
    {"customer_id": 2, "segment": "ENT", "updated_at": datetime(2026, 3, 1, 9), "ingest_seq": 12},
    {"customer_id": 2, "segment": "MID", "updated_at": datetime(2026, 3, 1, 9), "ingest_seq": 13},  # tie
    {"customer_id": 3, "segment": "SMB", "updated_at": None, "ingest_seq": 14},
]
for r in latest_by_key(updates):
    print(r["customer_id"], r["segment"])
# 1 MID
# 2 MID   (tie on updated_at, higher ingest_seq wins)
```
Customer 3 is dropped by the "skip without timestamp" policy. Say this policy out loud in an interview, or route such records to quarantine instead.

**Real-world use case**

A CDC landing job deduplicated each micro-batch in Python before a bulk MERGE. Without the ingest-sequence tiebreak, two updates in the same second were applied in arbitrary order. The tuple comparison made results deterministic.

**Common mistakes and red flags**

- O(n²) nested loops.
- No tiebreaker.
- Silently mishandling missing timestamps.

**Follow-up questions**

- What if data doesn't fit in memory? — Sort externally / use DuckDB or Spark window functions.
- How do you dedupe a stream continuously? — State per key with TTL/watermark (Spark `dropDuplicatesWithinWatermark`).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q389" data-level="Beginner">
<summary><span class="qb-num">Q389</span><h3 class="qb-title">Coding: word count and top-k frequent words</h3><span class="qb-meta">Beginner · Very Common · Coding Patterns</span></summary>

**Short answer**

Tokenise the text (lower-case it, then split on non-word characters with a regex), count with `collections.Counter`, and take the top k with `Counter.most_common(k)`, which uses a heap internally for small k. For huge inputs, stream line by line into the counter. Discuss tie-breaking, stopwords, unicode, and how to scale it (MapReduce or Spark word count; see [Q297](/interview/pyspark/#q297)).

**Detailed explanation**

**Steps**
1. **Normalise:** `text.lower()`, and decide on unicode handling (`casefold()` for aggressive case-insensitivity).
2. **Tokenise:** `re.findall(r"[a-z0-9']+", line)`, or `\w+` for unicode words. Handle apostrophes deliberately.
3. **Filter:** stopwords, minimum length, numbers.
4. **Count:** `Counter.update(tokens)` per line, so memory is bounded by the vocabulary, not the text size.
5. **Top-k:** `most_common(k)` is O(n log k) with `heapq.nlargest`. Full sorting is O(n log n).
6. **Ties:** decide on deterministic output (count descending, then word ascending), and implement it explicitly with `heapq.nsmallest(k, items, key=lambda kv: (-kv[1], kv[0]))`.

**Complexity:** O(N) to tokenise and count N tokens, O(V log k) for top-k over V unique words, and O(V) memory.

**Scaling discussion**
- Bigger than one machine: map (emit word, 1) → shuffle by word → reduce (sum). That's Spark `groupBy("word").count()` or `reduceByKey` (see [Q285](/interview/pyspark/#q285), [Q297](/interview/pyspark/#q297)).
- Approximate top-k with limited memory: Count-Min Sketch plus a heap, or Space-Saving / Misra–Gries algorithms for streams.
- Parallel on one machine: split files across processes, count per process, then merge Counters (`c1 + c2` or `update`).

**Example**

```python
# Runtime: Python 3.12
import heapq
import re
from collections import Counter
from typing import Iterable

TOKEN = re.compile(r"[a-z0-9']+")
STOPWORDS = {"the", "a", "an", "and", "of", "to", "is"}

def word_counts(lines: Iterable[str]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for line in lines:
        counts.update(w for w in TOKEN.findall(line.lower()) if w not in STOPWORDS and len(w) > 1)
    return counts

def top_k(counts: Counter[str], k: int) -> list[tuple[str, int]]:
    # deterministic: highest count first, then alphabetical
    return heapq.nsmallest(k, counts.items(), key=lambda kv: (-kv[1], kv[0]))

text = [
    "Timeout error while calling the payments API.",
    "Retry succeeded after timeout; payments API slow.",
    "Error: invalid token. Error persisted.",
]
c = word_counts(text)
print(top_k(c, 3))      # [('error', 3), ('api', 2), ('payments', 2)]
```
`payments` and `timeout` both appear twice. Alphabetical tie-breaking puts `payments` third, before `timeout`.

**Real-world use case**

An on-call script scanned the last hour of error logs line by line and printed the top 10 tokens. That surfaced "timeout" and a specific endpoint name within seconds of a partner outage.

**Common mistakes and red flags**

- `text.split()` leaving punctuation attached.
- Sorting the whole vocabulary for small k.
- Non-deterministic tie order.

**Follow-up questions**

- Complexity of `most_common(k)`? — O(n log k) using a heap when k is given.
- How do you merge results from multiple workers? — Sum Counters (associative, commutative).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q390" data-level="Intermediate">
<summary><span class="qb-num">Q390</span><h3 class="qb-title">Coding: merge overlapping intervals</h3><span class="qb-meta">Intermediate · Common · Coding Patterns</span></summary>

**Short answer**

Sort the intervals by start time, then sweep. If the next interval starts at or before the current merged interval's end, extend the end to the maximum of the two ends. Otherwise, close the current interval and start a new one. That's O(n log n) for the sort and O(n) for the sweep. Clarify whether touching intervals (end == next start) count as overlapping, and whether ends are inclusive.

**Detailed explanation**

**Clarify**
- Are intervals closed `[start, end]` or half-open `[start, end)`? For time ranges in data, half-open is common, so `[1, 3)` and `[3, 5)` touch but don't overlap. Should they merge?
- Is the input already sorted? Can it be empty? Are there invalid intervals (start > end)?
- Should you merge per key (per user or per device)? Then group first, and apply the algorithm within each group.

**Algorithm**
1. Sort by start.
2. `merged = [first]`.
3. For each `(s, e)`: if `s <= merged[-1].end` (or `<` for strict overlap), set `merged[-1].end = max(merged[-1].end, e)`. Otherwise append `(s, e)`.

**Data engineering applications**
- Merging subscription periods or contract coverage periods per customer.
- Combining overlapping maintenance windows or outages to compute total downtime.
- Building SCD2 validity ranges from overlapping source records, and gaps-and-islands problems (see [Q114](/interview/sql/#q114) for the SQL version).
- Sessionisation is a cousin problem: events within a gap threshold merge into sessions (see [Q112](/interview/sql/#q112)).

**Complexity:** O(n log n) time, O(n) output space. Sorting dominates, and it's O(n) if the input is already sorted.

**Example**

```python
# Runtime: Python 3.12
from datetime import datetime

def merge_intervals(intervals: list[tuple], touching_merges: bool = True) -> list[tuple]:
    if not intervals:
        return []
    for s, e in intervals:
        if s > e:
            raise ValueError(f"invalid interval ({s}, {e})")
    ordered = sorted(intervals)
    merged = [list(ordered[0])]
    for s, e in ordered[1:]:
        last = merged[-1]
        overlaps = s <= last[1] if touching_merges else s < last[1]
        if overlaps:
            last[1] = max(last[1], e)
        else:
            merged.append([s, e])
    return [tuple(m) for m in merged]

print(merge_intervals([(1, 3), (2, 6), (8, 10), (10, 12), (15, 18)]))
# [(1, 6), (8, 12), (15, 18)]
print(merge_intervals([(1, 3), (3, 5)], touching_merges=False))
# [(1, 3), (3, 5)]

outages = [(datetime(2026, 3, 1, 10, 0), datetime(2026, 3, 1, 10, 30)),
           (datetime(2026, 3, 1, 10, 20), datetime(2026, 3, 1, 11, 0))]
total_minutes = sum((e - s).total_seconds() / 60 for s, e in merge_intervals(outages))
print(total_minutes)   # 60.0, not 70.0 (overlap counted once)
```

**Real-world use case**

An SLA report summed outage durations from multiple monitors and double-counted overlapping incidents, reporting 140 minutes of downtime when it was really 95. Merging intervals per service before summing fixed the report.

**Common mistakes and red flags**

- Not sorting first.
- Not taking `max` of the ends (a nested interval shrinks the merge).
- Ignoring the touching-interval semantics.

**Follow-up questions**

- How about inserting a new interval into a sorted, merged list? — Binary search position, then merge neighbors (O(n) worst case for shifting).
- How in SQL? — Gaps-and-islands with window functions (see [Q114](/interview/sql/#q114)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q391" data-level="Intermediate">
<summary><span class="qb-num">Q391</span><h3 class="qb-title">Coding: validate records and split good from bad</h3><span class="qb-meta">Intermediate · Common · Data Quality</span></summary>

**Short answer**

Define the validation rules explicitly: required fields, types and parse-ability, ranges, allowed values, formats, and cross-field rules. Run each record through all the rules, collecting every error rather than stopping at the first. Route valid records to the main output and invalid ones, with their reasons and source context, to a quarantine output. Report counts, and fail the run if the bad-record rate exceeds a threshold.

**Detailed explanation**

**Design**
- **Rules as data or functions:** a list of `(name, check_fn, message)`, so rules are testable and reusable. Or use Pydantic models for schema plus field validators (see [Q362](/interview/python/#q362)).
- **Collect all errors per record.** Fixing one at a time is painful for data providers.
- **Keep context:** the source file, line number or offset, the raw value and the rule name, so problems can be traced.
- **Quarantine, don't drop.** Write bad records (raw plus reasons) to a separate file or table for review and replay (see [Q39](/interview/data-engineering/#q39)).
- **Thresholds:** fail the job when the error rate is above, say, 1% or above an absolute count. That signals an upstream contract break rather than a few dirty rows.
- **Metrics:** emit counts per rule (valid, invalid by rule) to logs or monitoring (see [Q380](/interview/python/#q380)).

**At scale**
- Columnar validation (pandera, Great Expectations, Polars expressions, Spark) is much faster than per-row Python for millions of rows.
- In warehouses: dbt tests, constraints and expectations (see [Q222](/interview/databricks/#q222), [Q313](/interview/dbt/#q313)).

**Typical rules**
| Category | Example |
|---|---|
| Presence | `order_id` not empty |
| Type/parse | `amount` parses as Decimal; `created_at` parses as ISO-8601 with timezone |
| Range | `amount >= 0`, `quantity <= 1000` |
| Domain | `currency in {USD, EUR, INR}` |
| Format | email regex, country code length 2 |
| Cross-field | `shipped_at >= created_at` |
| Uniqueness (batch) | duplicate `order_id` within file |

**Example**

```python
# Runtime: Python 3.12
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Callable, Iterable

@dataclass
class Result:
    good: list[dict] = field(default_factory=list)
    bad: list[dict] = field(default_factory=list)

def _is_decimal(v) -> bool:
    try:
        Decimal(str(v)); return True
    except (InvalidOperation, ValueError):
        return False

def _is_iso_ts(v) -> bool:
    try:
        return datetime.fromisoformat(str(v)).tzinfo is not None
    except ValueError:
        return False

RULES: list[tuple[str, Callable[[dict], bool]]] = [
    ("order_id_present", lambda r: bool(str(r.get("order_id") or "").strip())),
    ("amount_is_decimal", lambda r: _is_decimal(r.get("amount"))),
    ("amount_non_negative", lambda r: _is_decimal(r.get("amount")) and Decimal(str(r["amount"])) >= 0),
    ("currency_allowed", lambda r: r.get("currency") in {"USD", "EUR", "INR"}),
    ("created_at_iso_tz", lambda r: _is_iso_ts(r.get("created_at"))),
]

def validate(records: Iterable[dict], source: str, max_bad_ratio: float = 0.01) -> Result:
    res, seen = Result(), set()
    for i, r in enumerate(records, start=1):
        errors = [name for name, check in RULES if not check(r)]
        if r.get("order_id") in seen:
            errors.append("duplicate_order_id_in_batch")
        seen.add(r.get("order_id"))
        (res.bad if errors else res.good).append(
            {"source": source, "row": i, "errors": errors, "raw": r} if errors else r)
    total = len(res.good) + len(res.bad)
    if total and len(res.bad) / total > max_bad_ratio:
        raise ValueError(f"{source}: {len(res.bad)}/{total} bad records exceeds {max_bad_ratio:.0%}")
    return res

batch = [{"order_id": "o1", "amount": "10.00", "currency": "USD", "created_at": "2026-03-01T10:00:00+00:00"},
         {"order_id": "", "amount": "-3", "currency": "GBP", "created_at": "2026-03-01 10:00"}]
try:
    validate(batch, "orders_2026-03-01.csv")
except ValueError as e:
    print(e)          # orders_2026-03-01.csv: 1/2 bad records exceeds 1%
print(validate(batch, "orders_2026-03-01.csv", max_bad_ratio=0.6).bad[0]["errors"])
# ['order_id_present', 'amount_non_negative', 'currency_allowed', 'created_at_iso_tz']
```

**Real-world use case**

A vendor feed started sending timestamps without a timezone. Validation quarantined 100% of the rows and failed the run on the threshold, with a per-rule report showing `created_at_iso_tz`. The vendor fixed the export the same day, and nothing bad reached the warehouse.

**Common mistakes and red flags**

- Stopping at the first error.
- Dropping bad rows silently.
- No threshold, so a 90% bad file "succeeds".

**Follow-up questions**

- How do you replay quarantined records? — Fix source/rules, then reprocess quarantine table through the same pipeline with idempotent writes.
- Row-level vs dataset-level checks? — Row rules here; dataset checks (row counts, distributions) after loading.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q392" data-level="Intermediate">
<summary><span class="qb-num">Q392</span><h3 class="qb-title">Coding: parse log files with regular expressions</h3><span class="qb-meta">Intermediate · Common · Coding Patterns</span></summary>

**Short answer**

Compile one regex with named groups (`(?P<ts>...)`), anchored to the line format, and stream the file line by line, matching each line. Convert the groups to typed fields (datetime, int), count or quarantine lines that don't match, and aggregate as needed (status counts, p95 latency). Keep patterns readable with `re.VERBOSE`, and test them against sample lines, including edge cases.

**Detailed explanation**

**Regex practices**
- **Compile once** at module level: `LOG_RE = re.compile(r"...")`.
- **Named groups** make extraction self-documenting: `m["status"]` or `m.group("status")`.
- **Anchor** with `^` and `$`, or `fullmatch`, to avoid partial matches on malformed lines.
- **Specific character classes** (`\d{3}`, `[^"]*`) instead of greedy `.*`, which avoids catastrophic backtracking.
- `re.VERBOSE` for multi-line patterns with comments.
- `match` (at start) vs `search` (anywhere) vs `fullmatch` (entire string).

**Processing pattern**
1. Stream lines (see [Q377](/interview/python/#q377)).
2. `m = LOG_RE.match(line)`. If there's no match, increment `unparsed` and store a sample.
3. Convert the fields: `datetime.strptime(ts, "%d/%b/%Y:%H:%M:%S %z")`, `int(status)`, `float(latency)`.
4. Aggregate: `Counter` for statuses, lists or histograms for latencies (sampling for percentiles on huge files), per-endpoint stats.

**When not to use regex**
- Structured logs (JSON): use `json.loads` (see [Q380](/interview/python/#q380)).
- Proper CSV: use the `csv` module.
- Very large volumes: push parsing into Spark (`regexp_extract`) or a log platform.

**Security and performance:** user-supplied regex patterns can cause ReDoS (catastrophic backtracking). Don't compile untrusted patterns. Python 3.11 added atomic groups and possessive quantifiers, which help.

**Example**

```python
# Runtime: Python 3.12
import re
from collections import Counter, defaultdict
from datetime import datetime
from statistics import quantiles

ACCESS_RE = re.compile(r"""
    ^(?P<ip>\d{1,3}(?:\.\d{1,3}){3})\s-\s-\s
    \[(?P<ts>[^\]]+)\]\s
    "(?P<method>[A-Z]+)\s(?P<path>\S+)\sHTTP/[\d.]+"\s
    (?P<status>\d{3})\s(?P<bytes>\d+|-)\s
    (?P<latency_ms>\d+(?:\.\d+)?)$
""", re.VERBOSE)

lines = [
    '10.0.0.1 - - [01/Mar/2026:10:00:01 +0000] "GET /api/orders HTTP/1.1" 200 512 35.2',
    '10.0.0.2 - - [01/Mar/2026:10:00:02 +0000] "POST /api/orders HTTP/1.1" 503 0 1200',
    '10.0.0.3 - - [01/Mar/2026:10:00:03 +0000] "GET /api/orders HTTP/1.1" 200 498 41.0',
    'garbage line',
]

status_counts: Counter[int] = Counter()
latency_by_path: dict[str, list[float]] = defaultdict(list)
unparsed = 0
for line in lines:
    m = ACCESS_RE.match(line)
    if not m:
        unparsed += 1
        continue
    _ = datetime.strptime(m["ts"], "%d/%b/%Y:%H:%M:%S %z")
    status_counts[int(m["status"])] += 1
    latency_by_path[f'{m["method"]} {m["path"]}'].append(float(m["latency_ms"]))

print(status_counts, unparsed)                      # Counter({200: 2, 503: 1}) 1
p = quantiles(latency_by_path["GET /api/orders"], n=20, method="inclusive")
print(f"GET p95 ≈ {p[-1]:.1f} ms")
```

**Real-world use case**

During an incident, an engineer parsed 2 GB of load-balancer logs with a compiled regex in a streaming script. It found that 503s were concentrated on one POST endpoint after a deploy, which took 3 minutes, with no need to wait for the log platform's indexing backlog.

**Common mistakes and red flags**

- Compiling the regex inside the loop.
- Greedy `.*` patterns that mis-split fields.
- Ignoring unmatched lines.

**Follow-up questions**

- `match` vs `search`? — `match` anchors at the start; `search` scans anywhere.
- How do you compute percentiles on huge logs? — Streaming sketches (t-digest/HDR histogram) instead of storing all values.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q393" data-level="Advanced">
<summary><span class="qb-num">Q393</span><h3 class="qb-title">Coding: implement an LRU cache</h3><span class="qb-meta">Advanced · Occasional · Coding Patterns</span></summary>

**Short answer**

An LRU (least recently used) cache with O(1) `get` and `put` combines a hash map, for lookup, with recency ordering. In Python, `collections.OrderedDict` provides both: `move_to_end(key)` on access, and `popitem(last=False)` to evict the oldest item when capacity is exceeded. A from-scratch version uses a dict plus a doubly linked list. In real code, use `functools.lru_cache` for function memoisation.

**Detailed explanation**

**Operations**
- `get(key)`: if present, mark it most recent and return the value. Otherwise return a miss (-1 or None).
- `put(key, value)`: insert or update, and mark it most recent. If the size exceeds capacity, evict the least recent item.

**Why a dict plus a doubly linked list:** the dict gives O(1) node lookup. The list gives O(1) move-to-front and remove-tail. `OrderedDict` is implemented similarly and exposes those operations.

**Thread safety:** wrap the operations in a `threading.Lock` if it's shared across threads. `functools.lru_cache` is thread-safe for its internal structure, but may call the function more than once concurrently for the same key.

**Data engineering uses**
- Caching dimension lookups (customer ID to segment) in a streaming enrichment job, to avoid a database call per event.
- Memoising expensive API calls (FX rates per day) inside a batch.
- Bounded dedupe sets ("recently seen IDs") in streaming consumers (see [Q370](/interview/python/#q370)).
- `@functools.lru_cache(maxsize=10_000)` for pure functions. `@cache` is unbounded, so beware memory growth with high-cardinality inputs.
- TTL caches (`cachetools.TTLCache`) when values go stale over time.

**Complexity:** O(1) average for get and put, O(capacity) space.

**Example**

```python
# Runtime: Python 3.12
from collections import OrderedDict
from functools import lru_cache
from threading import Lock
from typing import Generic, Hashable, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")

class LRUCache(Generic[K, V]):
    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._data: OrderedDict[K, V] = OrderedDict()
        self._lock = Lock()
        self.hits = self.misses = 0

    def get(self, key: K) -> V | None:
        with self._lock:
            if key not in self._data:
                self.misses += 1
                return None
            self._data.move_to_end(key)
            self.hits += 1
            return self._data[key]

    def put(self, key: K, value: V) -> None:
        with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            if len(self._data) > self.capacity:
                self._data.popitem(last=False)          # evict least recently used

cache: LRUCache[str, str] = LRUCache(2)
cache.put("c1", "SMB"); cache.put("c2", "ENT")
cache.get("c1")                     # c1 becomes most recent
cache.put("c3", "MID")              # evicts c2
print(cache.get("c2"), cache.get("c1"), cache.hits, cache.misses)   # None SMB 2 1

@lru_cache(maxsize=1024)
def fx_rate(day: str, currency: str) -> float:
    return {"EUR": 0.92, "INR": 83.1}.get(currency, 1.0)    # stand-in for an API call
fx_rate("2026-03-01", "EUR"); fx_rate("2026-03-01", "EUR")
print(fx_rate.cache_info())          # CacheInfo(hits=1, misses=1, maxsize=1024, currsize=1)
```

**Real-world use case**

A Kafka enrichment consumer queried Postgres for each event's merchant category, about 3,000 queries per second. A 50,000-entry LRU cache achieved a 98% hit rate, which cut database load to about 60 queries per second and lowered end-to-end latency.

**Common mistakes and red flags**

- O(n) eviction scans with a list.
- Forgetting to update recency on `get`.
- Unbounded caches in long-running services.

**Follow-up questions**

- How would you add TTL? — Store (value, expires_at) and evict expired on access, or use `cachetools.TTLCache`.
- LRU vs LFU? — Least recently vs least frequently used eviction; LFU suits stable hot keys.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q394" data-level="Beginner">
<summary><span class="qb-num">Q394</span><h3 class="qb-title">Coding: group anagrams / two-sum with dictionaries</h3><span class="qb-meta">Beginner · Common · Coding Patterns</span></summary>

**Short answer**

Both problems use a dict for O(1) lookups. For two-sum, do one pass storing `value → index`, and for each number check whether `target - number` was already seen. That's O(n) instead of O(n²). For group anagrams, map each word to a canonical key, its sorted letters (or a 26-letter count tuple), and append the word to `groups[key]` in a `defaultdict(list)`.

**Detailed explanation**

**Two-sum**
- Brute force checks all pairs, O(n²).
- Hash map: for `i, x` in the list, if `target - x` is in `seen`, return `(seen[target - x], i)`. Otherwise `seen[x] = i`. That's O(n) time and O(n) space.
- Variants: return all pairs (handle duplicates), sorted input (two pointers, O(1) space), or counting pairs (a frequency map).
- Clarify the edge cases: no solution, multiple solutions, using the same element twice (not allowed), negative numbers.

**Group anagrams**
- Key options:
  - `"".join(sorted(word))`: O(m log m) per word of length m.
  - A count tuple over a fixed alphabet: O(m) per word. Works for lowercase a–z only.
- `defaultdict(list)` groups the words. The result is `list(groups.values())`.
- Clarify: case sensitivity, spaces and punctuation, unicode.

**Why interviewers ask these in DE interviews:** they test whether you reach for hashing to turn quadratic scans into linear passes. That's the same instinct needed for joins, dedupe, reconciliation and hash-partitioning in distributed systems.

**Data engineering parallels**
- Two-sum is like finding matching debit and credit entries that net to zero (reconciliation), or pairing request and response events.
- Group anagrams is like grouping records by a normalised key (canonical names, fuzzy-match blocking keys) before deduplication.

**Example**

```python
# Runtime: Python 3.12
from collections import defaultdict

def two_sum(nums: list[int], target: int) -> tuple[int, int] | None:
    seen: dict[int, int] = {}
    for i, x in enumerate(nums):
        j = seen.get(target - x)
        if j is not None:
            return j, i
        seen[x] = i
    return None

def group_anagrams(words: list[str]) -> list[list[str]]:
    groups: dict[str, list[str]] = defaultdict(list)
    for w in words:
        key = "".join(sorted(w.lower().replace(" ", "")))
        groups[key].append(w)
    return list(groups.values())

print(two_sum([3, 8, 11, 4, -2], 9))                    # (2, 4) -> 11 + (-2)
print(two_sum([1, 2, 3], 7))                             # None
print(group_anagrams(["listen", "silent", "enlist", "google", "Elgoog", "cat"]))
# [['listen', 'silent', 'enlist'], ['google', 'Elgoog'], ['cat']]

# Reconciliation flavour: find ledger entries that net to zero
ledger = [("t1", 250), ("t2", -100), ("t3", -250), ("t4", 40)]
by_amount: dict[int, str] = {}
for txn, amt in ledger:
    if -amt in by_amount:
        print("offsetting pair:", by_amount[-amt], txn)  # offsetting pair: t1 t3
    by_amount[amt] = txn
```

**Real-world use case**

A finance reconciliation script compared 400,000 debits against 400,000 credits with nested loops, and ran overnight. A dict keyed by `(account, amount)` matched them in under 2 seconds.

**Common mistakes and red flags**

- Nested loops.
- Using the same element twice.
- Not clarifying normalisation for anagrams.

**Follow-up questions**

- What if input is sorted? — Two pointers from both ends, O(n) time and O(1) space.
- What's the complexity of the sorted-key anagram approach? — O(n · m log m).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q395" data-level="Beginner">
<summary><span class="qb-num">Q395</span><h3 class="qb-title">Coding: batch an iterable into chunks</h3><span class="qb-meta">Beginner · Common · Coding Patterns</span></summary>

**Short answer**

Use `itertools.batched(iterable, n)` (Python 3.12+), which yields tuples of up to n items lazily. On older versions, use a generator with `itertools.islice` that keeps taking n items until the iterator is exhausted. Batching is everywhere in data engineering: bulk inserts, API calls with batch limits, writing Parquet row groups, and controlling memory when processing streams.

**Detailed explanation**

**Approaches**
| Approach | Works on | Notes |
|---|---|---|
| `itertools.batched(it, n)` (3.12+) | Any iterable, lazy | Last batch may be shorter; 3.13 adds `strict=True` to raise if last is short |
| `islice` loop generator | Any iterable, lazy | Portable to older versions |
| Slicing `seq[i:i+n]` | Sequences (lists) only | Requires all data in memory |
| `more_itertools.chunked` | Any iterable | Third-party, many variants |
| pandas `np.array_split`, Polars `iter_slices` | DataFrames | Columnar batching |

**Choosing batch size**
- Respect API limits (for example 100 records per request) and payload size limits.
- Database bulk loads: thousands to tens of thousands of rows per `executemany` or COPY, or better, write files and use COPY (see [Q397](/interview/python/#q397)).
- Memory: batch size × record size must fit comfortably.
- Latency vs throughput: bigger batches mean fewer calls but bigger retries when a batch fails.

**Failure handling:** if a batch fails, decide whether to retry the whole batch (idempotent sink), split it to isolate bad records (binary search on the batch), or quarantine it (see [Q391](/interview/python/#q391)). Track batch numbers for checkpointing and resume (see [Q399](/interview/python/#q399)).

**Example**

```python
# Runtime: Python 3.12
from itertools import batched, islice
from typing import Iterable, Iterator, TypeVar

T = TypeVar("T")

def chunked(iterable: Iterable[T], n: int) -> Iterator[list[T]]:
    """Portable lazy batching (pre-3.12)."""
    if n < 1:
        raise ValueError("n must be >= 1")
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch

ids = range(1, 11)
print(list(batched(ids, 4)))     # [(1, 2, 3, 4), (5, 6, 7, 8), (9, 10)]
print(list(chunked(ids, 4)))     # [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10]]

def send_batches(records: Iterable[dict], batch_size: int = 100) -> int:
    sent = 0
    for batch_no, batch in enumerate(batched(records, batch_size), start=1):
        payload = {"batch": batch_no, "records": list(batch)}
        # client.post("/bulk", json=payload)  # API limit: 100 records per call
        sent += len(payload["records"])
    return sent

print(send_batches(({"id": i} for i in range(250))))    # 250 (3 calls: 100, 100, 50)
```

**Real-world use case**

A CRM sync sent 1 record per API call, 2 million calls in total, and kept hitting rate limits. Batching 200 per call (the API maximum) with `itertools.batched` cut it to 10,000 calls and finished within the nightly window.

**Common mistakes and red flags**

- Slicing generators (not possible).
- Forgetting the last partial batch.
- Loading everything into a list just to slice it.

**Follow-up questions**

- What does `batched` yield? — Tuples (not lists).
- How do you parallelise batches? — Submit batches to a thread pool with bounded in-flight futures (see [Q368](/interview/python/#q368)).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q396" data-level="Intermediate">
<summary><span class="qb-num">Q396</span><h3 class="qb-title">datetime and time zones with zoneinfo</h3><span class="qb-meta">Intermediate · Common · Dates &amp; Times</span></summary>

**Short answer**

Use timezone-aware datetimes. Store and process in UTC (`datetime.now(timezone.utc)`), and convert to local time only for display or business-calendar logic, using `zoneinfo.ZoneInfo("Asia/Kolkata")` (standard library since 3.9). Avoid naive datetimes and `datetime.utcnow()`, which returns a naive value and is deprecated since 3.12. Watch DST gaps and overlaps, and parse with explicit formats or ISO-8601.

**Detailed explanation**

**Naive vs aware**
- **Naive:** no `tzinfo`. Its meaning is ambiguous, and comparing naive with aware datetimes raises `TypeError`.
- **Aware:** carries `tzinfo` (UTC or an IANA zone). Arithmetic and conversion are correct.

**Key APIs**
| Task | Code |
|---|---|
| Now in UTC | `datetime.now(timezone.utc)` |
| Attach zone to wall-clock time | `datetime(2026, 3, 8, 9, 0, tzinfo=ZoneInfo("America/New_York"))` |
| Convert | `dt.astimezone(ZoneInfo("Europe/Berlin"))` |
| Parse ISO-8601 | `datetime.fromisoformat("2026-03-01T10:00:00+05:30")` (3.11+ accepts `Z`) |
| Parse custom | `datetime.strptime(s, "%d/%m/%Y %H:%M")` then `.replace(tzinfo=...)` if the source zone is known |
| Format | `dt.isoformat()`, `dt.strftime(...)` |
| Epoch | `dt.timestamp()`, `datetime.fromtimestamp(ts, tz=timezone.utc)` |

**DST pitfalls**
- **Gap (spring forward):** local times like 02:30 don't exist. ZoneInfo resolves them according to the `fold` rules, so be explicit about how you handle them.
- **Overlap (fall back):** 01:30 occurs twice. `fold=0` gives the first occurrence and `fold=1` the second.
- Adding `timedelta(days=1)` to an aware local datetime keeps the same wall time but may be 23 or 25 hours of elapsed time. For elapsed durations, compute in UTC.
- **Daily partitions by local business day:** convert UTC to the business zone, then take `.date()`. Don't take the UTC date (see [Q140](/interview/sql/#q140)).

**Data engineering conventions**
- Store event timestamps in UTC, with an explicit offset or as `TIMESTAMP_TZ`/`TIMESTAMP_LTZ` depending on the warehouse. Keep the source's local zone in a separate column if needed.
- Document each source's timezone in contracts. Many feeds send local times without an offset.
- Pass dates as `YYYY-MM-DD` and timestamps as ISO-8601 with an offset between systems.
- pandas: `pd.to_datetime(col, utc=True)`, and `.dt.tz_convert("Asia/Kolkata")`.
- Keep the tz database current (`tzdata` package on Windows and in slim containers).

**Example**

```python
# Runtime: Python 3.12 (Windows/slim images: pip install tzdata)
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
IST = ZoneInfo("Asia/Kolkata")

event_utc = datetime.fromisoformat("2026-03-08T06:30:00Z")
print(event_utc.astimezone(NY))                   # 2026-03-08 01:30:00-05:00 (before DST starts)

before = datetime(2026, 3, 7, 12, 0, tzinfo=NY)
after = before + timedelta(days=1)                # same wall time next day
elapsed_h = (after.astimezone(timezone.utc) - before.astimezone(timezone.utc)) / timedelta(hours=1)
print(after, elapsed_h)                           # 2026-03-08 12:00:00-04:00 23.0

ambiguous_first = datetime(2026, 11, 1, 1, 30, tzinfo=NY, fold=0)
ambiguous_second = datetime(2026, 11, 1, 1, 30, tzinfo=NY, fold=1)
print(ambiguous_first.utcoffset(), ambiguous_second.utcoffset())   # -1 day, 20:00:00  -1 day, 19:00:00

order_ts = datetime(2026, 3, 1, 20, 0, tzinfo=timezone.utc)
print(order_ts.date(), order_ts.astimezone(IST).date())            # 2026-03-01 2026-03-02 (IST business day)
```

**Real-world use case**

A daily sales report for India used UTC dates. Orders placed between 00:00 and 05:30 IST landed on the previous day, so end-of-day reconciliation never matched the POS system. Converting to `Asia/Kolkata` before taking the date fixed it.

**Common mistakes and red flags**

- Naive datetimes in pipelines.
- `utcnow()`.
- Fixed offsets (`+05:30`, or `-05:00` for New York) instead of IANA zones for regions with DST.

**Follow-up questions**

- What's `fold`? — Disambiguates repeated wall-clock times during DST fall-back.
- `pytz` vs `zoneinfo`? — zoneinfo is the stdlib standard since 3.9; pytz needs `localize()` and is legacy.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q397" data-level="Intermediate">
<summary><span class="qb-num">Q397</span><h3 class="qb-title">Querying databases from Python safely (parameterised queries, SQLAlchemy)</h3><span class="qb-meta">Intermediate · Common · Databases</span></summary>

**Short answer**

Never build SQL by putting user or data values into strings. Use parameterised queries (`cursor.execute("... WHERE id = %s", (value,))`, or SQLAlchemy `text("... :id")` with params), so the driver sends values separately, which prevents SQL injection and type bugs. Validate identifiers (table and column names) against an allow-list. Use context-managed connections and transactions, batch writes with `executemany` or bulk loaders, and stream large reads.

**Detailed explanation**

**DB-API basics (PEP 249)**
- `conn = driver.connect(...)`, `cur = conn.cursor()`, `cur.execute(sql, params)`, `fetchone/fetchmany/fetchall`, `conn.commit()` / `rollback()`.
- **Paramstyles** differ by driver: `%s` (psycopg, Snowflake connector `pyformat`), `?` (sqlite3), `:name` (Oracle, SQLAlchemy `text`). Check `driver.paramstyle`.
- Parameters can only replace **values**, not identifiers or keywords.

**Identifiers**
- Validate them against an allow-list or a regex (`^[A-Za-z_][A-Za-z0-9_]*$`), or quote them with the driver's identifier helpers (psycopg `sql.Identifier`).
- Build dynamic column lists only from trusted metadata, not from request parameters.

**SQLAlchemy**
- **Engine** (connection pool) plus **Connection** (`with engine.begin() as conn:` gives an automatic commit or rollback).
- `text()` with bound parameters, or the Core expression language (`select(table).where(table.c.id == bindparam("id"))`).
- `pd.read_sql(text(...), conn, params=..., chunksize=...)` for DataFrames, and `df.to_sql(..., method="multi", chunksize=...)` for modest writes.

**Performance**
- **Reads:** `fetchmany(n)` or server-side cursors (psycopg named cursors, `stream_results=True`) for large results. Push filters and aggregations into SQL.
- **Writes:** `executemany` (psycopg 3 pipelines this efficiently) for moderate volumes. For large volumes use bulk paths: Postgres `COPY`, Snowflake `write_pandas` or PUT + COPY, warehouse loaders (see [Q67](/interview/data-engineering/#q67)).
- **Connection pooling**, timeouts, and closing connections (context managers; see [Q358](/interview/python/#q358)).

**Security:** a least-privilege database user, secrets from a secret store (see [Q381](/interview/python/#q381)), and TLS connections.

**Example**

```python
# Runtime: Python 3.12 with SQLAlchemy 2.x, psycopg 3.x
import re
from datetime import date
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg://etl_reader@orders-db.internal:5432/orders",
                       pool_size=5, pool_pre_ping=True,
                       connect_args={"connect_timeout": 10, "sslmode": "require"})

ALLOWED_TABLES = {"orders", "refunds"}
IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")

def recent_rows(table: str, since: date, status: str, limit: int = 1000) -> list[dict]:
    if table not in ALLOWED_TABLES or not IDENT.fullmatch(table):
        raise ValueError(f"table not allowed: {table!r}")
    sql = text(f"""
        SELECT id, customer_id, amount, status, updated_at
        FROM public.{table}
        WHERE updated_at >= :since AND status = :status
        ORDER BY updated_at
        LIMIT :limit
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {"since": since, "status": status, "limit": int(limit)})
        return [dict(row._mapping) for row in result]

# Unsafe (never do this): f"... WHERE status = '{status}'"  -> status = "x' OR '1'='1"
rows = recent_rows("orders", date(2026, 3, 1), "paid")
```
The password comes from the environment or a secrets manager (for example through a `PGPASSWORD` env var or a `.pgpass` file) and isn't embedded in the URL.

**Real-world use case**

An internal reporting tool built WHERE clauses with f-strings from query-string parameters. A security test extracted other teams' data with a crafted `status` value. Switching to bound parameters plus a table allow-list closed the hole.

**Common mistakes and red flags**

- String-formatting values into SQL.
- Trusting identifiers from users.
- Row-by-row inserts for millions of rows.

**Follow-up questions**

- Can you parameterise `IN` lists? — Yes: expanding bind params (`bindparam("ids", expanding=True)`) or `= ANY(:ids)` in Postgres.
- How do you load a DataFrame into Snowflake fast? — `write_pandas` (Parquet + COPY under the hood).

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q398" data-level="Intermediate">
<summary><span class="qb-num">Q398</span><h3 class="qb-title">Working with S3 using boto3</h3><span class="qb-meta">Intermediate · Common · Cloud Storage</span></summary>

**Short answer**

Create a client (`boto3.client("s3")`) using IAM-role credentials, not hard-coded keys. List objects with paginators (`list_objects_v2` returns at most 1,000 keys per call). Upload and download with the managed transfer methods (`upload_file`, `download_file`), which handle multipart uploads and retries. Stream large objects through `get_object()["Body"]`. Use prefixes for partitions, conditional writes and checksums for safety, and retry configuration for throttling.

**Detailed explanation**

**Credentials:** boto3 resolves credentials from the environment, profiles, container or instance roles, or IRSA and workload identity. In production, use **roles**, never keys in code (see [Q86](/interview/data-engineering/#q86)).

**Common operations**
| Task | API |
|---|---|
| List keys | `client.get_paginator("list_objects_v2").paginate(Bucket=..., Prefix=...)` |
| Upload file (multipart automatic) | `client.upload_file(local, bucket, key, ExtraArgs={...})` with `TransferConfig` |
| Download | `client.download_file(bucket, key, local)` |
| Stream read | `obj = client.get_object(...); for line in obj["Body"].iter_lines()` |
| Put small object | `client.put_object(Bucket, Key, Body, ContentType=...)` |
| Metadata | `head_object` (size, ETag, LastModified) |
| Copy | `copy_object` / managed `copy` (server-side) |
| Delete many | `delete_objects` (≤1000 keys per call) |
| Presigned URL | `generate_presigned_url` (time-limited access) |

**Practices**
- **Paginate.** Forgetting to paginate silently processes only the first 1,000 keys.
- **Retries:** `Config(retries={"max_attempts": 10, "mode": "adaptive"})` handles throttling (503 SlowDown).
- **Concurrency:** clients are generally thread-safe, and sessions aren't. Use thread pools for many small objects (see [Q367](/interview/python/#q367)).
- **Atomic publish:** write to a temp prefix, then copy or rename, or write a `_SUCCESS` or manifest file last, so readers only see complete outputs (see [Q399](/interview/python/#q399)). S3 offers conditional writes (`IfNoneMatch="*"`), which prevent overwriting an existing key.
- **Integrity:** checksum algorithms (`ChecksumAlgorithm="SHA256"`). Use ETags carefully, because multipart ETags aren't simple MD5s.
- **Encryption and tagging:** `ServerSideEncryption="aws:kms"`, and object tags for lifecycle rules.
- **Cost:** LIST and GET requests cost money at scale. Avoid listing huge prefixes repeatedly. Use event notifications or inventory reports for big buckets.
- For DataFrames: pyarrow and Polars read `s3://` directly (s3fs or native object-store support).

**Example**

```python
# Runtime: Python 3.12 with boto3 1.3x
from pathlib import Path
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

s3 = boto3.client("s3", config=Config(retries={"max_attempts": 10, "mode": "adaptive"}))
BUCKET = "acme-landing"

def list_keys(prefix: str, suffix: str = "") -> list[str]:
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(suffix)]
    return keys

def upload_partition(local_dir: Path, prefix: str) -> None:
    cfg = TransferConfig(multipart_threshold=64 * 1024 * 1024, max_concurrency=8)
    for f in sorted(local_dir.glob("*.parquet")):
        s3.upload_file(str(f), BUCKET, f"{prefix}/{f.name}", Config=cfg,
                       ExtraArgs={"ServerSideEncryption": "aws:kms", "ChecksumAlgorithm": "SHA256"})
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}/_SUCCESS", Body=b"")      # publish marker last

def count_lines(key: str) -> int:
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"]
    return sum(1 for _ in body.iter_lines())                               # streams, no full download

keys = list_keys("orders/dt=2026-03-01/", suffix=".jsonl")
print(len(keys), sum(count_lines(k) for k in keys[:3]))
```

**Real-world use case**

A reconciliation job reported only 1,000 files processed for a day with 4,300 files, because it used a single `list_objects_v2` call. Switching to a paginator, plus a check against the producer's manifest count, fixed it and prevented recurrence.

**Common mistakes and red flags**

- No pagination.
- Hard-coded access keys.
- Readers consuming partially written prefixes.

**Follow-up questions**

- How do you speed up uploading many small files? — Thread pool + one client, or combine into larger files.
- How do you trigger processing on new objects? — S3 event notifications → SQS/EventBridge/Lambda, or Auto Loader/Snowpipe.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q399" data-level="Advanced">
<summary><span class="qb-num">Q399</span><h3 class="qb-title">Structure of an idempotent, restartable ETL script</h3><span class="qb-meta">Advanced · Common · Pipeline Design</span></summary>

**Short answer**

Parameterise each run by a logical partition (date or window) instead of "now". Read from immutable, replayable inputs. Write outputs atomically, either to a temp location then publish, or as a delete-and-replace or MERGE of exactly that partition, so reruns produce the same result. Checkpoint progress (processed files or batches, watermarks) after each committed unit, so a restart skips completed work. Validate before publishing, and exit with clear status codes.

**Detailed explanation**

**Building blocks**
| Concern | Design |
|---|---|
| Parameters | `--run-date 2026-03-01` (or window start/end) from orchestrator; never `datetime.now()` inside logic (see [Q46](/interview/data-engineering/#q46)) |
| Config & secrets | Validated settings object at startup (see [Q381](/interview/python/#q381)) |
| Inputs | Immutable raw landing (dated prefixes, manifests); explicit file lists |
| Processing | Pure functions (testable), streaming/batched (see [Q356](/interview/python/#q356), [Q377](/interview/python/#q377)) |
| Outputs | Write to `tmp/` or staging table → validate → atomic publish (rename/swap/`replaceWhere`/delete+insert in a transaction) |
| Idempotency | Same parameters → same output; replace the partition instead of appending (see [Q47](/interview/data-engineering/#q47), [Q61](/interview/data-engineering/#q61)) |
| Checkpoints | Record completed units (file keys, batch numbers, watermark) in a control table/file *after* each unit commits |
| Locking | Prevent concurrent runs for same partition (orchestrator `max_active_runs`, DB advisory lock, lock object) |
| Validation | Row counts, schema, key uniqueness, reconciliation with source totals before publish |
| Observability | Structured logs + metrics per step (see [Q380](/interview/python/#q380)) |
| Exit codes | 0 success, non-zero failure; distinguish retryable vs permanent (see [Q365](/interview/python/#q365)) |

**Restart flow**
1. Load the checkpoint for `(job, partition)`.
2. Skip the units already marked complete.
3. Process the remaining units. After each one commits, update the checkpoint.
4. Publish after all units succeed and validation passes. Mark the partition complete.
5. A rerun of a completed partition either no-ops or, with `--force`, rebuilds deterministically.

**Atomic publishing patterns**
- **Files:** write `part-*.parquet` to `_tmp_<run_id>/`, then move or copy to the final prefix, then write `_SUCCESS` last. Readers wait for `_SUCCESS`.
- **Warehouse:** load into a staging table, then `BEGIN; DELETE target WHERE dt = :d; INSERT ... SELECT FROM staging; COMMIT;`, or MERGE, or a table swap.
- **Lakehouse:** Delta or Iceberg atomic commits with `replaceWhere` or partition overwrite (see [Q280](/interview/pyspark/#q280)).

**Example**

```python
# Runtime: Python 3.12 (sqlite3 control table for illustration; use your warehouse/DB in production)
import argparse
import json
import logging
import shutil
import sqlite3
import sys
from datetime import date
from pathlib import Path

log = logging.getLogger("orders_etl")

def init_control(db: sqlite3.Connection) -> None:
    db.execute("""CREATE TABLE IF NOT EXISTS etl_checkpoint (
                    job TEXT, run_date TEXT, unit TEXT, PRIMARY KEY (job, run_date, unit))""")

def done_units(db, job: str, run_date: str) -> set[str]:
    return {u for (u,) in db.execute("SELECT unit FROM etl_checkpoint WHERE job=? AND run_date=?", (job, run_date))}

def mark_done(db, job: str, run_date: str, unit: str) -> None:
    with db:                                             # commits on success
        db.execute("INSERT OR IGNORE INTO etl_checkpoint VALUES (?, ?, ?)", (job, run_date, unit))

def process_file(src: Path, out_dir: Path) -> int:
    rows = [json.loads(l) for l in src.read_text(encoding="utf-8").splitlines() if l.strip()]
    paid = [r for r in rows if r.get("status") == "paid"]
    (out_dir / f"{src.stem}.jsonl").write_text("\n".join(json.dumps(r) for r in paid), encoding="utf-8")
    return len(paid)

def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--run-date", type=date.fromisoformat, required=True)
    p.add_argument("--landing", type=Path, default=Path("landing"))
    p.add_argument("--output", type=Path, default=Path("curated"))
    args = p.parse_args(argv)
    job, rd = "orders_paid", args.run_date.isoformat()

    db = sqlite3.connect("etl_control.db"); init_control(db)
    src_files = sorted((args.landing / f"dt={rd}").glob("*.jsonl"))
    if not src_files:
        log.error("no input files for %s", rd); return 2                  # permanent: nothing to do

    staging = args.output / f"_tmp_dt={rd}"; staging.mkdir(parents=True, exist_ok=True)
    already = done_units(db, job, rd)
    for f in src_files:
        if f.name in already and (staging / f"{f.stem}.jsonl").exists():
            continue                                                   # resume: skip completed unit
        n = process_file(f, staging); mark_done(db, job, rd, f.name)
        log.info("processed %s rows=%s", f.name, n)

    final = args.output / f"dt={rd}"
    if final.exists():
        shutil.rmtree(final)                                           # replace partition (idempotent)
    staging.rename(final)                                              # atomic on same filesystem
    (final / "_SUCCESS").touch()
    log.info("published %s", final)
    return 0

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main(sys.argv[1:]))
```
Removing the final directory and then renaming leaves a short window in which the partition is absent. Where readers can't tolerate that, publish through a pointer, such as a "current" manifest, a view swap, or table-format commits.

**Real-world use case**

A file-processing job crashed at file 380 of 400 and, on rerun, appended duplicates for the first 379 files. The redesigned job checkpoints per file, writes to staging and publishes atomically, so reruns resume at file 380 and the output is byte-identical to a clean run.

**Common mistakes and red flags**

- `now()`-based logic.
- Append-only writes in rerunnable jobs.
- Checkpointing before the unit's output is committed.

**Follow-up questions**

- Why checkpoint after commit, not before? — Otherwise a crash between checkpoint and commit skips a unit forever.
- How do you prevent two runs at once? — Orchestrator concurrency limits plus a lock row/object with expiry.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>

<details class="qb-item" id="q400" data-level="Beginner">
<summary><span class="qb-num">Q400</span><h3 class="qb-title">lambda, map, filter and reduce</h3><span class="qb-meta">Beginner · Common · Functional Programming</span></summary>

**Short answer**

`lambda args: expr` creates a small anonymous function, typically used inline as a `key=` for sorting or a quick callback. `map(f, it)` and `filter(pred, it)` return lazy iterators that apply a function or keep the items where a predicate is true. `functools.reduce(f, it, initial)` folds an iterable into one value. Comprehensions are usually clearer than `map` and `filter` with lambdas, and built-ins like `sum`, `max` and `any` beat `reduce`.

**Detailed explanation**

**lambda**
- Expression only: no statements, annotations or docstrings.
- Good uses: `sorted(rows, key=lambda r: (r["country"], -r["revenue"]))`, `max(items, key=lambda x: x.ts)`, `defaultdict(lambda: 0)`.
- Avoid assigning lambdas to names (`f = lambda x: ...`). Use `def`, which gives a better name in tracebacks (PEP 8 recommends this).
- **Late binding in loops:** `[lambda: i for i in range(3)]` all return 2. Bind with a default argument (`lambda i=i: i`) or use `functools.partial`.

**map / filter**
- Lazy iterators in Python 3. Wrap them in `list()` to materialise.
- `map(str.strip, lines)` is clean with existing functions. `[s.strip() for s in lines]` is equally clear.
- `filter(None, items)` drops falsy values. Careful: it also drops `0` and `""`.

**reduce**
- `reduce(lambda acc, x: acc + x, nums, 0)` is just `sum(nums)`.
- Legitimate uses: combining many DataFrames (`reduce(DataFrame.unionByName, dfs)` in PySpark), composing functions, merging dicts or Counters, applying a sequence of transformations.
- Always provide an **initial value**, so empty inputs don't raise `TypeError`.

**Helpers:** `operator.itemgetter("key")`, `attrgetter("ts")` and `methodcaller("lower")` are often faster and clearer than lambdas. `functools.partial(f, x=1)` pre-binds arguments.

**Connection to distributed processing:** `map` and `reduce` are the conceptual roots of MapReduce and Spark (map → shuffle → reduce; see [Q285](/interview/pyspark/#q285)). Associative reduce functions let partial results combine in any order across partitions.

**Example**

```python
# Runtime: Python 3.12
from collections import Counter
from functools import partial, reduce
from operator import itemgetter

rows = [
    {"country": "IN", "revenue": 120.0, "status": "paid"},
    {"country": "US", "revenue": 300.0, "status": "refunded"},
    {"country": "IN", "revenue": 80.0,  "status": "paid"},
]

ranked = sorted(rows, key=lambda r: (r["country"], -r["revenue"]))
paid_revenue = list(map(itemgetter("revenue"), filter(lambda r: r["status"] == "paid", rows)))
same_with_comprehension = [r["revenue"] for r in rows if r["status"] == "paid"]

monthly_counts = [Counter({"IN": 2}), Counter({"US": 1}), Counter({"IN": 1, "UK": 3})]
total_counts = reduce(lambda a, b: a + b, monthly_counts, Counter())

def apply_tax(amount: float, rate: float) -> float:
    return round(amount * (1 + rate), 2)
with_gst = list(map(partial(apply_tax, rate=0.18), paid_revenue))

funcs = [lambda i=i: i * 10 for i in range(3)]          # default arg avoids late binding
print(ranked[0], paid_revenue == same_with_comprehension,
      total_counts == Counter({"IN": 3, "US": 1, "UK": 3}), with_gst, [f() for f in funcs])
# {'country': 'IN', 'revenue': 120.0, 'status': 'paid'} True True [141.6, 94.4] [0, 10, 20]
```

**Real-world use case**

A PySpark job unioned 365 daily DataFrames with a Python loop of `df = df.union(x)`. Using `functools.reduce(DataFrame.unionByName, dfs)`, with `allowMissingColumns=True` through `partial`, made the intent clear and handled schema drift between days (see [Q272](/interview/pyspark/#q272) for the plan-size caution).

**Common mistakes and red flags**

- Lambda late-binding bugs.
- `reduce` without an initial value.
- `filter(None, ...)` dropping valid zeros.

**Follow-up questions**

- Are map/filter faster than comprehensions? — Sometimes marginally with built-in functions; with lambdas, comprehensions are usually as fast or faster.
- Where did reduce move in Python 3? — To `functools`.

<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>

</details>
