#!/usr/bin/env python3
"""Build the interview question-bank pages from docs/question-bank/data-engineering-interview-master.md.

The master file is the single source of truth: 468 questions in nine sections. This script writes one
Markdown entry per section to src/content/question-banks/<slug>.md. Each question becomes an expandable
<details> block whose id (q1 … q468) is the anchor used by the on-page index and by cross-references,
so "see Q209" links to the right category page. Only presentation changes here; wording edits belong in
the master file.

Usage: python3 scripts/import-question-bank.py
"""
import html
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "docs/question-bank/data-engineering-interview-master.md"
OUT = ROOT / "src/content/question-banks"
DATE = "2026-10-16"

# Section number → page. `slug` is the /interview/<slug>/ URL; hubs that already exist get the bank added.
PAGES = {
    1: dict(slug="data-engineering", label="Data Engineering",
            title="Data Engineering interview questions and answers",
            seoTitle="Data Engineering Interview Questions and Answers",
            description="100 data engineering interview questions with answers: modeling, ETL vs ELT, CDC, Kafka, Airflow, file formats, data quality, governance and cost."),
    2: dict(slug="sql", label="SQL",
            title="SQL interview questions for Data Engineers",
            seoTitle="SQL Interview Questions & Answers for Data Engineers",
            description="50 SQL interview questions for data engineers with answers and runnable queries: joins, window functions, gaps and islands, deduplication and query tuning."),
    3: dict(slug="snowflake", label="Snowflake",
            title="Snowflake interview questions for Data Engineers",
            seoTitle="Snowflake Interview Questions and Answers",
            description="50 Snowflake interview questions with answers: virtual warehouses, micro-partitions, loading, streams and tasks, dynamic tables, Time Travel, RBAC and cost."),
    4: dict(slug="databricks", label="Databricks",
            title="Databricks interview questions for Data Engineers",
            seoTitle="Databricks Interview Questions and Answers",
            description="50 Databricks interview questions with answers: Delta Lake internals, Unity Catalog, Lakeflow pipelines and jobs, Auto Loader, compute choices and cost."),
    5: dict(slug="pyspark", label="PySpark and Spark",
            title="PySpark and Spark interview questions for Data Engineers",
            seoTitle="PySpark and Spark Interview Questions and Answers",
            description="50 PySpark and Apache Spark interview questions with answers: execution model, shuffles, joins, skew, caching, memory tuning and Structured Streaming."),
    6: dict(slug="dbt", label="dbt",
            title="dbt interview questions for Data Engineers",
            seoTitle="dbt Interview Questions and Answers",
            description="50 dbt interview questions with answers: models and materializations, incremental strategies, tests, snapshots, Jinja and macros, CI/CD and governance."),
    7: dict(slug="python", label="Python",
            title="Python interview questions for Data Engineers",
            seoTitle="Python Interview Questions for Data Engineers",
            description="50 Python interview questions for data engineers with answers: generators, concurrency, pandas, error handling, testing, API ingestion and pipeline code."),
    8: dict(slug="scenario-based", label="Scenario-based and system design",
            title="Scenario-based data engineering interview questions",
            seoTitle="Scenario-Based Data Engineering Interview Questions",
            description="50 scenario-based data engineering interview questions with strong sample answers: incidents, late data, duplicates, backfills, migrations and system design."),
    9: dict(slug="behavioral", label="Behavioral",
            title="Behavioral interview questions for Data Engineers",
            seoTitle="Behavioral Interview Questions for Data Engineers",
            description="18 behavioral interview questions for data engineers with STAR-style answers: project walkthroughs, incidents, mistakes, stakeholders and leadership."),
}

# Bank questions that overlap an existing single-question page. The bank answer links to it, so the deeper page
# stays the target for that specific query instead of the two competing (keyword cannibalisation).
DEEP = {
    23: "data-engineering/etl-vs-elt-factors", 25: "data-engineering/design-incremental-load",
    31: "data-engineering/lake-warehouse-lakehouse", 36: "data-engineering/data-quality-checks",
    42: "airflow/choosing-an-executor", 45: "airflow/catchup-and-backfill",
    55: "delta-lake/delta-vs-iceberg-vs-hudi", 62: "kafka/at-least-once-delivery",
    68: "kafka/partitions-and-consumer-groups", 71: "kafka/log-compaction-use-cases",
    87: "data-engineering/cicd-for-data-pipelines",
    103: "sql/inner-vs-left-join", 105: "sql/customers-without-orders-anti-join",
    107: "sql/second-highest-salary", 114: "sql/top-n-per-group-department-salaries",
    115: "sql/remove-duplicate-records", 127: "sql/join-fan-out-double-counting",
    142: "sql/optimize-slow-query", 145: "sql/window-functions-vs-group-by",
    159: "snowflake/time-travel-vs-fail-safe", 161: "snowflake/zero-copy-clone-uses",
    170: "snowflake/streams-tasks-incremental-pipeline",
    202: "delta-lake/what-delta-lake-solves", 204: "delta-lake/delta-merge-internals-conflicts",
    205: "delta-lake/time-travel-use-cases-limits", 206: "delta-lake/vacuum-retention-risks",
    209: "delta-lake/optimize-zorder-vs-liquid-clustering", 212: "delta-lake/schema-evolution-safety",
    218: "databricks/unity-catalog",
    252: "spark/jobs-stages-tasks", 254: "spark/transformation-vs-action", 256: "spark/what-causes-a-shuffle",
    259: "spark/partition-count-performance", 262: "pyspark/broadcast-join", 263: "spark/data-skew",
    270: "pyspark/avoid-python-udfs",
    342: "data-engineering/late-arriving-data",
    351: "python/list-tuple-set", 352: "python/mutable-default-arguments", 353: "python/shallow-vs-deep-copy",
    356: "python/generators-large-datasets", 364: "python/exceptions-in-pipelines",
    374: "python/process-large-csv-limited-memory", 379: "python/api-retries-backoff",
    422: "kafka/consumer-lag-causes-fixes", 427: "airflow/slow-scheduler-stuck-tasks",
}
QDIR = ROOT / "src/content/interview-questions"


def deep_title(path: str) -> str:
    files = list(QDIR.glob(f"{path}.md*"))
    if not files:
        raise SystemExit(f"DEEP map points at a missing page: {path}")
    return re.search(r'^title:\s*"(.*)"', files[0].read_text(encoding="utf-8"), re.M).group(1)


LABELS = {
    "Short Answer (2–3 lines):": "Short answer:",
    "Detailed Explanation:": "Detailed explanation:",
    "Example / Code:": "Example:",
    "Real-World Use Case:": "Real-world use case:",
    "Common Mistakes / Interviewer Red Flags:": "Common mistakes and red flags:",
    "Follow-up Questions Interviewers Ask:": "Follow-up questions:",
    "What the Interviewer Is Testing:": "What the interviewer is testing:",
    "Step-by-Step Approach:": "Step-by-step approach:",
    "Sample Strong Answer:": "Sample strong answer:",
    "Architecture / Flow:": "Architecture and flow:",
    "Alternatives & Trade-offs:": "Alternatives and trade-offs:",
    "Follow-up Questions:": "Follow-up questions:",
}
META = re.compile(r"^\*\*(ID|Topic|Level|Frequency|Tags):\*\*")


def inline(text: str) -> str:
    """Escape a question title for HTML and turn `code` spans into <code>."""
    parts = re.split(r"(`[^`]+`)", text)
    return "".join(
        f"<code>{html.escape(p[1:-1])}</code>" if p.startswith("`") and p.endswith("`") else html.escape(p)
        for p in parts
    )


def parse():
    lines = SRC.read_text(encoding="utf-8").split("\n")
    sections, cur, q = {}, None, None
    fence = False
    for line in lines:
        if line.startswith("```"):
            fence = not fence
        if not fence:
            m = re.match(r"^## Section (\d+):", line)
            if m:
                cur = int(m.group(1))
                sections[cur] = []
                q = None
                continue
            if line.startswith("## Appendix"):
                cur, q = None, None
                continue
            m = re.match(r"^### Q(\d+)\. (.*)$", line)
            if m and cur:
                q = dict(num=int(m.group(1)), title=m.group(2).strip(), raw=[])
                sections[cur].append(q)
                continue
        if q is not None:
            q["raw"].append(line)
    for qs in sections.values():
        for q in qs:
            meta = {}
            body = []
            for line in q["raw"]:
                if META.match(line):
                    for key, val in re.findall(r"\*\*(ID|Topic|Level|Frequency|Tags):\*\*\s*([^|]*)", line):
                        meta[key] = val.strip()
                else:
                    body.append(line)
            while body and body[-1].strip() in ("", "---"):
                body.pop()
            while body and not body[0].strip():
                body.pop(0)
            q.update(
                id=meta.get("ID", ""),
                topic=(meta.get("Topic", "").split(">")[-1].strip() or None),
                level=meta.get("Level", ""),
                frequency=meta.get("Frequency", ""),
                tags=re.findall(r"`([^`]+)`", meta.get("Tags", "")),
                body=body,
            )
    return sections


RUN = re.compile(r"Q\d{1,3}(?:\s*(?:,|–|/|and)\s*Q\d{1,3})*")


def link_refs(text: str, page_of: dict) -> str:
    """Link cross-references such as "(see Q31)", "see Q208, Q209", "covered in Q13" or "Q450 covers" to the
    question's anchor. Other Q-numbers (quarters such as Q1 = 100 in an example) stay plain. Skips inline code."""

    def one(m):
        n = int(m.group(1))
        return f"[Q{n}](/interview/{page_of[n]}/#q{n})" if n in page_of else m.group(0)

    def run(m):
        before = m.string[max(0, m.start() - 14) : m.start()]
        after = m.string[m.end() : m.end() + 8]
        if re.search(r"(?:\b[Ss]ee(?: also)?|covered in|are in)\s*$", before) or after.startswith(" covers"):
            return re.sub(r"Q(\d{1,3})", one, m.group(0))
        return m.group(0)

    out = []
    for part in re.split(r"(`[^`]*`)", text):
        out.append(part if part.startswith("`") else RUN.sub(run, part))
    return "".join(out)


def render_body(body, page_of):
    out, fence = [], False
    for line in body:
        if line.startswith("```"):
            fence = not fence
            out.append(line)
            continue
        if fence:
            out.append(line)
            continue
        label_only = False
        for old, new in LABELS.items():
            if line.startswith(f"**{old}**"):
                rest = line[len(old) + 4 :]
                label_only = not rest.strip()
                line = f"**{new.rstrip(':') if label_only else new}**" + rest
        out.append(link_refs(line, page_of))
        if label_only:
            out.append("")  # a label on its own line is its own paragraph, not the start of the next one
    return "\n".join(out)


# Same rule as src/utils/validation.ts, applied to prose only: code such as seen[x] = i is legitimate.
PLACEHOLDER = re.compile(
    r"lorem ipsum|\bTODO\b|\bTBD\b|\[(?:X|N)\s*%?\](?!\()|\[\d+\s*%\](?!\()|\[(?:company|your)[^\]]*\](?!\()", re.I
)


def prose(body):
    text = re.sub(r"```.*?```", "", "\n".join(body), flags=re.S)
    return re.sub(r"`[^`]*`", "", text)


def main():
    sections = parse()
    bad = [f"Q{q['num']}" for qs in sections.values() for q in qs if PLACEHOLDER.search(prose(q["body"]) + q["title"])]
    if bad:
        raise SystemExit(f"Placeholder text in prose: {', '.join(bad)}")
    nums = [q["num"] for qs in sections.values() for q in qs]
    if nums != list(range(1, len(nums) + 1)):
        raise SystemExit("Question numbers are not sequential from Q1")
    page_of = {q["num"]: PAGES[s]["slug"] for s, qs in sections.items() for q in qs}
    OUT.mkdir(parents=True, exist_ok=True)
    for s, qs in sections.items():
        page = PAGES[s]
        levels = {lv: sum(q["level"] == lv for q in qs) for lv in ("Beginner", "Intermediate", "Advanced")}
        fm = {
            "title": page["title"],
            "seoTitle": page["seoTitle"],
            "description": page["description"],
            "hub": page["slug"],
            "label": page["label"],
            "section": s,
            "publishedDate": DATE,
            "updatedDate": DATE,
            "levels": levels,
            "questions": [
                {"num": q["num"], "id": q["id"], "title": q["title"], "topic": q["topic"],
                 "level": q["level"], "frequency": q["frequency"]}
                for q in qs
            ],
        }
        parts = []
        for q in qs:
            meta = " · ".join(x for x in (q["level"], q["frequency"], q["topic"]) if x)
            parts.append(
                f'<details class="qb-item" id="q{q["num"]}" data-level="{q["level"]}">\n'
                f'<summary><span class="qb-num">Q{q["num"]}</span><h3 class="qb-title">{inline(q["title"])}</h3>'
                f'<span class="qb-meta">{html.escape(meta)}</span></summary>\n\n'
                f'{render_body(q["body"], page_of)}\n\n'
                + (f'**In depth:** [{deep_title(DEEP[q["num"]])}](/interview/{DEEP[q["num"]]}/)\n\n' if q["num"] in DEEP else "")
                + 
                f'<p class="qb-back"><a href="#qb-index">Back to the question list</a></p>\n\n'
                "</details>\n"
            )
        text = "---\n" + "\n".join(f"{k}: {json.dumps(v, ensure_ascii=False)}" for k, v in fm.items()) + "\n---\n\n"
        text += "\n".join(parts)
        (OUT / f'{page["slug"]}.md').write_text(text, encoding="utf-8")
        print(f'{page["slug"]}: {len(qs)} questions, {len(text) // 1024} KB')


if __name__ == "__main__":
    main()
