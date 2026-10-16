#!/usr/bin/env python3
"""Build the interview question-bank articles from docs/question-bank/data-engineering-interview-master.md.

The master file is the single source of truth: 468 questions in nine sections. Each section becomes one
article in src/content/articles/ (rendered in the normal lesson layout). Every question is an <h3> with its
short answer visible and the full answer in an expandable block. Questions are grouped by level and numbered
1..N on each page; "see Q209" references become links to the referenced question's heading on its own page.
Only presentation changes here; wording edits belong in the master file. Also writes
src/data/question-banks.json, which the interview hubs and the interview overview use to link the pages.

Usage: python3 scripts/import-question-bank.py
"""
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "docs/question-bank/data-engineering-interview-master.md"
ARTICLES = ROOT / "src/content/articles"
DATA = ROOT / "src/data/question-banks.json"
DATE = "2026-10-16"

# Section number -> article. `hub` is the /interview/<hub>/ practice page that links to it (if any).
PAGES = {
    1: dict(folder="career", slug="data-engineering-interview-questions", hub="data-engineering",
            label="Data Engineering", noun="data engineering interview questions",
            technology=["career", "data-engineering"], section="career",
            title="Top 100 Data Engineering Interview Questions and Answers",
            seoTitle="Top 100 Data Engineer Interview Questions & Answers",
            description="Top 100 data engineering interview questions with answers: data modeling, ETL vs ELT, CDC, Kafka, Airflow, file formats, data quality and cost.",
            covers="data modeling, ETL and ELT, batch and streaming, CDC, Kafka, Airflow, file and table formats, data quality, governance, cloud services and cost"),
    2: dict(folder="sql", slug="sql-interview-questions", hub="sql", label="SQL", noun="SQL interview questions",
            technology=["sql", "data-engineering"], section="learn",
            title="Top 50 SQL Interview Questions and Answers for Data Engineers",
            seoTitle="Top 50 SQL Interview Questions for Data Engineers",
            description="Top 50 SQL interview questions for data engineers with answers and queries: joins, window functions, gaps and islands, deduplication and tuning.",
            covers="joins, aggregation, window functions, gaps and islands, deduplication, CTEs, pivots, NULL handling, transactions and query optimisation"),
    3: dict(folder="snowflake", slug="snowflake-interview-questions", hub="snowflake", label="Snowflake",
            noun="Snowflake interview questions", technology=["snowflake", "data-engineering"], section="learn",
            title="Top 50 Snowflake Interview Questions and Answers",
            seoTitle="Top 50 Snowflake Interview Questions and Answers",
            description="Top 50 Snowflake interview questions with answers: virtual warehouses, micro-partitions, loading, streams and tasks, Time Travel, RBAC and cost.",
            covers="architecture, virtual warehouses, micro-partitions and performance, loading, streams and tasks, dynamic tables, Time Travel, security, sharing and cost"),
    4: dict(folder="databricks", slug="databricks-interview-questions", hub="databricks", label="Databricks",
            noun="Databricks interview questions", technology=["databricks", "delta-lake", "data-engineering"],
            section="learn", title="Top 50 Databricks Interview Questions and Answers",
            seoTitle="Top 50 Databricks Interview Questions and Answers",
            description="Top 50 Databricks interview questions with answers: Delta Lake internals, Unity Catalog, Lakeflow pipelines and jobs, Auto Loader and compute.",
            covers="Delta Lake internals and maintenance, Unity Catalog, Lakeflow Declarative Pipelines and Jobs, Auto Loader, compute choices, streaming and cost"),
    5: dict(folder="pyspark", slug="pyspark-interview-questions", hub="pyspark", label="PySpark and Spark",
            noun="PySpark and Spark interview questions", technology=["pyspark", "spark", "data-engineering"],
            section="learn", title="Top 50 PySpark and Spark Interview Questions and Answers",
            seoTitle="Top 50 PySpark Interview Questions and Answers",
            description="Top 50 PySpark and Apache Spark interview questions with answers: execution model, shuffles, joins, skew, caching, memory and Structured Streaming.",
            covers="the execution model, partitions and shuffles, join strategies, skew, caching, memory and resource tuning, AQE and Structured Streaming"),
    6: dict(folder="etl-elt", slug="dbt-interview-questions", hub="dbt", label="dbt", noun="dbt interview questions",
            technology=["etl-elt", "dbt", "data-engineering"], section="learn",
            title="Top 50 dbt Interview Questions and Answers",
            seoTitle="Top 50 dbt Interview Questions and Answers",
            description="Top 50 dbt interview questions with answers: models and materializations, incremental models, tests, snapshots, Jinja and macros, and CI/CD.",
            covers="models and materializations, incremental strategies, tests, snapshots, sources, Jinja and macros, project design, CI/CD and governance"),
    7: dict(folder="python", slug="python-interview-questions", hub="python", label="Python",
            noun="Python interview questions", technology=["python", "data-engineering"], section="learn",
            title="Top 50 Python Interview Questions for Data Engineers",
            seoTitle="Top 50 Python Interview Questions for Data Engineers",
            description="Top 50 Python interview questions for data engineers with answers: data structures, generators, concurrency, pandas, testing and API ingestion.",
            covers="data structures, iterators and generators, error handling, concurrency, pandas, file I/O, testing, API ingestion and coding patterns"),
    8: dict(folder="career", slug="scenario-based-data-engineering-interview-questions", hub="scenario-based",
            label="Scenario-based", noun="scenario-based questions", technology=["career", "data-engineering", "system-design"],
            section="career", title="50 Scenario-Based Data Engineering Interview Questions and Answers",
            seoTitle="50 Scenario-Based Data Engineer Interview Questions",
            description="50 scenario-based data engineering interview questions with sample answers: failed pipelines, late data, duplicates, backfills, migrations and design.",
            covers="incidents, late-arriving data, duplicates, backfills, cost spikes, migrations and system design scenarios with concrete numbers"),
    9: dict(folder="career", slug="behavioral-interview-questions", hub="behavioral", label="Behavioral",
            noun="behavioral questions", technology=["career", "data-engineering"], section="career",
            title="18 Behavioral Interview Questions for Data Engineers (with STAR Answers)",
            seoTitle="Behavioral Interview Questions for Data Engineers",
            description="18 behavioral interview questions for data engineers with STAR-style answers: project walkthroughs, incidents, mistakes, stakeholders and leadership.",
            covers="project walkthroughs, incidents, mistakes, conflict, prioritisation, stakeholders, mentoring and leadership"),
}

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
    "Detailed Explanation:": "Detailed explanation",
    "Example / Code:": "Example",
    "Real-World Use Case:": "Real-world use case",
    "Common Mistakes / Interviewer Red Flags:": "Common mistakes and red flags",
    "Follow-up Questions Interviewers Ask:": "Follow-up questions",
    "What the Interviewer Is Testing:": "What the interviewer is testing",
    "Step-by-Step Approach:": "Step-by-step approach",
    "Sample Strong Answer:": "Sample strong answer",
    "Architecture / Flow:": "Architecture and flow",
    "Alternatives & Trade-offs:": "Alternatives and trade-offs",
    "Follow-up Questions:": "Follow-up questions",
}
META = re.compile(r"^\*\*(ID|Topic|Level|Frequency|Tags):\*\*")
LEVELS = ["Beginner", "Intermediate", "Advanced"]
LEVEL_SUFFIX = {"Beginner": " (freshers)", "Intermediate": "", "Advanced": " (experienced)"}


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
                # Command-line flags in titles (--full-refresh, --defer) become code, so typography does not
                # turn "--" into a dash.
                title = re.sub(r"(?<![`\w-])(--[a-z][\w-]*)", r"`\1`", m.group(2).strip())
                q = dict(num=int(m.group(1)), title=title, raw=[])
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


def slugs(texts):
    """Heading ids exactly as Astro generates them (github-slugger, one slugger per document)."""
    js = "import S from 'github-slugger';const s=new S();const t=JSON.parse(require('fs').readFileSync(0,'utf8'));console.log(JSON.stringify(t.map(x=>s.slug(x))))"
    js = js.replace("require('fs')", "(await import('fs'))")
    out = subprocess.run(["node", "--input-type=module", "-e", js], input=json.dumps(texts), capture_output=True,
                         text=True, cwd=ROOT, check=True)
    return json.loads(out.stdout)


def plain(title: str) -> str:
    return title.replace("`", "")


RUN = re.compile(r"Q\d{1,3}(?:\s*(?:,|–|/|and)\s*Q\d{1,3})*")


def link_refs(text: str, target: dict, here: str) -> str:
    """Turn cross-references ("(see Q31)", "see Q208, Q209", "covered in Q13", "Q450 covers") into links whose
    text is the referenced question. Other Q-numbers (quarters such as Q1 = 100) stay plain. Skips inline code."""

    def one(m):
        n = int(m.group(1))
        if n not in target:
            return m.group(0)
        url, title = target[n]
        return f"[{title}]({url.replace(here, '') if url.startswith(here + '#') else url})"

    def run(m):
        before = m.string[max(0, m.start() - 14) : m.start()]
        after = m.string[m.end() : m.end() + 8]
        if re.search(r"(?:\b[Ss]ee(?: also)?|covered in|are in)\s*$", before) or after.startswith(" covers"):
            return re.sub(r"Q(\d{1,3})", one, m.group(0))
        return m.group(0)

    return "".join(p if p.startswith("`") else RUN.sub(run, p) for p in re.split(r"(`[^`]*`)", text))


def transform(lines, target, here):
    out, fence = [], False
    for line in lines:
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
                line = f'<p class="qb-label">{new.rstrip(":")}</p>\n' if label_only else f"**{new}**" + rest
        out.append(link_refs(line, target, here))
    return "\n".join(out).strip()


def split_visible(body):
    """Visible part: the short answer (standard questions) or the scenario (Section 8). The rest expands."""
    text = body
    start = next((i for i, l in enumerate(text) if l.startswith("**Short Answer") or l.startswith("**Scenario:**")), None)
    if start is None:
        return [], text
    end = next((i for i in range(start + 1, len(text)) if re.match(r"^\*\*[A-Z][^*]*:\*\*", text[i])), len(text))
    visible = text[start:end]
    return visible, text[:start] + text[end:]


PLACEHOLDER = re.compile(
    r"lorem ipsum|\bTODO\b|\bTBD\b|\[(?:X|N)\s*%?\](?!\()|\[\d+\s*%\](?!\()|\[(?:company|your)[^\]]*\](?!\()", re.I
)


def prose(body):
    text = re.sub(r"```.*?```", "", "\n".join(body), flags=re.S)
    return re.sub(r"`[^`]*`", "", text)


def main():
    sections = parse()
    nums = [q["num"] for qs in sections.values() for q in qs]
    if nums != list(range(1, len(nums) + 1)):
        raise SystemExit("Question numbers are not sequential from Q1")
    bad = [f"Q{q['num']}" for qs in sections.values() for q in qs if PLACEHOLDER.search(prose(q["body"]) + q["title"])]
    if bad:
        raise SystemExit(f"Placeholder text in prose: {', '.join(bad)}")

    # Order each page by level (source order within a level), number 1..N, and compute heading anchors.
    plan, target = {}, {}
    for s, qs in sections.items():
        page = PAGES[s]
        url = f"/{page['folder']}/{page['slug']}/"
        ordered = [q for lv in LEVELS for q in qs if q["level"] == lv]
        assert len(ordered) == len(qs), f"section {s}: unknown level"
        groups = [lv for lv in LEVELS if any(q["level"] == lv for q in ordered)]
        h2 = ["Question list"] + [f"{lv} {page['noun']}{LEVEL_SUFFIX[lv]}" for lv in groups]
        texts, k = [h2[0]], 0
        for lv, heading in zip(groups, h2[1:]):
            texts.append(heading)
            for q in [q for q in ordered if q["level"] == lv]:
                k += 1
                q["n"] = k
                texts.append(f"{k}. {plain(q['title'])}")
        ids = slugs(texts)
        anchor = dict(zip(texts, ids))
        for q in ordered:
            q["anchor"] = anchor[f"{q['n']}. {plain(q['title'])}"]
            target[q["num"]] = (f"{url}#{q['anchor']}", q["title"])
        plan[s] = dict(page=page, url=url, ordered=ordered, groups=groups, h2=h2, h2ids=[anchor[h] for h in h2])

    index = []
    for s, p in plan.items():
        page, ordered = p["page"], p["ordered"]
        levels = {lv: sum(q["level"] == lv for q in ordered) for lv in LEVELS}
        related = [f"interview-questions:{DEEP[q['num']]}" for q in ordered if q["num"] in DEEP][:12]
        fm = {
            "title": page["title"],
            "seoTitle": page["seoTitle"],
            "description": page["description"],
            "technology": page["technology"],
            "topic": ["interview-questions"],
            "difficulty": "Intermediate",
            "section": page["section"],
            "publishedDate": DATE,
            "updatedDate": DATE,
            "reviewedDate": DATE,
            "related": related,
            "versionContext": "Product names and defaults as of October 2026; code snippets state their dialect or runtime",
        }
        n = len(ordered)
        out = [
            f"These {n} {page['noun']} cover {page['covers']}. They are grouped from beginner (freshers) to advanced "
            "(experienced). Each one starts with the answer you would give in the first 20 seconds; open the full "
            "answer for the detailed explanation, an example, how it shows up in real work, common mistakes and the "
            "follow-up questions interviewers ask.",
            "",
            f"## {p['h2'][0]}",
            "",
            f'<details class="qb-index" open>\n<summary>All {n} questions</summary>\n',
        ]
        out += [f"{q['n']}. [{q['title']}](#{q['anchor']})" for q in ordered]
        out += ["", "</details>", ""]
        for lv, heading in zip(p["groups"], p["h2"][1:]):
            out += [f"## {heading}", ""]
            for q in [q for q in ordered if q["level"] == lv]:
                meta = " · ".join(x for x in (q["level"], q["frequency"], q["topic"]) if x)
                visible, more = split_visible(q["body"])
                out += [f"### {q['n']}. {q['title']}", "", f'<p class="qb-meta">{meta}</p>', ""]
                if visible:
                    out += [transform(visible, target, p["url"]), ""]
                summary = "Sample answer and approach" if s == 8 else "Full answer: explanation, example and follow-ups"
                out += [f'<details class="qb-more">\n<summary>{summary}</summary>\n', transform(more, target, p["url"]), ""]
                if q["num"] in DEEP:
                    out += [f"**In depth:** [{deep_title(DEEP[q['num']])}](/interview/{DEEP[q['num']]}/)", ""]
                out += ["</details>", ""]
        path = ARTICLES / page["folder"] / f"{page['slug']}.md"
        path.write_text("---\n" + "\n".join(f"{k}: {json.dumps(v, ensure_ascii=False)}" for k, v in fm.items())
                        + "\n---\n\n" + "\n".join(out), encoding="utf-8")
        index.append(dict(hub=page["hub"], label=page["label"], title=page["title"], url=p["url"],
                          ref=f"articles:{page['folder']}/{page['slug']}", count=n, levels=levels))
        print(f"{p['url']}: {n} questions, {path.stat().st_size // 1024} KB")
    DATA.write_text(json.dumps(index, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
