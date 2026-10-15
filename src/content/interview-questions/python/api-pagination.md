---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "How do you paginate an API safely in a Python ingestion job?"
seoTitle: "Python API Pagination: Interview Answer"
description: "Build reliable API pagination with documented cursors, bounded retries, checkpoints, idempotent writes and explicit rate-limit handling."
technology: ["python", "data-engineering"]
topic: ["apis", "ingestion", "reliability"]
difficulty: "Medium"
questionType: ["architecture", "coding", "scenario"]
estimatedMinutes: 8
interviewRelevance: "High"
shortAnswer: "I follow the API's documented cursor or continuation token rather than assuming page numbers, set connect and read timeouts, honor rate-limit responses, and retry only transient failures with bounded exponential backoff and jitter. Each page is written idempotently with a stable source key, and the continuation state is checkpointed only after the page is durably stored. I also detect repeated cursors, enforce a maximum page or time budget, and record request IDs and progress so the load can resume safely."
followUps: ["When should a 4xx response be retried?", "How do you avoid losing a page after a crash?", "What if the API changes records during pagination?", "How do you handle rate limits?"]
related: ["interview-questions:python/api-retries-backoff", "interview-questions:data-engineering/design-incremental-load"]
sources:
  - { label: "Python documentation: urllib error handling", url: "https://docs.python.org/3/library/urllib.error.html" }
versionContext: "The API's pagination and retry contract is service-specific; Python example code is not tied to a particular vendor."
---

## Failure-safe page processing

```python
cursor = load_checkpoint()
while True:
    page = fetch_page(cursor=cursor, timeout=(5, 30))
    write_page_idempotently(page.items)
    save_checkpoint(page.next_cursor)  # only after durable output
    if page.next_cursor is None:
        break
    if page.next_cursor == cursor:
        raise RuntimeError("API returned the same cursor twice")
    cursor = page.next_cursor
```

The timeout tuple above is illustrative; the HTTP client determines exact syntax. Writing before advancing the cursor means a crash may replay a page, so the sink needs a stable key or deduplication strategy. Advancing first risks skipping data after a crash. If the API mutates records while pages are read, prefer a server-provided snapshot token, stable sort order or overlap-and-deduplicate design.

Do not retry validation errors blindly. Handle `429` using the API's documented retry guidance, cap attempts and total duration, and emit metrics for pages, records, latency and failures.

## Common mistakes

1. Treating page numbers as stable when records can be inserted during pagination.
2. Saving the next cursor before the current page has been persisted.
3. Retrying every error indefinitely and overwhelming a failing API.
