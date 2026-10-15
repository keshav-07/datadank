---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "Why use a context manager when reading files or managing resources?"
seoTitle: "Python Context Managers: Interview Answer"
description: "A context manager guarantees cleanup around a code block, which helps close files, release locks and manage database resources on failure."
technology: ["python"]
topic: ["resources", "reliability"]
difficulty: "Easy"
questionType: ["conceptual", "coding"]
estimatedMinutes: 6
interviewRelevance: "High"
shortAnswer: "A context manager brackets work with setup and cleanup, even when the block raises an exception. The `with` statement calls the manager's entry and exit protocol, so files close, locks release or transactions roll back in a controlled place. In a data pipeline this prevents leaked file handles and connections during partial failures. Use a library's documented manager where available, and do not suppress exceptions unless the recovery behavior is intentional."
followUps: ["What do __enter__ and __exit__ do?", "How does ExitStack help with dynamic resources?", "When should an exception be suppressed?"]
related: ["articles:python/error-handling-logging", "interview-questions:python/exceptions-in-pipelines"]
sources:
  - { label: "Python documentation: The with statement", url: "https://docs.python.org/3/reference/compound_stmts.html#the-with-statement" }
  - { label: "Python documentation: contextlib", url: "https://docs.python.org/3/library/contextlib.html" }
versionContext: "Python 3 context manager protocol."
---

## Example

```python
with open("events.jsonl", encoding="utf-8") as source:
    for line in source:
        process(line)
```

The file is closed when the block exits normally or because an exception is raised. The same structure applies to many database and cloud SDK resources, but transaction semantics vary: inspect whether leaving the context commits, rolls back or only closes the connection.

Use `contextlib.ExitStack` when the number of resources is known only at runtime. For a custom manager, cleanup belongs in `__exit__` or a `finally` block. Avoid broad exception suppression because it can turn a failed load into a misleading success.

## Common mistakes

1. Opening resources without a `finally` or context manager.
2. Assuming every library context manager commits a transaction on exit.
3. Returning true from `__exit__` and accidentally swallowing errors.
