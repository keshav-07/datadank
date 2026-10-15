---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "When would you use threads versus processes in Python data work?"
seoTitle: "Python Threads vs Processes: Interview Answer"
description: "Choose threads for overlapping I/O and processes for CPU-bound parallel work, while considering serialization, memory and the Python runtime."
technology: ["python", "data-engineering"]
topic: ["concurrency", "performance"]
difficulty: "Medium"
questionType: ["conceptual", "scenario"]
estimatedMinutes: 8
interviewRelevance: "Medium"
shortAnswer: "Threads are often useful for I/O-bound work such as concurrent API requests because waiting on network responses can overlap. Processes can use multiple CPU cores for CPU-bound Python work, at the cost of process startup, memory duplication and serialization between workers. In standard CPython builds, the GIL limits simultaneous execution of Python bytecode by threads, although native libraries may release it and free-threaded builds have different behavior. Measure the real workload and cap concurrency to protect the source system."
followUps: ["Why might NumPy threads scale despite the GIL?", "What is the cost of sending a large DataFrame to worker processes?", "How do you bound API concurrency?", "When is async I/O suitable?"]
related: ["articles:python/python-for-data-engineering", "interview-questions:python/api-retries-backoff"]
sources:
  - { label: "Python documentation: threading", url: "https://docs.python.org/3/library/threading.html" }
  - { label: "Python documentation: multiprocessing", url: "https://docs.python.org/3/library/multiprocessing.html" }
versionContext: "GIL behavior depends on Python implementation and build; this answer describes standard GIL-enabled CPython unless stated otherwise."
---

## Match concurrency to the bottleneck

| Workload | Starting choice | Watch for |
|---|---|---|
| Many independent HTTP requests | Bounded threads or async I/O | API rate limits, retries and connection pools |
| Pure-Python CPU-heavy parsing | Processes | Startup, serialization and memory |
| Vectorized native computation | Benchmark threads or library parallelism | Internal thread pools oversubscribing the machine |
| Large distributed transforms | A distributed engine such as Spark | Avoid moving large datasets back into local Python workers |

For API ingestion, concurrency is a source-protection decision as well as a speed decision. Use timeouts, bounded queues, backoff and an explicit request budget. For process pools, pass compact records or paths rather than serializing a giant in-memory dataset for every task.

## Common mistakes

1. Assuming threads speed up all CPU-bound Python code.
2. Spawning unbounded workers against a rate-limited API.
3. Ignoring native-library threads and creating too many layers of parallelism.
