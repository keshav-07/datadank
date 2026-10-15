---
publishedDate: "2026-10-15"
updatedDate: "2026-10-15"
reviewedDate: "2026-10-15"
title: "What is the difference between an iterable and an iterator?"
seoTitle: "Python Iterables vs Iterators: Interview Answer"
description: "An iterable can produce an iterator; an iterator returns values one at a time and maintains state, which matters for streaming large inputs."
technology: ["python"]
topic: ["iteration", "memory"]
difficulty: "Easy"
questionType: ["conceptual", "coding"]
estimatedMinutes: 6
interviewRelevance: "High"
shortAnswer: "An iterable is an object you can obtain an iterator from, usually with `iter(obj)`. An iterator implements `__next__()` and returns one value at a time while retaining its current position; it is generally exhausted after its values are consumed. Lists are iterable and reusable, while a generator is an iterator that computes values lazily. This distinction matters when processing large files or API pages without materializing all records in memory."
followUps: ["Can an iterator be reused after exhaustion?", "How does a generator function create an iterator?", "What does StopIteration mean?", "When would you deliberately materialize an iterator?"]
related: ["interview-questions:python/generators-large-datasets", "articles:python/iterators-generators"]
sources:
  - { label: "Python documentation: Iterator types", url: "https://docs.python.org/3/library/stdtypes.html#iterator-types" }
versionContext: "Python 3 iterator protocol."
---

## Example

```python
rows = ["a", "b"]       # iterable: can create an iterator
it = iter(rows)          # iterator: remembers its position
print(next(it))          # a
print(next(it))          # b
```

Calling `next(it)` again raises `StopIteration`. A generator function produces an iterator:

```python
def read_ids(records):
    for record in records:
        yield record["id"]
```

That lets a transformation pass values through incrementally. If a downstream step needs two passes, either create a fresh iterator from a reusable source or deliberately store a bounded/materialized result. Do not assume every iterable is a list with a known length.

## Common mistakes

1. Calling `len()` on an iterator that does not implement a length.
2. Trying to iterate a one-shot generator a second time and getting no rows.
3. Converting a huge iterator to a list and losing the memory advantage.
