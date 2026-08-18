# async-fetch-hash-pipeline

Downloads a batch of files concurrently, then hashes each one across
worker processes. Built to demonstrate correct use of Python's
concurrency primitives, not just syntax familiarity.

## What it demonstrates

- **Bounded concurrency**: `asyncio.Semaphore` caps simultaneous
  downloads instead of firing everything at once.
- **Per-task timeout + isolation**: `asyncio.wait_for` per request,
  combined with `gather(..., return_exceptions=True)` so one bad URL
  doesn't kill the batch.
- **I/O-bound vs CPU-bound separation**: downloads run on the async
  event loop; hashing (CPU-bound) is offloaded to a
  `ProcessPoolExecutor` via `loop.run_in_executor`, so the CPU work
  doesn't block the event loop and actually uses multiple cores.

## Run it

```bash
pip install httpx
python pipeline.py
```

## Why this design

Async gives you concurrency for I/O-bound work (waiting on network
responses), but it doesn't help with CPU-bound work (hashing) — that
still blocks the single event loop thread. This pipeline pairs
asyncio for the fetch stage with a process pool for the compute
stage, which is the correct pattern when a workload has both.
