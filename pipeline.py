"""
Async Fetch -> Hash Pipeline

Demonstrates:
- Bounded concurrent I/O with asyncio.Semaphore
- Per-task timeout + exception isolation (asyncio.wait_for)
- gather(..., return_exceptions=True) to keep one failure from killing the batch
- Handoff from I/O-bound async code to CPU-bound work via ProcessPoolExecutor
"""

import asyncio
import hashlib
import httpx

URLS = [
    "https://raw.githubusercontent.com/github/explore/main/topics/python/python.png",
    "https://raw.githubusercontent.com/github/explore/main/topics/rust/rust.png",
    "https://raw.githubusercontent.com/github/explore/main/topics/go/go.png",
    "https://raw.githubusercontent.com/github/explore/main/topics/docker/docker.png",
    "https://raw.githubusercontent.com/github/explore/main/topics/kubernetes/kubernetes.png",
    "https://raw.githubusercontent.com/github/explore/main/topics/nodejs/nodejs.png",
    "https://raw.githubusercontent.com/github/explore/main/topics/does-not-exist/x.png",  # bad path -> exercises error handling
    "https://raw.githubusercontent.com/github/explore/main/topics/java/java.png",
]

MAX_CONCURRENT_DOWNLOADS = 3
PER_REQUEST_TIMEOUT = 8.0


async def download_url(url: str, semaphore: asyncio.Semaphore, client: httpx.AsyncClient):
    async with semaphore:
        try:
            resp = await asyncio.wait_for(client.get(url), timeout=PER_REQUEST_TIMEOUT)
            resp.raise_for_status()
            print(f"[ok] downloaded {url} ({len(resp.content)} bytes)")
            return url, resp.content
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            print(f"[fail] {url} -> {e}")
            return url, e
        except asyncio.TimeoutError:
            print(f"[timeout] {url}")
            return url, asyncio.TimeoutError(f"timeout on {url}")


def hash_bytes(item: tuple[str, bytes]) -> tuple[str, str]:
    url, data = item
    digest = hashlib.sha256(data).hexdigest()
    return url, digest


async def main():
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    async with httpx.AsyncClient() as client:
        tasks = [download_url(url, semaphore, client) for url in URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    successes = [(url, data) for (url, data) in results if isinstance(data, (bytes, bytearray))]
    failures = [(url, data) for (url, data) in results if not isinstance(data, (bytes, bytearray))]

    print(f"\n--- Download summary: {len(successes)} ok, {len(failures)} failed ---")

    if not successes:
        print("Nothing downloaded successfully, skipping CPU stage.")
        return

    print(f"\n--- Hashing {len(successes)} files across processes ---")
    from concurrent.futures import ProcessPoolExecutor

    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor() as executor:
        hash_tasks = [loop.run_in_executor(executor, hash_bytes, item) for item in successes]
        hash_results = await asyncio.gather(*hash_tasks)

    print("\n--- Final results ---")
    for url, digest in hash_results:
        print(f"{url}\n  sha256: {digest}\n")


if __name__ == "__main__":
    asyncio.run(main())
