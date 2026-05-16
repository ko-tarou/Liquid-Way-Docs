"""Lightweight inference metrics: tok/s, token count, elapsed, RSS memory."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager


def rss_mb() -> float:
    """Resident set size of the current process in MB.

    Uses ``resource`` (stdlib, always available on macOS/Linux). On macOS
    ``ru_maxrss`` is in bytes; on Linux it is in kilobytes.
    """
    import resource

    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if os.uname().sysname == "Darwin":
        return maxrss / (1024 * 1024)
    return maxrss / 1024


@contextmanager
def timer():
    """Context manager yielding a callable that returns elapsed seconds."""
    start = time.perf_counter()
    elapsed = {"value": 0.0}

    def read() -> float:
        return elapsed["value"]

    try:
        yield read
    finally:
        elapsed["value"] = time.perf_counter() - start


def build_metrics(generated_tokens: int, elapsed_s: float) -> dict:
    """Assemble a metrics dict from a generation run."""
    tok_per_s = generated_tokens / elapsed_s if elapsed_s > 0 else 0.0
    return {
        "generated_tokens": generated_tokens,
        "elapsed_s": round(elapsed_s, 3),
        "tok_per_s": round(tok_per_s, 2),
        "rss_mb": round(rss_mb(), 1),
    }
