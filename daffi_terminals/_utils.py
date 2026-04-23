"""Shared utilities for daffi-terminals.

Keep this module free of heavy imports so it can be used from both the
router and the worker entry points without adding startup latency.
"""

from __future__ import annotations

import os
import sys


def silence_native_stdio() -> None:
    """Permanently redirect fd 1 and fd 2 to ``/dev/null``."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass

    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(devnull_fd, 1)
        os.dup2(devnull_fd, 2)
    finally:
        os.close(devnull_fd)
