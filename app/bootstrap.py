"""Bootstrap utilities for the standalone MCP service."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional


def _candidate_roots(marker: str) -> list[Path]:
    """Generate possible repository roots that might contain `marker`."""
    candidates: list[Path] = []

    env_root = os.getenv("YMD_REPO_ROOT")
    if env_root:
        candidates.append(Path(env_root).expanduser().resolve())

    current = Path(__file__).resolve()
    candidates.extend([current] + list(current.parents))
    candidates.append(Path.cwd().resolve())
    candidates.extend(Path.cwd().resolve().parents)

    unique: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path)
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return unique


def ensure_repo_root(marker: str = "ymda") -> Path:
    """Ensure the original repo root (containing `marker`) is on sys.path."""
    for candidate in _candidate_roots(marker):
        if (candidate / marker).exists():
            repo_root = candidate
            if str(repo_root) not in sys.path:
                sys.path.append(str(repo_root))
            return repo_root
    raise RuntimeError(
        f"Unable to locate repository root containing '{marker}'. "
        "Set YMD_REPO_ROOT to the original repo path."
    )


__all__ = ["ensure_repo_root"]
