"""Standalone MCP server entrypoint for deployment."""

from __future__ import annotations

import os
from pathlib import Path

from .bootstrap import ensure_repo_root

# Ensure we can import the original YMD modules
ensure_repo_root()

from ymda.mcp.server import app  # noqa: E402

__all__ = ["app"]


def run_local() -> None:
    """Launch the MCP server with uvicorn for local dev."""
    import uvicorn

    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "3000"))
    uvicorn.run(app, host=host, port=port, log_level=os.getenv("LOG_LEVEL", "info"))


if __name__ == "__main__":
    run_local()
