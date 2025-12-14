# YMD MCP Standalone Service

This directory packages the existing MCP server into a deployable microservice. It now **bundles a
copy of the `ymda` package** (search pipeline, Supabase repository, etc.), so the service can be
deployed independently without referencing files outside `mcp_service/`.

## Local usage

```bash
cd mcp_service
python3 -m app.main  # equivalent to python3 start_mcp_server.py
```

The helper ensures the local `ymda` package is on `PYTHONPATH`, so imports like
`from ymda.mcp.server import app` resolve inside this directory alone.

Environment variables (`SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `OPENAI_API_KEY`, ...)
should be defined exactly as in the main project.

## Deploying on Vercel

1. Install the Vercel CLI and log in.
2. From the repo root, run:
   ```bash
   cd mcp_service
   vercel deploy --prod
   ```
3. Vercel reads `vercel.json`, installs `requirements.txt`, and serves the FastAPI app defined in
   `api/index.py`.

The vendored `ymda` package lives under `mcp_service/ymda`, so deploying just this folder to Vercel
is sufficient—as long as you provide the required environment variables.

## Structure

```
mcp_service/
├── api/index.py        # Vercel entrypoint
├── app/
│   ├── __init__.py
│   ├── bootstrap.py    # ensures repo root on sys.path
│   └── main.py         # shared FastAPI app + local runner
├── requirements.txt    # dependencies (copied from the main project)
├── ymda/               # vendored copy of the YMD search stack
└── vercel.json         # platform routing/runtime config
```

No changes were made to the original MCP implementation; this folder only adds packaging glue.
# ymd-mcp
