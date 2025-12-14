# YMD Hybrid Search MCP Server (HTTP/SSE)

MCP (Model Context Protocol) server providing hybrid search capabilities for YMD metrics via HTTP/SSE.

## 🌐 HTTP/SSE Transport

This server uses **Server-Sent Events (SSE)** over HTTP, allowing access via URL instead of stdio.

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or install individually
pip install fastapi uvicorn sse-starlette mcp>=0.9.0
```

## Running the Server

### Start HTTP Server

```bash
# Method 1: Direct run
python3 -m ymda.mcp.server

# Method 2: Using uvicorn
uvicorn ymda.mcp.server:app --host 0.0.0.0 --port 8000

# Method 3: With reload (development)
uvicorn ymda.mcp.server:app --reload --port 8000
```

Server will be available at: **`http://localhost:8000`**

### Endpoints

- **`GET /`** - Server info
- **`GET /health`** - Health check
- **`GET /sse`** - MCP SSE endpoint (for MCP clients)
- **`POST /sse`** - MCP SSE endpoint (alternative)

## Configuration for Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "ymd-hybrid-search": {
      "url": "http://localhost:8000/sse",
      "transport": "sse"
    }
  }
}
```

**Note**: Server must be running before starting Claude Desktop.

## Available Tools

### search_metrics

Hybrid search for YMD metrics using vector similarity + BM25 keyword matching.

**Parameters**:
- `query_text` (string, required): Natural language query
- `top_k` (integer, optional): Number of results (default: 30, max: 100)
- `ymq_id` (integer, optional): YMQ ID for expected_fields

**Example**:
```json
{
  "query_text": "美甲机的回本周期一般是多少？",
  "top_k": 10,
  "ymq_id": 190
}
```

## Testing

### 1. Check Server Status

```bash
curl http://localhost:8000/
```

Response:
```json
{
  "name": "ymd-hybrid-search",
  "version": "1.0.0",
  "transport": "SSE",
  "endpoints": {
    "sse": "/sse",
    "health": "/health"
  }
}
```

### 2. Health Check

```bash
curl http://localhost:8000/health
```

Response:
```json
{
  "status": "healthy"
}
```

### 3. Test SSE Connection

```bash
curl -N http://localhost:8000/sse
```

Should receive SSE events.

## Environment Variables

Required:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `OPENAI_API_KEY`

Set in `.env` file:
```bash
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_SERVICE_ROLE_KEY=xxx
OPENAI_API_KEY=sk-xxx
```

## Architecture

```
HTTP Client (Claude Desktop)
    ↓ SSE
FastAPI Server (port 8000)
    ↓
MCP Server
    ↓
search_metrics tool
    ↓
HybridSearchService
```

## Deployment

### Development

```bash
uvicorn ymda.mcp.server:app --reload --port 8000
```

### Production

```bash
# Using uvicorn
uvicorn ymda.mcp.server:app --host 0.0.0.0 --port 8000 --workers 4

# Or using gunicorn
gunicorn ymda.mcp.server:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

### Docker

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["uvicorn", "ymda.mcp.server:app", "--host", "0.0.0.0", "--port", "8000"]
```

## Troubleshooting

### Server won't start

**Check port availability**:
```bash
lsof -i :8000
```

**Kill existing process**:
```bash
kill -9 $(lsof -t -i:8000)
```

### Claude Desktop can't connect

1. Ensure server is running: `curl http://localhost:8000/health`
2. Check config file path
3. Restart Claude Desktop
4. Check server logs

### CORS issues

If accessing from browser, add CORS middleware:

```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
```

## Performance

- Server startup: ~1s
- First query: ~4-8s (loads corpus stats)
- Subsequent queries: ~2-3s
- Concurrent requests: Supported (async)

## Logging

Logs are output to stdout:
```
2025-12-12 11:00:00 - ymd-mcp-server - INFO - Starting YMD Hybrid Search MCP Server (HTTP/SSE)...
2025-12-12 11:00:05 - ymd-mcp-server - INFO - Executing hybrid search: query='...', top_k=10
```

Adjust log level:
```python
logging.basicConfig(level=logging.DEBUG)
```

## Security

**Production recommendations**:
1. Use HTTPS (not HTTP)
2. Add authentication middleware
3. Rate limiting
4. Input validation
5. Environment variable protection

## License

Internal use only.
