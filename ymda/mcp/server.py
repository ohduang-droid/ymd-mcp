"""YMD MCP Server with HTTP/SSE Support

提供 MCP (Model Context Protocol) 服务，支持:
- SSE (Server-Sent Events) 传输 (标准 MCP 协议)
- HTTP JSON-RPC (自定义扩展)
"""

import json
import logging
import asyncio
import uuid
from typing import Any, Dict, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from ymda.settings import Settings
from ymda.services.hybrid_search import HybridSearchService
from ymda.data.repository import get_repository
from ymda.mcp.tools.ymd_search import ymd_search, TOOL_METADATA

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ymd-mcp-server")

# 初始化服务
settings = Settings()
search_service = HybridSearchService(settings)
repository = get_repository(settings)

# SSE 连接管理
sse_connections: Dict[str, asyncio.Queue] = {}

MCP_PROTOCOL_VERSION = "2024-11-05"
MCP_SERVER_INFO = {
    "name": "ymd-mcp-server",
    "version": "1.0.0"
}
MCP_CAPABILITIES = {
    "tools": {
        "listChanged": False
    }
}
MCP_TOOL_DEFINITION = {
    "name": TOOL_METADATA["name"],
    "description": TOOL_METADATA["description"],
    "inputSchema": TOOL_METADATA["parameters"]
}



# 请求模型
class SearchRequest(BaseModel):
    query_text: str = Field(..., description="User's natural language query")
    top_k: int = Field(30, ge=1, le=100, description="Number of results to return")
    ymq_id: Optional[int] = Field(None, description="Optional YMQ ID for expected_fields")


# 创建 FastAPI 应用
@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    logger.info("Starting YMD Hybrid Search API Server...")
    yield
    logger.info("Shutting down YMD Hybrid Search API Server...")


app = FastAPI(
    title="YMD Hybrid Search API",
    description="HTTP API providing hybrid search for YMD metrics",
    version="1.0.0",
    lifespan=lifespan
)

# 添加 CORS 支持
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.get("/")
async def root():
    """根路径 - 服务信息"""
    return {
        "name": "ymd-mcp-server",
        "version": "1.0.0",
        "description": "YMD MCP Server with HTTP + SSE support",
        "endpoints": {
            "mcp": "/mcp (POST - JSON-RPC)",
            "search": "/api/search (deprecated)",
            "health": "/health",
            "docs": "/docs"
        }
    }




@app.get("/health")
async def health():
    """健康检查"""
    try:
        # 简单检查数据库连接
        repository.client.table('metric').select('id').limit(1).execute()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "error": str(e)}


@app.get("/sse")
async def sse_endpoint(request: Request):
    """
    MCP SSE 端点 (GET) - 建立 SSE 连接
    
    MCP clients 通过此端点建立长连接，接收服务器消息
    """
    session_id = uuid.uuid4().hex
    queue: asyncio.Queue = asyncio.Queue()
    sse_connections[session_id] = queue
    logger.info(f"SSE connection established: {session_id}")
    endpoint_path = f"/sse/messages?session_id={session_id}"
    
    async def event_generator():
        """生成 SSE 事件流"""
        try:
            # 通知客户端 POST 端点
            yield {
                "event": "endpoint",
                "data": endpoint_path
            }
            
            # 持续监听消息队列
            while True:
                if await request.is_disconnected():
                    logger.info(f"SSE connection closed: {session_id}")
                    break
                
                try:
                    # 等待消息（带超时）
                    message = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {
                        "event": "message",
                        "data": json.dumps(message, ensure_ascii=False)
                    }
                except asyncio.TimeoutError:
                    # 发送心跳
                    yield {
                        "event": "ping",
                        "data": ""
                    }
                    
        except Exception as e:
            logger.error(f"SSE error: {e}")
        finally:
            # 清理连接
            if session_id in sse_connections:
                del sse_connections[session_id]
            logger.info(f"SSE connection cleaned up: {session_id}")
    
    return EventSourceResponse(event_generator())


@app.post("/sse/messages")
async def sse_post_endpoint(request: Request, session_id: str = Query(..., description="SSE session ID")):
    """
    MCP SSE 端点 (POST) - 接收客户端消息
    
    MCP clients 通过此端点发送 JSON-RPC 请求
    """
    try:
        # 获取 SSE 队列
        queue = sse_connections.get(session_id)
        if not queue:
            raise HTTPException(status_code=404, detail="Invalid session_id")
        
        body = await request.json()
        method = body.get("method")
        params = body.get("params", {})
        request_id = body.get("id")
        if not method:
            raise HTTPException(status_code=400, detail="Missing 'method'")

        logger.info(f"SSE POST received: method={method}, id={request_id}")
        
        response_payload: Optional[Dict[str, Any]] = None

        # 处理 MCP 方法
        if method == "initialize":
            response_payload = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": MCP_CAPABILITIES,
                    "serverInfo": MCP_SERVER_INFO,
                }
            }
        elif method == "tools/list":
            response_payload = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": [MCP_TOOL_DEFINITION],
                    "partial": False
                }
            }
        elif method == "prompts/list":
            response_payload = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "prompts": [],
                    "nextCursor": None
                }
            }
        elif method == "resources/list":
            response_payload = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "resources": [],
                    "nextCursor": None
                }
            }
        elif method == "tools/call":
            tool_name = params.get("name")
            tool_args = params.get("arguments", {})
            
            if tool_name != TOOL_METADATA["name"]:
                raise HTTPException(status_code=404, detail=f"Unknown tool: {tool_name}")
            
            result = ymd_search(**tool_args)
            response_payload = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(result, ensure_ascii=False, indent=2)
                        }
                    ],
                    "isError": False
                }
            }
        elif method == "ping":
            response_payload = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {}
            }
        elif method == "notifications/initialized":
            logger.info(f"SSE session initialized: {session_id}")
        elif method == "notifications/session/close":
            logger.info(f"SSE session closed by client: {session_id}")
            sse_connections.pop(session_id, None)
        else:
            response_payload = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method}"
                }
            }
        
        # 仅当请求包含 id 时返回 JSON-RPC 响应
        if request_id is not None and response_payload:
            await queue.put(response_payload)
        
        return {"status": "accepted"}
        
    except Exception as e:
        logger.error(f"SSE POST error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/mcp")
async def mcp_endpoint(request: Dict[str, Any]):
    """
    MCP JSON-RPC 端点
    
    接受 JSON-RPC 格式的请求:
    {
        "jsonrpc": "2.0",  // optional
        "method": "ymd.search",
        "params": {...},
        "id": 1  // optional
    }
    
    或简化格式:
    {
        "method": "ymd.search",
        "params": {...}
    }
    """
    try:
        # 解析请求
        method = request.get("method")
        params = request.get("params", {})
        request_id = request.get("id")
        
        if not method:
            raise HTTPException(status_code=400, detail="Missing 'method' field")
        
        # 路由到对应的 tool
        if method == "ymd.search":
            result = ymd_search(**params)
        else:
            raise HTTPException(status_code=404, detail=f"Unknown method: {method}")
        
        # 返回 JSON-RPC 格式响应
        response = {
            "jsonrpc": "2.0",
            "result": result
        }
        if request_id is not None:
            response["id"] = request_id
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"MCP endpoint error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/search")
async def search_metrics(request: SearchRequest) -> Dict[str, Any]:
    """
    混合检索 API (Deprecated - 保留兼容性)
    
    执行 Vector + BM25 混合检索，支持中英文查询
    
    推荐使用 POST /mcp 端点
    """
    try:
        # 获取 expected_fields（如果提供了 ymq_id）
        expected_fields = None
        if request.ymq_id:
            try:
                ymq_data = repository.client.table('ymq').select('expected_fields').eq('id', request.ymq_id).execute()
                if ymq_data.data and len(ymq_data.data) > 0:
                    expected_fields_json = ymq_data.data[0].get('expected_fields')
                    if expected_fields_json and isinstance(expected_fields_json, dict):
                        expected_fields = expected_fields_json.get('fields', [])
                        logger.info(f"Loaded {len(expected_fields)} expected_fields from YMQ {request.ymq_id}")
            except Exception as e:
                logger.warning(f"Failed to load expected_fields for YMQ {request.ymq_id}: {e}")
        
        # 执行混合检索
        logger.info(f"Executing hybrid search: query='{request.query_text}', top_k={request.top_k}")
        result = search_service.search(
            query_text=request.query_text,
            top_k=request.top_k,
            expected_fields=expected_fields
        )
        
        # 返回结果
        return result.to_dict()
        
    except Exception as e:
        logger.error(f"search_metrics failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# 用于直接运行
if __name__ == "__main__":
    import uvicorn
    
    # 运行 FastAPI 服务器
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
