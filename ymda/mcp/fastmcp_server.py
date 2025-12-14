"""YMD MCP Server using FastMCP

使用 FastMCP 简化 MCP 服务器实现
"""

from fastmcp import FastMCP
from ymda.settings import Settings
from ymda.services.ymd_search_service import YMDSearchService
from ymda.mcp.schemas import SearchRequest

# 创建 FastMCP 服务器
mcp = FastMCP("YMD Hybrid Search")

# 初始化服务
settings = Settings()
search_service = YMDSearchService(settings)


@mcp.tool()
def ymd_search(
    query: str,
    top_k: int = 20,
    mode: str = "auto",
    explain: bool = False
) -> dict:
    """
    YMD Hybrid Search - 混合检索 YMD metrics
    
    Args:
        query: 用户查询文本
        top_k: 返回结果数量 (默认: 20)
        mode: 查询模式 (auto | semantic_only | structured_only | hybrid)
        explain: 是否返回详细explain信息
        
    Returns:
        包含 trace_id, results, stats 的字典
    """
    # 构建请求
    request = SearchRequest(
        query=query,
        top_k=top_k,
        mode=mode,
        explain=explain
    )
    
    # 执行查询
    response = search_service.search(request)
    
    return response.to_dict()



# 运行服务器
if __name__ == "__main__":
    # FastMCP SSE 传输 - 自动处理 MCP 协议
    mcp.run(
        transport="sse",
        host="127.0.0.1",
        port=8000
    )

