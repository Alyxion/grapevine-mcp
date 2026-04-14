"""Grapevine MCP — unofficial MCP server for accessing data from Staffbase."""

__version__ = "0.1.0"

from grapevine_mcp.staffbase_adapter import (
    Article,
    ArticleImage,
    NewsChannel,
    StaffbaseAdapter,
    create_adapter,
)
from grapevine_mcp.staffbase_client import StaffbaseClient
from grapevine_mcp.inprocess_server import GrapevineMCPServer, GrapevineConfig, build_preview_router

__all__ = [
    "Article",
    "ArticleImage",
    "NewsChannel",
    "StaffbaseAdapter",
    "StaffbaseClient",
    "GrapevineMCPServer",
    "GrapevineConfig",
    "build_preview_router",
    "create_adapter",
]
