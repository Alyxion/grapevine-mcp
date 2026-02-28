"""Grapevine MCP server — exposes Staffbase data via the Model Context Protocol."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from grapevine_mcp.staffbase_client import StaffbaseClient

logger = logging.getLogger(__name__)

INSTRUCTIONS = """\
Grapevine MCP — unofficial read-only access to Staffbase intranet data.

Available tools let you browse spaces, read news posts (global and local
channels), view pages, search content, and list available news channels.
All operations are read-only.
"""

# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

TOOLS: list[dict[str, Any]] = [
    {
        "name": "list_spaces",
        "description": "List all Staffbase spaces (locations / sub-instances).",
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_news",
        "description": (
            "Fetch recent news posts. Without a channel_id, returns global posts. "
            "With a channel_id (installation ID), returns posts from that local channel."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "channel_id": {
                    "type": "string",
                    "description": "Optional channel installation ID for local news.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max posts to return (default 10).",
                    "default": 10,
                },
            },
        },
    },
    {
        "name": "list_channels",
        "description": (
            "List news channels available in a space. Returns channel names and "
            "installation IDs that can be used with get_news."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "space_id": {
                    "type": "string",
                    "description": "The space ID to list channels for.",
                },
            },
            "required": ["space_id"],
        },
    },
    {
        "name": "get_page",
        "description": "Fetch a Staffbase page by its ID. Returns title and HTML content.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "page_id": {
                    "type": "string",
                    "description": "The page ID.",
                },
            },
            "required": ["page_id"],
        },
    },
    {
        "name": "search",
        "description": "Full-text search across all Staffbase content.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default 10).",
                    "default": 10,
                },
            },
            "required": ["query"],
        },
    },
]

# ---------------------------------------------------------------------------
# Client singleton
# ---------------------------------------------------------------------------


def _get_client() -> StaffbaseClient:
    base_url = os.environ.get("STAFFBASE_URL", "")
    api_key = os.environ.get("STAFFBASE_API_KEY", "")
    if not base_url or not api_key:
        raise RuntimeError(
            "STAFFBASE_URL and STAFFBASE_API_KEY environment variables are required."
        )
    return StaffbaseClient(base_url=base_url, api_key=api_key)


# ---------------------------------------------------------------------------
# Tool dispatch
# ---------------------------------------------------------------------------


async def _handle_tool(name: str, arguments: dict[str, Any]) -> str:
    client = _get_client()

    if name == "list_spaces":
        spaces = await client.list_spaces()
        result = [{"id": s["id"], "name": s.get("name", "")} for s in spaces]
        return json.dumps(result, indent=2)

    elif name == "get_news":
        channel_id = arguments.get("channel_id")
        limit = arguments.get("limit", 10)
        if channel_id:
            data = await client.get_channel_posts(channel_id, limit=limit)
            posts = data.get("data", data) if isinstance(data, dict) else data
        else:
            posts = await client.get_global_posts(limit=limit)
        result = []
        for p in posts[:limit] if isinstance(posts, list) else []:
            contents = p.get("contents", {})
            # Pick first available locale
            locale = next(iter(contents), None)
            localized = contents.get(locale, {}) if locale else {}
            result.append({
                "id": p.get("id", ""),
                "title": localized.get("title", ""),
                "teaser": localized.get("teaser", "")[:200],
                "published": p.get("publishedAt", ""),
                "locale": locale or "",
            })
        return json.dumps(result, indent=2)

    elif name == "list_channels":
        space_id = arguments["space_id"]
        news = await client.get_space_news(space_id)
        channels = _extract_channels(news)
        return json.dumps(channels, indent=2)

    elif name == "get_page":
        page_id = arguments["page_id"]
        page = await client.get_page(page_id)
        contents = page.get("contents", {})
        locale = next(iter(contents), None)
        localized = contents.get(locale, {}) if locale else {}
        return json.dumps({
            "id": page.get("id", ""),
            "title": localized.get("title", ""),
            "content": localized.get("content", "")[:5000],
            "locale": locale or "",
            "updated": page.get("updatedAt", ""),
        }, indent=2)

    elif name == "search":
        query = arguments["query"]
        limit = arguments.get("limit", 10)
        data = await client.search(query, limit=limit)
        return json.dumps(data, indent=2, default=str)

    else:
        return json.dumps({"error": f"Unknown tool: {name}"})


def _extract_channels(
    items: list[dict[str, Any]], result: list[dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    """Recursively extract news channels from the space news tree."""
    if result is None:
        result = []
    for item in items:
        if item.get("type") == "news":
            contents = item.get("contents", {})
            locale = next(iter(contents), None)
            title = contents.get(locale, {}).get("title", "") if locale else ""
            result.append({
                "name": title or item.get("title", ""),
                "installation_id": item.get("installationID", item.get("id", "")),
            })
        # Recurse into children (folders)
        children = item.get("children", [])
        if children:
            _extract_channels(children, result)
    return result


# ---------------------------------------------------------------------------
# MCP server setup
# ---------------------------------------------------------------------------

server = Server("grapevine-mcp", instructions=INSTRUCTIONS)


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name=t["name"],
            description=t["description"],
            inputSchema=t["inputSchema"],
        )
        for t in TOOLS
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    try:
        result = await _handle_tool(name, arguments)
    except Exception as exc:
        logger.exception(f"Tool {name} failed")
        result = json.dumps({"error": str(exc)})
    return [TextContent(type="text", text=result)]


async def main() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )
