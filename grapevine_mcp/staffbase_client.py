"""Async HTTP client for the Staffbase REST API."""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 30.0


class StaffbaseClient:
    """Lightweight async wrapper around the Staffbase public API.

    Uses a shared ``httpx.AsyncClient`` with connection pooling for efficiency.

    Args:
        base_url: Staffbase instance URL, e.g. ``https://app.staffbase.com``.
        api_key: Base64-encoded Basic-auth token (``id:secret`` encoded).
        timeout: HTTP request timeout in seconds.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

    def _ensure_client(self) -> httpx.AsyncClient:
        """Return the shared client, creating it lazily on first use."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                headers={"Authorization": f"Basic {self._api_key}"},
                timeout=self._timeout,
                limits=httpx.Limits(
                    max_connections=10,
                    max_keepalive_connections=5,
                ),
            )
        return self._client

    async def aclose(self) -> None:
        """Explicitly close the underlying HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "StaffbaseClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Perform an authenticated GET and return parsed JSON."""
        client = self._ensure_client()
        resp = await client.get(path, params=params)
        resp.raise_for_status()
        return resp.json()

    async def _get_raw(self, path: str) -> bytes:
        """Perform an authenticated GET and return raw bytes."""
        client = self._ensure_client()
        resp = await client.get(path)
        resp.raise_for_status()
        return resp.content

    # ------------------------------------------------------------------
    # Spaces
    # ------------------------------------------------------------------

    async def list_spaces(self, include_hidden: bool = True) -> list[dict[str, Any]]:
        """Return all spaces (locations / sub-instances)."""
        params = {"includeHidden": str(include_hidden).lower()}
        data = await self._get("/api/spaces", params=params)
        return data.get("data", data) if isinstance(data, dict) else data

    # ------------------------------------------------------------------
    # Posts / News
    # ------------------------------------------------------------------

    async def get_global_posts(
        self, limit: int = 20, offset: int = 0
    ) -> list[dict[str, Any]]:
        """Fetch posts from the global branch."""
        params = {"limit": limit, "offset": offset}
        data = await self._get("/api/posts", params=params)
        return data.get("data", data) if isinstance(data, dict) else data

    async def get_channel_posts(
        self, channel_id: str, limit: int = 20, offset: int = 0
    ) -> dict[str, Any]:
        """Fetch posts from a specific channel (by installation ID)."""
        params = {"limit": limit, "offset": offset}
        return await self._get(f"/api/channels/{channel_id}/posts", params=params)

    # ------------------------------------------------------------------
    # Pages
    # ------------------------------------------------------------------

    async def get_page(self, page_id: str) -> dict[str, Any]:
        """Fetch a single page by ID."""
        return await self._get(f"/api/pages/{page_id}")

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, limit: int = 10) -> dict[str, Any]:
        """Full-text search across Staffbase content."""
        params = {"q": query, "limit": limit}
        return await self._get("/api/search", params=params)

    # ------------------------------------------------------------------
    # Media
    # ------------------------------------------------------------------

    async def download_media(self, media_path: str) -> bytes:
        """Download a media file (PDF, image, etc.) by its path or full URL.

        Accepts:
        - Full URL: ``https://<your-instance>.staffbase.com/api/media/secure/...``
        - Absolute path: ``/api/media/secure/...``
        - Relative path: ``media/secure/external/v2/raw/upload/<id>.pdf``
        """
        # Strip base URL prefix if present (full URL passed)
        if media_path.startswith(("http://", "https://")):
            # Extract path portion after the domain
            from urllib.parse import urlparse
            parsed = urlparse(media_path)
            media_path = parsed.path

        path = media_path if media_path.startswith("/api/") else f"/api/{media_path}"
        return await self._get_raw(path)

    # ------------------------------------------------------------------
    # Channels / Installations
    # ------------------------------------------------------------------

    async def get_space_news(self, space_id: str) -> list[dict[str, Any]]:
        """Fetch the news-menu structure for a space (channels, folders)."""
        data = await self._get(f"/api/spaces/{space_id}/news")
        return data.get("data", data) if isinstance(data, dict) else data

    # ------------------------------------------------------------------
    # Menu / Navigation
    # ------------------------------------------------------------------

    async def get_menu(self, menu_id: str) -> dict[str, Any]:
        """Fetch a menu node (section/navigation node) by ID.

        Returns the node including its children (sub-pages).
        """
        return await self._get(f"/api/menu/{menu_id}", params={"platform": "web"})

    async def get_menu_page_ids(self, menu_id: str) -> list[str]:
        """Extract page IDs from a menu node and its children.

        If the node itself is a page installation (pluginID == "page"),
        returns its installationID. If it has children, returns their IDs.
        """
        data = await self.get_menu(menu_id)
        if not data:
            return []

        page_ids: list[str] = []

        # If this node itself is a page, use its installationID
        if data.get("pluginID") == "page" and data.get("installationID"):
            page_ids.append(data["installationID"])

        # Collect children
        children_data = data.get("children", {})
        if isinstance(children_data, dict):
            for child in children_data.get("data", []):
                inst_id = child.get("installationID", "")
                node_type = child.get("nodeType", "")
                cid = child.get("id", "")
                if node_type == "installation" and inst_id:
                    page_ids.append(inst_id)
                elif node_type != "folder" and cid:
                    page_ids.append(cid)
        # Fallback: childrenIds list
        for cid in data.get("childrenIds", []):
            if cid not in page_ids:
                page_ids.append(cid)
        return page_ids
