"""Async HTTP client for the Staffbase REST API."""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 30.0


class StaffbaseClient:
    """Lightweight async wrapper around the Staffbase public API.

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
        self._headers = {"Authorization": f"Basic {api_key}"}
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Perform an authenticated GET and return parsed JSON."""
        url = f"{self._base_url}{path}"
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.get(url, headers=self._headers, params=params)
            resp.raise_for_status()
            return resp.json()

    async def _get_raw(self, path: str) -> bytes:
        """Perform an authenticated GET and return raw bytes."""
        url = f"{self._base_url}{path}"
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.get(url, headers=self._headers)
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
        """Download a media file (PDF, image, etc.) by its path.

        ``media_path`` is the portion after ``/api/``, e.g.
        ``media/secure/external/v2/raw/upload/<id>.pdf``.
        """
        path = media_path if media_path.startswith("/api/") else f"/api/{media_path}"
        return await self._get_raw(path)

    # ------------------------------------------------------------------
    # Channels / Installations
    # ------------------------------------------------------------------

    async def get_space_news(self, space_id: str) -> list[dict[str, Any]]:
        """Fetch the news-menu structure for a space (channels, folders)."""
        data = await self._get(f"/api/spaces/{space_id}/news")
        return data.get("data", data) if isinstance(data, dict) else data
