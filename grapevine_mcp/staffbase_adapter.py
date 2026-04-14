"""High-level adapter for Staffbase content — locale resolution, thumbnails, article formatting."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from grapevine_mcp.staffbase_client import StaffbaseClient

logger = logging.getLogger(__name__)

_DEFAULT_LOCALES = ("de_DE", "en_US")
_DEFAULT_TEASER_MAX = 200


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ArticleImage:
    url: str
    variant: str  # "compact", "original_scaled", or "original"


@dataclass
class Article:
    id: str
    title: str
    teaser: str
    image: str  # thumbnail URL (empty string if none)
    published_at: str  # ISO 8601 from Staffbase
    published_label: str  # formatted date string, e.g. "28.02.2026"
    author_name: str
    web_link: str
    locale: str
    channel_name: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a dict matching the hub JS widget keys."""
        return {
            "id": self.id,
            "title": self.title,
            "teaser": self.teaser,
            "image": self.image,
            "published": self.published_label,
            "published_at": self.published_at,
            "author": self.author_name,
            "web_link": self.web_link,
            "locale": self.locale,
            "channel_name": self.channel_name,
        }


@dataclass
class NewsChannel:
    name: str
    installation_id: str


# ---------------------------------------------------------------------------
# StaffbaseAdapter
# ---------------------------------------------------------------------------


class StaffbaseAdapter:
    """Adapter that turns raw Staffbase API responses into clean domain objects.

    Args:
        client: A :class:`StaffbaseClient` instance.
        base_url: Staffbase instance URL (used for building fallback links).
        preferred_locales: Default locale preference chain, e.g. ``("de_DE", "en_US")``.
        teaser_max_length: Maximum teaser character length before truncation.
    """

    def __init__(
        self,
        client: StaffbaseClient,
        base_url: str,
        preferred_locales: tuple[str, ...] = _DEFAULT_LOCALES,
        teaser_max_length: int = _DEFAULT_TEASER_MAX,
    ) -> None:
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.preferred_locales = preferred_locales
        self.teaser_max_length = teaser_max_length

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    async def get_global_articles(
        self,
        limit: int = 20,
        preferred_locales: tuple[str, ...] | None = None,
    ) -> list[Article]:
        """Fetch articles from the global news feed."""
        posts = await self.client.get_global_posts(limit=limit)
        locales = preferred_locales or self.preferred_locales
        return self._parse_posts(posts[:limit], locales)

    async def get_channel_articles(
        self,
        channel_id: str,
        limit: int = 20,
        channel_name: str = "",
        preferred_locales: tuple[str, ...] | None = None,
    ) -> list[Article]:
        """Fetch articles from a specific news channel."""
        data = await self.client.get_channel_posts(channel_id, limit=limit)
        posts = data.get("data", data) if isinstance(data, dict) else data
        locales = preferred_locales or self.preferred_locales
        return self._parse_posts(
            posts[:limit] if isinstance(posts, list) else [],
            locales,
            channel_name=channel_name,
        )

    async def get_merged_articles(
        self,
        channel_ids: list[str],
        global_limit: int = 10,
        channel_limit: int = 5,
        total_limit: int = 20,
        preferred_locales: tuple[str, ...] | None = None,
    ) -> list[Article]:
        """Fetch from global + multiple channels, merge and sort by date."""
        articles = await self.get_global_articles(
            limit=global_limit, preferred_locales=preferred_locales
        )
        for cid in channel_ids:
            channel_articles = await self.get_channel_articles(
                cid, limit=channel_limit, preferred_locales=preferred_locales
            )
            articles.extend(channel_articles)

        # Deduplicate by id
        seen: set[str] = set()
        unique: list[Article] = []
        for a in articles:
            if a.id not in seen:
                seen.add(a.id)
                unique.append(a)

        # Sort newest first
        unique.sort(key=lambda a: a.published_at, reverse=True)
        return unique[:total_limit]

    async def discover_channels(self, space_id: str) -> list[NewsChannel]:
        """List news channels in a space."""
        news = await self.client.get_space_news(space_id)
        return self._extract_channels(news)

    async def discover_channels_for_email(
        self,
        email: str,
        domain_space_map: dict[str, str],
    ) -> list[NewsChannel]:
        """Discover channels based on user email domain."""
        domain = email.rsplit("@", 1)[-1].lower() if "@" in email else ""
        space_id = domain_space_map.get(domain)
        if not space_id:
            return []
        return await self.discover_channels(space_id)

    def resolve_locale(
        self,
        contents: dict[str, Any] | list[Any],
        preferred_locales: tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        """Pick the best locale from a Staffbase ``contents`` structure.

        Handles both dict-keyed (``{"de_DE": {...}}``) and list-of-dicts
        (``[{"locale": "de_DE", ...}]``) formats.
        """
        locales = preferred_locales or self.preferred_locales

        if isinstance(contents, dict):
            for loc in locales:
                if loc in contents:
                    return contents[loc]
            # Fallback: first available
            return next(iter(contents.values()), {})

        if isinstance(contents, list):
            for loc in locales:
                for item in contents:
                    if item.get("locale") == loc:
                        return item
            return contents[0] if contents else {}

        return {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_posts(
        self,
        posts: list[dict[str, Any]],
        preferred_locales: tuple[str, ...],
        channel_name: str = "",
    ) -> list[Article]:
        result: list[Article] = []
        for post in posts:
            article = self._parse_post(post, preferred_locales, channel_name)
            if article:
                result.append(article)
        return result

    def _parse_post(
        self,
        post: dict[str, Any],
        preferred_locales: tuple[str, ...],
        channel_name: str = "",
    ) -> Article | None:
        contents = post.get("contents", {})
        content = self.resolve_locale(contents, preferred_locales)

        # Determine which locale was selected
        locale = self._detect_locale(contents, content)

        title = content.get("title", "")
        if not title:
            return None

        teaser = (content.get("teaser", "") or "")[:self.teaser_max_length]

        thumb = self._extract_thumbnail(content)
        image_url = thumb.url if thumb else ""

        # Published date
        published_at = post.get("published", post.get("publishedAt", ""))
        published_label = self._format_date(published_at)

        # Author
        author = post.get("author", {}) or {}
        author_name = ""
        if author.get("firstName"):
            author_name = f"{author['firstName']} {author.get('lastName', '')}".strip()

        # Web link
        web_link = (
            (post.get("links", {}) or {})
            .get("detail_view", {})
            .get("href", "")
        )
        if not web_link and post.get("id"):
            web_link = f"{self.base_url}/openlink/content/news/article/{post['id']}"

        return Article(
            id=post.get("id", ""),
            title=title,
            teaser=teaser,
            image=image_url,
            published_at=published_at,
            published_label=published_label,
            author_name=author_name,
            web_link=web_link,
            locale=locale,
            channel_name=channel_name,
        )

    @staticmethod
    def _extract_thumbnail(content: dict[str, Any]) -> ArticleImage | None:
        image = content.get("image", {}) or {}
        for variant in ("compact", "original_scaled", "original"):
            img = image.get(variant)
            if img and img.get("url"):
                return ArticleImage(url=img["url"], variant=variant)
        return None

    @staticmethod
    def _format_date(iso_str: str) -> str:
        if not iso_str:
            return ""
        try:
            dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
            return dt.strftime("%d.%m.%Y")
        except (ValueError, TypeError):
            return iso_str[:10]

    @staticmethod
    def _detect_locale(
        contents: dict[str, Any] | list[Any],
        selected: dict[str, Any],
    ) -> str:
        if isinstance(contents, dict):
            for k, v in contents.items():
                if v is selected:
                    return k
            return next(iter(contents), "")
        if isinstance(contents, list):
            return selected.get("locale", "")
        return ""

    def _extract_channels(
        self,
        items: list[dict[str, Any]],
        result: list[NewsChannel] | None = None,
    ) -> list[NewsChannel]:
        if result is None:
            result = []
        for item in items:
            if item.get("type") == "news":
                contents = item.get("contents", {})
                locale = next(iter(contents), None)
                title = contents.get(locale, {}).get("title", "") if locale else ""
                result.append(
                    NewsChannel(
                        name=title or item.get("title", ""),
                        installation_id=item.get(
                            "installationID", item.get("id", "")
                        ),
                    )
                )
            children = item.get("children", [])
            if children:
                self._extract_channels(children, result)
        return result


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_adapter(
    base_url: str | None = None,
    api_key: str | None = None,
    preferred_locales: tuple[str, ...] = _DEFAULT_LOCALES,
    teaser_max_length: int = _DEFAULT_TEASER_MAX,
    timeout: float = 30.0,
) -> tuple[StaffbaseClient, StaffbaseAdapter]:
    """Create a ``(client, adapter)`` pair, reading from env vars if args not given."""
    url = base_url or os.environ.get("STAFFBASE_URL", "")
    key = api_key or os.environ.get("STAFFBASE_API_KEY", "")
    if not url or not key:
        raise RuntimeError(
            "STAFFBASE_URL and STAFFBASE_API_KEY must be provided or set as env vars."
        )
    client = StaffbaseClient(base_url=url, api_key=key, timeout=timeout)
    adapter = StaffbaseAdapter(
        client=client,
        base_url=url,
        preferred_locales=preferred_locales,
        teaser_max_length=teaser_max_length,
    )
    return client, adapter
