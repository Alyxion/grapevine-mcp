"""
GrapevineMCPServer — InProcessMCPServer for live Staffbase intranet search.

Provides real-time access to Staffbase content (pages, news, search) without
requiring a pre-crawled data package. Subsidiary-aware, with enforced pages
(e.g. canteen menus) that are always available.

At init, fetches the full menu/navigation structure and builds a local page
index so that searches match page titles even though the Staffbase search API
only indexes news posts.

Usage (host app):
    from grapevine_mcp.inprocess_server import GrapevineMCPServer

    server = GrapevineMCPServer(
        space_id="679b7a64f7c3a352daada2f4",
        channels={"HR News": "65ccb8a0a6e20a56400e4408"},
        enforced_pages={"Speisepläne": "67c84a55bf194540c908cf6b"},
    )
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
from io import BytesIO
from dataclasses import dataclass, field
from typing import Any

from grapevine_mcp.staffbase_adapter import StaffbaseAdapter, create_adapter
from grapevine_mcp.staffbase_client import StaffbaseClient

try:
    from starlette.requests import Request as _Request
except ImportError:  # starlette not installed (e.g. test env)
    _Request = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

try:
    from llming_models.tools.mcp.connection import InProcessMCPServer
except ImportError:
    from abc import ABC, abstractmethod

    class InProcessMCPServer(ABC):  # type: ignore[no-redef]
        @abstractmethod
        async def list_tools(self) -> list[dict]: ...
        @abstractmethod
        async def call_tool(self, name: str, arguments: dict) -> str: ...


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class GrapevineConfig:
    """Configuration for the GrapevineMCPServer."""
    base_url: str = ""
    api_key: str = ""
    preferred_locales: tuple[str, ...] = ("de_DE", "en_US")
    space_id: str = ""
    channels: dict[str, str] = field(default_factory=dict)
    enforced_pages: dict[str, str] = field(default_factory=dict)
    enforced_page_follow_pdfs: bool = True
    subsidiary_channels: dict[str, dict[str, str]] = field(default_factory=dict)
    display_name: str = "Grapevine for Staffbase"  # shown in tool names, prompts, UI


# ---------------------------------------------------------------------------
# Menu index entry
# ---------------------------------------------------------------------------

@dataclass
class _MenuEntry:
    """A page/section discovered from the Staffbase menu tree."""
    title: str
    menu_id: str
    installation_id: str
    node_type: str  # "installation" or "folder"
    path: list[str]  # breadcrumb path, e.g. ["Organisation", "Kantine"]
    plugin_id: str = ""  # "page", "news", etc.


# ---------------------------------------------------------------------------
# HTML / PDF helpers
# ---------------------------------------------------------------------------

_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")
_MEDIA_URL_RE = re.compile(
    r'(?:href|src)=["\']([^"\']*?/media/[^"\']+\.pdf)["\']',
    re.IGNORECASE,
)
def _md_link(title: str, url: str) -> str:
    """Build a markdown link. Escapes brackets in title."""
    title_esc = title.replace("[", "\\[").replace("]", "\\]")
    return f"[{title_esc}]({url})"


def _query_fragments(query: str) -> list[str]:
    """Extract search fragments from a query for German compound-word matching.

    For each query word ≥ 6 chars, extracts suffixes of length ≥ 4.
    E.g. "betriebsratswahl" → ["wahl", "swahl", "tswahl", "atswahl", "ratswahl", ...]
    This allows "Betriebsratswahl" to match "Wahlvorschläge" via shared "wahl" fragment.
    """
    fragments: list[str] = []
    for word in query.lower().split():
        word = re.sub(r"[^a-zäöüß]", "", word)
        if len(word) < 6:
            continue
        # Suffixes from position 2 onwards, min length 4
        for i in range(2, len(word) - 3):
            fragments.append(word[i:])
    return fragments


def _html_to_text(raw_html: str) -> str:
    if not raw_html:
        return ""
    text = _TAG_RE.sub(" ", raw_html)
    text = html.unescape(text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text


def _extract_pdf_urls(content_html: str) -> list[str]:
    return _MEDIA_URL_RE.findall(content_html or "")


async def _extract_pdf_text(pdf_bytes: bytes) -> str:
    try:
        import pdfplumber
    except ImportError:
        logger.warning("pdfplumber not available — cannot extract PDF text")
        return ""

    def _extract(data: bytes) -> str:
        pages_text = []
        with pdfplumber.open(BytesIO(data)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                if text.strip():
                    pages_text.append(text)
        return "\n\n".join(pages_text)

    return await asyncio.to_thread(_extract, pdf_bytes)


# ---------------------------------------------------------------------------
# MCP media proxy helpers
# ---------------------------------------------------------------------------

import base64 as _b64

_IMG_SRC_RE = re.compile(r'<img[^>]+src="([^"]+)"[^>]*>', re.IGNORECASE)
_VIDEO_BLOCK_RE = re.compile(
    r'<div[^>]*data-widget-type="VideoBlock"[^>]*>',
    re.IGNORECASE,
)
_CONF_URL_RE = re.compile(r'data-widget-conf-url="([^"]+)"')
_CONF_THUMB_RE = re.compile(r'data-widget-conf-thumbnail-url="([^"]+)"')


def _media_proxy_url(original_url: str, media_type: str = "i") -> str:
    """Convert a Staffbase media URL to an MCP media proxy URL.

    URL scheme: ``/api/mcp-media/grapevine/{i|v}/{base64url}``
    """
    encoded = _b64.urlsafe_b64encode(original_url.encode()).decode().rstrip("=")
    return f"/api/mcp-media/grapevine/{media_type}/{encoded}"


def _html_to_text_with_media(raw_html: str) -> str:
    """Convert Staffbase HTML to plain text with inline media markers.

    Images become ``![Bild](proxy_url)`` and videos become fenced
    ``sb-video`` code blocks at the positions where they appear in the
    original HTML.  All other tags are stripped as usual.
    """
    if not raw_html:
        return ""

    result = raw_html

    # 1. Replace VideoBlock divs with sb-video code blocks (before stripping tags)
    def _video_repl(m: re.Match) -> str:
        tag = m.group(0)
        url_m = _CONF_URL_RE.search(tag)
        if not url_m:
            return ""
        video_url = url_m.group(1)
        thumb_m = _CONF_THUMB_RE.search(tag)
        thumb_url = thumb_m.group(1) if thumb_m else ""
        obj: dict[str, str] = {"url": _media_proxy_url(video_url, "v")}
        if thumb_url:
            obj["poster"] = _media_proxy_url(thumb_url, "i")
        import json as _json
        return "\n\n```sb-video\n" + _json.dumps(obj) + "\n```\n\n"

    result = _VIDEO_BLOCK_RE.sub(_video_repl, result)

    # 2. Replace <img> tags pointing to Staffbase media with markdown images
    def _img_repl(m: re.Match) -> str:
        url = m.group(1)
        if "/api/media/" not in url:
            return ""
        proxy = _media_proxy_url(url, "i")
        return f"\n\n![Bild]({proxy})\n\n"

    result = _IMG_SRC_RE.sub(_img_repl, result)

    # 3. Strip remaining HTML tags and clean up whitespace
    result = _TAG_RE.sub(" ", result)
    result = html.unescape(result)
    result = re.sub(r"[ \t]+", " ", result)          # collapse horizontal whitespace
    result = re.sub(r"\n{3,}", "\n\n", result)       # max 2 consecutive newlines
    return result.strip()


# ---------------------------------------------------------------------------
# Client-side inline renderer (hover previews for Staffbase links)
# ---------------------------------------------------------------------------

_INLINE_RENDERER_CSS = """\
.sb-link-popup{position:fixed;z-index:9999;width:340px;max-width:90vw;
  background:var(--hub-overlay-bg,rgba(26,28,46,.92));
  backdrop-filter:blur(24px);-webkit-backdrop-filter:blur(24px);
  border:1px solid var(--hub-overlay-border,rgba(255,255,255,.10));
  border-radius:var(--hub-radius,14px);
  box-shadow:var(--hub-overlay-shadow,0 8px 32px rgba(0,0,0,.45));
  overflow:hidden;pointer-events:auto;animation:sb-popup-in .18s ease-out}
@keyframes sb-popup-in{from{opacity:0;transform:translateY(4px) scale(.98)}to{opacity:1;transform:none}}
.sb-link-popup-img{width:100%;height:180px;object-fit:cover;display:block}
.sb-link-popup-body{padding:14px}
.sb-link-popup-title{font-size:14px;font-weight:600;color:var(--hub-text,#e0e0e0);line-height:1.35;margin-bottom:6px}
.sb-link-popup-text{font-size:13px;color:var(--hub-text-muted,#8890a0);line-height:1.4;
  display:-webkit-box;-webkit-line-clamp:5;-webkit-box-orient:vertical;overflow:hidden}
.sb-link-popup-meta{font-size:11px;color:var(--hub-text-muted,#8890a0);margin-top:8px}
.sb-link-popup-loading{padding:24px 14px;text-align:center;color:var(--hub-text-muted,#8890a0);font-size:13px}
a[data-sb-link]{text-decoration-style:dotted;text-underline-offset:3px}
.sb-media-img{max-width:100%;border-radius:8px;cursor:pointer;margin:8px 0;display:block;
  transition:opacity .15s ease}.sb-media-img:hover{opacity:.85}
.sb-media-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:12px;margin:12px 0}
@media(max-width:600px){.sb-media-grid{grid-template-columns:1fr}}
.sb-media-grid>.sb-media-img{margin:0;width:100%;height:auto;object-fit:cover}
.sb-video-wrapper{border-radius:12px;overflow:hidden;background:#111}
.sb-video-player{width:100%;max-height:500px;display:block}
.sb-video-title{padding:8px 14px;font-size:13px;font-weight:500;
  color:var(--hub-text,#e0e0e0);background:rgba(0,0,0,.5)}
.sb-video-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:12px;margin:12px 0}
@media(max-width:600px){.sb-video-grid{grid-template-columns:1fr}}
"""

_INLINE_RENDERER_JS = r"""
(function(registry) {
  var _urlCache = {};  // href → {title, teaser, image, meta} or null (pending)
  var _popup = null, _timer = null;

  function _esc(s) { var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

  function hidePopup() { clearTimeout(_timer); if (_popup) { _popup.remove(); _popup = null; } }

  function showPopup(anchor, data) {
    hidePopup();
    var el = document.createElement('div');
    el.className = 'sb-link-popup';
    var html = '';
    if (data.image) html += '<img class="sb-link-popup-img" src="' + _esc(data.image) + '" alt="">';
    html += '<div class="sb-link-popup-body">';
    html += '<div class="sb-link-popup-title">' + _esc(data.title || anchor.textContent.trim()) + '</div>';
    if (data.teaser) html += '<div class="sb-link-popup-text">' + _esc(data.teaser) + '</div>';
    if (data.meta) html += '<div class="sb-link-popup-meta">' + _esc(data.meta) + '</div>';
    html += '</div>';
    el.innerHTML = html;
    document.body.appendChild(el);
    _popup = el;
    // Position
    var rect = anchor.getBoundingClientRect(), pw = 340, vw = window.innerWidth, vh = window.innerHeight, gap = 8;
    var left = rect.left; if (left + pw > vw - gap) left = vw - pw - gap; if (left < gap) left = gap;
    var top = rect.bottom + 6, ph = el.offsetHeight;
    if (top + ph > vh - gap) top = rect.top - ph - 6; if (top < gap) top = gap;
    el.style.left = left + 'px'; el.style.top = top + 'px';
    el.addEventListener('mouseenter', function() { clearTimeout(_timer); });
    el.addEventListener('mouseleave', hidePopup);
  }

  function showLoading(anchor) {
    hidePopup();
    var el = document.createElement('div');
    el.className = 'sb-link-popup';
    el.innerHTML = '<div class="sb-link-popup-loading">Lade Vorschau\u2026</div>';
    document.body.appendChild(el);
    _popup = el;
    var rect = anchor.getBoundingClientRect(), gap = 8;
    var left = rect.left; if (left + 340 > window.innerWidth - gap) left = window.innerWidth - 340 - gap;
    el.style.left = Math.max(gap, left) + 'px'; el.style.top = (rect.bottom + 6) + 'px';
    el.addEventListener('mouseenter', function() { clearTimeout(_timer); });
    el.addEventListener('mouseleave', hidePopup);
  }

  async function fetchPreview(href) {
    if (_urlCache[href] !== undefined) return _urlCache[href];
    _urlCache[href] = null; // mark pending
    try {
      var resp = await fetch('/api/sb-preview?url=' + encodeURIComponent(href));
      if (!resp.ok) { _urlCache[href] = false; return false; }
      var data = await resp.json();
      _urlCache[href] = data;
      return data;
    } catch(e) { _urlCache[href] = false; return false; }
  }

  function _autoGrid(container, selector, gridClass) {
    // Group consecutive matching elements into a grid wrapper
    var items = container.querySelectorAll(selector);
    var runs = [], cur = [];
    for (var i = 0; i < items.length; i++) {
      var el = items[i];
      if (el.closest('.' + gridClass)) continue; // already in a grid
      if (cur.length > 0) {
        // Check if this element is a sibling of the previous (ignoring whitespace text nodes)
        var prev = cur[cur.length - 1];
        var next = prev.nextElementSibling;
        // Walk past <br>, empty <p>, whitespace
        while (next && next !== el && /^(BR|HR)$/.test(next.tagName)) next = next.nextElementSibling;
        if (next === el) { cur.push(el); continue; }
      }
      if (cur.length >= 2) runs.push(cur);
      cur = [el];
    }
    if (cur.length >= 2) runs.push(cur);
    for (var r = 0; r < runs.length; r++) {
      var grid = document.createElement('div');
      grid.className = gridClass;
      runs[r][0].parentNode.insertBefore(grid, runs[r][0]);
      for (var k = 0; k < runs[r].length; k++) grid.appendChild(runs[r][k]);
    }
  }

  function openLightbox(src) {
    var ex = document.querySelector('.cv2-lightbox');
    if (ex) ex.remove();
    var lb = document.createElement('div');
    lb.className = 'cv2-lightbox';
    lb.addEventListener('click', function(e) { if (e.target === lb) lb.remove(); });
    var img = document.createElement('img');
    img.src = src;
    lb.appendChild(img);
    var closeBtn = document.createElement('button');
    closeBtn.className = 'cv2-lightbox-btn cv2-lightbox-close';
    closeBtn.innerHTML = '<span class="material-icons">close</span>';
    closeBtn.addEventListener('click', function() { lb.remove(); });
    lb.appendChild(closeBtn);
    var onKey = function(e) { if (e.key === 'Escape') { lb.remove(); document.removeEventListener('keydown', onKey); } };
    document.addEventListener('keydown', onKey);
    document.body.appendChild(lb);
  }

  registry.registerInline(async function(container) {
    // --- Staffbase link hover previews ---
    var sbHost = '{{STAFFBASE_HOST}}';
    var selector = 'a[data-sb-link], a[href*="staffbase.com"]';
    if (sbHost) selector += ', a[href*="' + sbHost + '"]';
    var links = container.querySelectorAll(selector);
    for (var i = 0; i < links.length; i++) {
      var link = links[i];
      if (link.dataset.sbBound) continue;
      link.dataset.sbBound = '1';
      link.dataset.sbLink = '1';

      (function(a) {
        a.addEventListener('mouseenter', function() {
          clearTimeout(_timer);
          _timer = setTimeout(async function() {
            var href = a.href;
            var cached = _urlCache[href];
            if (cached) { showPopup(a, cached); return; }
            if (cached === false) return;
            showLoading(a);
            var data = await fetchPreview(href);
            if (data && _popup) showPopup(a, data);
          }, 300);
        });
        a.addEventListener('mouseleave', function() {
          clearTimeout(_timer);
          _timer = setTimeout(hidePopup, 200);
        });
      })(link);
    }

    // --- MCP media images — lightbox + auto-grid ---
    var mcpImgs = container.querySelectorAll('img[src*="/api/mcp-media/"]');
    for (var j = 0; j < mcpImgs.length; j++) {
      var img = mcpImgs[j];
      if (img.dataset.mcpBound) continue;
      img.dataset.mcpBound = '1';
      img.classList.add('sb-media-img');
      (function(el) {
        el.addEventListener('click', function(e) { e.preventDefault(); openLightbox(el.src); });
      })(img);
    }
    // Group consecutive sb-media-img siblings into .sb-media-grid
    _autoGrid(container, '.sb-media-img', 'sb-media-grid');
    // Group consecutive sb-video-wrapper (from doc plugin) into .sb-video-grid
    _autoGrid(container, '.cv2-doc-plugin-block[data-lang="sb-video"]', 'sb-video-grid');
  });
})(registry);
"""

# ---------------------------------------------------------------------------
# Client-side doc plugin: sb-video (fenced code block ```sb-video)
# ---------------------------------------------------------------------------

_VIDEO_PLUGIN_JS = r"""
registry.register('sb-video', {
  render: function(el, data) {
    try {
      var info = JSON.parse(data);
      var w = document.createElement('div');
      w.className = 'sb-video-wrapper';
      if (info.title) {
        var t = document.createElement('div');
        t.className = 'sb-video-title';
        t.textContent = info.title;
        w.appendChild(t);
      }
      var v = document.createElement('video');
      v.className = 'sb-video-player';
      v.controls = true;
      v.preload = 'metadata';
      v.src = info.url;
      if (info.poster) v.poster = info.poster;
      w.appendChild(v);
      el.appendChild(w);
    } catch(e) {
      el.innerHTML = '<pre style="color:#ef4444">Error: ' + e.message + '</pre>';
    }
  }
});
"""


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

TOOLS: list[dict[str, Any]] = [
    {
        "name": "intranet_search",
        "displayName": "Intranet Search",
        "icon": "search",
        "description": (
            "Search the company intranet for pages, news articles, sections, and documents.\n"
            "Searches the page/navigation index AND all news channels simultaneously.\n"
            "Returns rich results: titles, teasers, links, dates, channels.\n"
            "For news articles, results often contain enough info to answer directly.\n"
            "For pages, use intranet_get_content with the page_id to fetch full text + PDFs.\n\n"
            "Args:\n"
            '  query: Search query. E.g. "cafeteria menu", "company news", "onboarding"\n'
            "  limit: Max results (default 15)"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "limit": {"type": "integer", "default": 15, "description": "Max results"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "intranet_get_content",
        "displayName": "Intranet Content",
        "icon": "article",
        "description": (
            "Fetch full content for one or more intranet pages/articles by their IDs.\n"
            "Accepts multiple IDs to batch-fetch in a single call.\n"
            "Includes page text with images and videos inline at their original positions,\n"
            "plus attached PDF text (auto-extracted).\n"
            "For sections/folders, returns the list of sub-pages.\n\n"
            "Args:\n"
            "  ids: List of page/installation IDs (from search results).\n"
            "       Also accepts a single ID string for convenience."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of page/installation IDs to fetch",
                },
                "page_id": {"type": "string", "description": "Single page ID (convenience alias)"},
            },
        },
    },
    {
        "name": "intranet_get_news",
        "displayName": "Intranet News",
        "icon": "newspaper",
        "description": (
            "Fetch recent news from a specific channel or global feed.\n"
            "Note: intranet_search already searches all channels. Use this only to browse\n"
            "a specific channel's latest posts.\n\n"
            "Args:\n"
            "  channel: Channel name (optional). Available: see prompt hints.\n"
            "  limit: Max posts (default 10)"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "channel": {"type": "string", "description": "Channel name (optional)"},
                "limit": {"type": "integer", "default": 10, "description": "Max posts"},
            },
        },
    },
]


# ---------------------------------------------------------------------------
# GrapevineMCPServer
# ---------------------------------------------------------------------------


class GrapevineMCPServer(InProcessMCPServer):
    """InProcess MCP server providing live Staffbase intranet access.

    At startup, fetches the space menu tree to build a local page index.
    Searches match against both this index and the Staffbase search API.
    """

    def __init__(
        self,
        *,
        config: GrapevineConfig | None = None,
        space_id: str = "",
        channels: dict[str, str] | None = None,
        enforced_pages: dict[str, str] | None = None,
        enforced_page_follow_pdfs: bool = True,
        subsidiary_channels: dict[str, dict[str, str]] | None = None,
        display_name: str = "Grapevine for Staffbase",
        user_email: str = "",
        local_channel_id: str = "",
    ) -> None:
        if config is None:
            config = GrapevineConfig(
                base_url=os.environ.get("STAFFBASE_BASE_URL", os.environ.get("STAFFBASE_URL", "")),
                api_key=os.environ.get("STAFFBASE_API_KEY", ""),
                space_id=space_id,
                channels=channels or {},
                enforced_pages=enforced_pages or {},
                enforced_page_follow_pdfs=enforced_page_follow_pdfs,
                subsidiary_channels=subsidiary_channels or {},
                display_name=display_name,
            )
        self._config = config
        self._user_email = user_email
        self._local_channel_id = local_channel_id

        # Resolve user-specific channels
        self._channels = dict(config.channels)
        if config.subsidiary_channels and user_email:
            domain = user_email.rsplit("@", 1)[-1].lower() if "@" in user_email else ""
            sub_channels = config.subsidiary_channels.get(domain)
            if sub_channels:
                self._channels.update(sub_channels)

        # Lazy client/adapter
        self._client: StaffbaseClient | None = None
        self._adapter: StaffbaseAdapter | None = None

        # Menu index: list of _MenuEntry for local search
        self._menu_index: list[_MenuEntry] = []

        # Enforced page cache: label → {"text": ..., "pdf_texts": [...]}
        self._enforced_cache: dict[str, dict[str, Any]] = {}
        self._initialized = False

    def _get_user_local_channel(self) -> str | None:
        """Return the local channel ID for the current user, or None."""
        return self._local_channel_id or None

    def _get_client_adapter(self) -> tuple[StaffbaseClient, StaffbaseAdapter]:
        if self._adapter is None:
            self._client, self._adapter = create_adapter(
                base_url=self._config.base_url or None,
                api_key=self._config.api_key or None,
                preferred_locales=self._config.preferred_locales,
            )
        return self._client, self._adapter  # type: ignore[return-value]

    # ── Initialization ────────────────────────────────────────

    async def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        client, adapter = self._get_client_adapter()

        # Run menu index and enforced pages in parallel
        await asyncio.gather(
            self._build_menu_index(client),
            self._load_enforced_pages(client, adapter),
        )

    async def _build_menu_index(self, client: StaffbaseClient) -> None:
        """Fetch the space menu tree and build a local page index.

        The space menu API returns a flat list where folders have children=0.
        We expand folders by fetching their sub-menus in parallel.
        """
        space_id = self._config.space_id
        if not space_id:
            try:
                spaces = await client.list_spaces()
                if spaces:
                    space_id = spaces[0].get("id", "")
            except Exception as e:
                logger.warning(f"[GRAPEVINE] Failed to list spaces: {e}")
                return

        if not space_id:
            logger.warning("[GRAPEVINE] No space_id — cannot build menu index")
            return

        try:
            data = await client._get(f"/api/spaces/{space_id}/menu", params={"platform": "web"})
        except Exception as e:
            logger.warning(f"[GRAPEVINE] Failed to fetch menu for space {space_id}: {e}")
            return

        # Parse top-level menu items
        items = data.get("data", [data]) if isinstance(data, dict) else data
        if isinstance(items, list):
            for item in items:
                self._parse_menu_node(item, path=[])

        # Expand folders that have no children loaded yet (lazy-loaded by API)
        folders_to_expand = [
            e for e in self._menu_index
            if e.node_type == "folder"
        ]

        async def _expand_folder(entry: _MenuEntry) -> None:
            try:
                folder_data = await client.get_menu(entry.menu_id)
                children = folder_data.get("children", {})
                if isinstance(children, dict):
                    for child in children.get("data", []):
                        self._parse_menu_node(child, path=entry.path)
            except Exception as e:
                logger.debug(f"[GRAPEVINE] Failed to expand folder '{entry.title}': {e}")

        if folders_to_expand:
            await asyncio.gather(*[_expand_folder(f) for f in folders_to_expand])

        logger.info(f"[GRAPEVINE] Menu index built: {len(self._menu_index)} entries")

    def _parse_menu_node(self, node: dict, path: list[str]) -> None:
        """Recursively parse a menu node and add entries to the index."""
        # Extract title from localization config
        title = ""
        cfg = node.get("config", {})
        loc = cfg.get("localization", {})
        for lang in self._config.preferred_locales:
            if lang in loc:
                title = loc[lang].get("title", "")
                if title:
                    break
        if not title:
            # Fallback: try any locale
            for lang_data in loc.values():
                title = lang_data.get("title", "")
                if title:
                    break

        node_type = node.get("nodeType", "")
        menu_id = node.get("id", "")
        installation_id = node.get("installationID", "")
        plugin_id = node.get("pluginID", "")

        current_path = path + [title] if title else path

        if title and node_type in ("installation", "folder"):
            self._menu_index.append(_MenuEntry(
                title=title,
                menu_id=menu_id,
                installation_id=installation_id,
                node_type=node_type,
                path=current_path,
                plugin_id=plugin_id,
            ))

        # Recurse into children
        children = node.get("children", {})
        if isinstance(children, dict):
            for child in children.get("data", []):
                self._parse_menu_node(child, current_path)

    def _search_menu_index(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """Search the local menu index by title substring match."""
        query_lower = query.lower()
        query_words = query_lower.split()
        fragments = _query_fragments(query)
        results = []

        for entry in self._menu_index:
            title_lower = entry.title.lower()
            path_str = " > ".join(entry.path).lower()
            haystack = f"{title_lower} {path_str}"

            # Score: exact title match > all words match > partial > fragment
            if query_lower == title_lower:
                score = 100
            elif query_lower in title_lower:
                score = 80
            elif all(w in haystack for w in query_words):
                score = 60
            elif any(w in haystack for w in query_words):
                score = 30
            elif fragments and any(f in haystack for f in fragments):
                score = 15
            else:
                continue

            page_id = entry.installation_id or entry.menu_id
            # Use menu node ID for URLs (what the website uses), fallback to page_id
            url_id = entry.menu_id or page_id
            base_url = self._config.base_url or os.environ.get("STAFFBASE_BASE_URL", "")
            page_url = f"{base_url.rstrip('/')}/content/page/{url_id}" if base_url else ""

            results.append((score, {
                "type": "page" if entry.plugin_id == "page" else entry.node_type,
                "title": entry.title,
                "page_id": page_id,
                "path": " > ".join(entry.path),
                "source": "menu_index",
                "markdown": _md_link(entry.title, page_url) if page_url else "",
            }))

        results.sort(key=lambda x: -x[0])
        return [r[1] for r in results[:limit]]

    # ── Enforced pages ────────────────────────────────────────

    async def _load_enforced_pages(self, client: StaffbaseClient, adapter: StaffbaseAdapter) -> None:
        if not self._config.enforced_pages:
            return

        async def _fetch_page(page_id: str) -> dict[str, Any] | None:
            try:
                page_data = await client.get_page(page_id)
                contents = page_data.get("contents", {})
                localized = adapter.resolve_locale(contents)
                title = localized.get("title", "")
                content_html = localized.get("content", "")
                page_text = _html_to_text(content_html)

                if not title and not page_text:
                    return None

                entry: dict[str, Any] = {
                    "title": title,
                    "page_id": page_id,
                    "text": page_text,
                    "pdf_texts": [],
                }

                if self._config.enforced_page_follow_pdfs:
                    pdf_urls = _extract_pdf_urls(content_html)
                    for pdf_url in pdf_urls:
                        try:
                            pdf_bytes = await client.download_media(pdf_url)
                            pdf_text = await _extract_pdf_text(pdf_bytes)
                            if pdf_text.strip():
                                filename = pdf_url.split("/")[-1]
                                entry["pdf_texts"].append({
                                    "filename": filename,
                                    "text": pdf_text,
                                })
                        except Exception as e:
                            logger.warning(f"Failed to fetch PDF {pdf_url}: {e}")

                return entry
            except Exception as e:
                logger.warning(f"[GRAPEVINE] Failed to fetch page {page_id}: {e}")
                return None

        async def _fetch_enforced(label: str, enforced_id: str) -> None:
            try:
                child_ids = await client.get_menu_page_ids(enforced_id)
                if child_ids:
                    logger.info(f"[GRAPEVINE] Enforced '{label}': menu with {len(child_ids)} child pages")
                    sub_entries: list[dict[str, Any]] = []
                    for cid in child_ids:
                        entry = await _fetch_page(cid)
                        if entry:
                            sub_entries.append(entry)

                    if sub_entries:
                        combined_text = "\n\n".join(
                            f"## {e['title']}\n{e['text']}" for e in sub_entries if e["text"]
                        )
                        all_pdfs = []
                        for e in sub_entries:
                            all_pdfs.extend(e.get("pdf_texts", []))

                        self._enforced_cache[label] = {
                            "title": label,
                            "page_id": enforced_id,
                            "text": combined_text,
                            "pdf_texts": all_pdfs,
                            "sub_pages": [
                                {"title": e["title"], "page_id": e["page_id"]}
                                for e in sub_entries
                            ],
                        }
                        logger.info(
                            f"[GRAPEVINE] Enforced '{label}': "
                            f"{len(combined_text)} chars, {len(all_pdfs)} PDFs "
                            f"from {len(sub_entries)} pages"
                        )
                        return
            except Exception:
                pass

            entry = await _fetch_page(enforced_id)
            if entry:
                self._enforced_cache[label] = entry
                logger.info(
                    f"[GRAPEVINE] Enforced page '{label}': "
                    f"{len(entry['text'])} chars, {len(entry['pdf_texts'])} PDFs"
                )
            else:
                logger.warning(f"[GRAPEVINE] Enforced '{label}' ({enforced_id}): no content found")

        tasks = [_fetch_enforced(label, pid) for label, pid in self._config.enforced_pages.items()]
        await asyncio.gather(*tasks)

    # ── InProcessMCPServer interface ──────────────────────────

    async def get_prompt_hints(self) -> list[str]:
        await self._ensure_initialized()

        dn = self._config.display_name  # e.g. "myFLOW", "Staffbase"
        channel_list = ", ".join(self._channels.keys()) if self._channels else "(none)"
        hints = [
            f"## {dn} Search (Grapevine) — RULES\n\n"
            f"### Rule 1: ALWAYS search {dn} FIRST\n"
            "For ANY question about the company, internal processes, HR, events, canteen, policies, "
            f"org structure, or anything that could be on {dn}: you MUST call intranet_search "
            "BEFORE answering. NEVER answer from general knowledge alone.\n\n"
            "### Rule 2: Two-call pattern (search → batch fetch)\n"
            "1. Call `intranet_search` — returns pages, news articles, and documents with teasers and links.\n"
            "   For news articles, the search result often has enough info (title, teaser, date, link) to answer.\n"
            "2. If you need full page content or PDFs, call `intranet_get_content` with ALL relevant page_ids "
            "   in a single call: `{\"ids\": [\"id1\", \"id2\", \"id3\"]}`. This fetches them in parallel.\n"
            "   Do NOT call intranet_get_content multiple times — batch all IDs into one call.\n\n"
            "### Rule 3: Link format — use the `markdown` field\n"
            "NEVER write 'Artikel öffnen' or 'Link' or bare URLs. The article title IS the link.\n"
            "Each search result includes a `markdown` field with a ready-to-use link.\n"
            "Copy the `markdown` field into your response. Hover previews are added automatically.\n\n"
            "### Rule 4: Media (images & videos)\n"
            "Page content from `intranet_get_content` contains images (`![Bild](...)`) and videos "
            "(` ```sb-video ``` blocks) inline at their original positions.\n"
            "When presenting page content, **preserve media at their positions** in the text flow.\n"
            "Do NOT move all media to the end or group them separately — they belong where they appear.\n"
            "Copy the markdown image syntax and sb-video code blocks exactly as they appear in the content.\n\n"
            f"### Available news channels\n{channel_list}\n"
        ]

        if self._enforced_cache:
            enforced_lines = ["### Always-available pages\n"]
            for label, entry in self._enforced_cache.items():
                pdf_info = ""
                if entry["pdf_texts"]:
                    pdf_names = [p["filename"] for p in entry["pdf_texts"]]
                    pdf_info = f" (PDFs: {', '.join(pdf_names)})"
                enforced_lines.append(
                    f"- **{label}**: page_id=`{entry['page_id']}`{pdf_info}"
                )
            enforced_lines.append(
                "\nThese pages are pre-loaded. Use intranet_get_page with their page_id "
                "to get full content including extracted PDF text.\n"
            )
            hints.append("\n".join(enforced_lines))

        return hints

    async def get_client_renderers(self) -> list[dict[str, str]]:
        # Inject the configured base_url host so link previews match the instance
        from urllib.parse import urlparse
        host = urlparse(self._config.base_url).hostname or ""
        js = _INLINE_RENDERER_JS.replace("{{STAFFBASE_HOST}}", host)
        return [
            {"type": "inline", "js": js, "css": _INLINE_RENDERER_CSS},
            {"lang": "sb-video", "js": _VIDEO_PLUGIN_JS, "css": ""},
        ]

    async def list_tools(self) -> list[dict]:
        name = self._config.display_name
        import copy, re
        tools = copy.deepcopy(TOOLS)
        # Replace "Intranet"/"intranet" only when used as a standalone noun,
        # NOT inside tool names like intranet_search / intranet_get_content.
        _standalone = re.compile(r'(?i)(?<![_a-zA-Z])(intranet)(?![_a-zA-Z])')
        for tool in tools:
            if "displayName" in tool:
                tool["displayName"] = tool["displayName"].replace("Intranet", name)
            if "description" in tool:
                tool["description"] = _standalone.sub(name, tool["description"])
        return tools

    async def call_tool(self, name: str, arguments: dict) -> str:
        await self._ensure_initialized()

        dispatch = {
            "intranet_search": self._call_search,
            "intranet_get_content": self._call_get_content,
            "intranet_get_page": self._call_get_content,  # backwards compat
            "intranet_get_news": self._call_get_news,
        }
        handler = dispatch.get(name)
        if handler is None:
            return f"Unknown tool: {name}"
        try:
            return await handler(arguments)
        except Exception as e:
            logger.exception(f"[GRAPEVINE] Tool {name} failed")
            return json.dumps({"error": str(e)})

    # ── Tool implementations ──────────────────────────────────

    async def _call_search(self, arguments: dict) -> str:
        query = arguments.get("query", "")
        if not query:
            return "Error: 'query' argument is required."
        limit = min(max(1, arguments.get("limit", 15)), 30)

        # 1. Search local menu index (pages, sections)
        menu_hits = self._search_menu_index(query, limit=limit)

        # 2. Search enforced pages cache
        query_lower = query.lower()
        enforced_hits = []
        for label, entry in self._enforced_cache.items():
            haystack = f"{entry['title']} {entry['text']}"
            for pdf in entry.get("pdf_texts", []):
                haystack += f" {pdf['filename']} {pdf['text']}"
            if query_lower in haystack.lower():
                enforced_hits.append({
                    "type": "enforced_page",
                    "title": entry["title"],
                    "label": label,
                    "page_id": entry["page_id"],
                    "has_pdfs": bool(entry["pdf_texts"]),
                    "source": "enforced",
                })

        # 3. Search Staffbase API (news posts)
        api_hits = []
        try:
            client, _ = self._get_client_adapter()
            data = await client.search(query, limit=limit)
            entries = data.get("entries", data.get("data", []))
            if isinstance(entries, dict):
                entries = entries.get("data", [])
            if not isinstance(entries, list):
                entries = []

            for r in entries[:limit]:
                if not isinstance(r, dict):
                    continue
                item: dict[str, Any] = {
                    "type": r.get("type", ""),
                    "title": r.get("title", r.get("name", "")),
                    "id": r.get("id", ""),
                    "source": "api",
                }
                teaser = r.get("content", r.get("teaser", ""))
                if teaser:
                    item["teaser"] = teaser[:300]
                link = r.get("link", "")
                if link:
                    item["link"] = link
                if r.get("publishedAt") or r.get("published"):
                    item["published"] = r.get("publishedAt", r.get("published", ""))
                breadcrumbs = r.get("breadcrumbs", [])
                if breadcrumbs and isinstance(breadcrumbs, list):
                    item["breadcrumbs"] = " > ".join(
                        b.get("title", "") for b in breadcrumbs if isinstance(b, dict)
                    )
                api_hits.append(item)
        except Exception as e:
            logger.warning(f"[GRAPEVINE] Staffbase search API error: {e}")

        # 4. Search all configured news channels for matching articles
        news_hits = await self._search_news_channels(query, limit=limit)

        # Combine: enforced first, then menu hits, then news hits, then API results
        # Deduplicate by title
        seen_titles: set[str] = set()
        output: list[dict[str, Any]] = []

        for hit in enforced_hits + menu_hits + news_hits + api_hits:
            title = hit.get("title", "")
            if title.lower() in seen_titles:
                continue
            seen_titles.add(title.lower())
            output.append(hit)
            if len(output) >= limit:
                break

        if not output:
            return "No results found."
        return json.dumps(output, indent=2, ensure_ascii=False, default=str)

    async def _search_news_channels(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """Search all configured news channels for articles matching the query."""
        if not self._channels:
            return []

        query_lower = query.lower()
        query_words = query_lower.split()
        fragments = _query_fragments(query)
        _, adapter = self._get_client_adapter()

        async def _search_channel(name: str, cid: str) -> list[tuple[int, dict[str, Any]]]:
            try:
                articles = await adapter.get_channel_articles(cid, limit=20, channel_name=name)
            except Exception as e:
                logger.debug(f"[GRAPEVINE] Failed to search channel '{name}': {e}")
                return []

            hits: list[tuple[int, dict[str, Any]]] = []
            for article in articles:
                haystack = f"{article.title} {article.teaser}".lower()
                if query_lower in haystack:
                    score = 90
                elif all(w in haystack for w in query_words):
                    score = 70
                elif any(w in haystack for w in query_words):
                    score = 40
                elif fragments and any(f in haystack for f in fragments):
                    # Compound-word fragment match (e.g. "wahl" from "betriebsratswahl"
                    # matches "wahlvorschläge")
                    score = 25
                else:
                    continue

                item: dict[str, Any] = {
                    "type": "news_article",
                    "title": article.title,
                    "channel": name,
                    "published": article.published_label,
                    "author": article.author_name,
                    "link": article.web_link,
                    "source": "channel_search",
                    "markdown": _md_link(article.title, article.web_link),
                }
                if article.teaser:
                    item["teaser"] = article.teaser[:500]
                if article.id:
                    item["id"] = article.id
                hits.append((score, item))
            return hits

        # Query all channels in parallel
        channel_results = await asyncio.gather(
            *[_search_channel(name, cid) for name, cid in self._channels.items()]
        )

        # Flatten and sort by score
        all_hits: list[tuple[int, dict[str, Any]]] = []
        for hits in channel_results:
            all_hits.extend(hits)
        all_hits.sort(key=lambda x: -x[0])

        return [h[1] for h in all_hits[:limit]]

    async def _call_get_content(self, arguments: dict) -> str:
        # Accept both "ids" (list or string) and "page_id" (single string)
        ids_arg = arguments.get("ids", arguments.get("page_id", ""))
        if isinstance(ids_arg, str):
            ids = [ids_arg] if ids_arg else []
        elif isinstance(ids_arg, list):
            ids = [i for i in ids_arg if isinstance(i, str) and i]
        else:
            ids = []

        if not ids:
            return "Error: 'ids' argument is required (string or list of strings)."

        # Fetch all pages in parallel
        results = await asyncio.gather(*[self._fetch_single_page(pid) for pid in ids])

        # Single ID → return the object directly; multiple → return array
        if len(ids) == 1:
            return json.dumps(results[0], indent=2, ensure_ascii=False)
        return json.dumps(results, indent=2, ensure_ascii=False)

    async def _fetch_single_page(self, page_id: str) -> dict[str, Any]:
        """Fetch a single page/article by ID. Returns a result dict."""
        # Check enforced cache first
        for label, entry in self._enforced_cache.items():
            if entry["page_id"] == page_id:
                result: dict[str, Any] = {
                    "title": entry["title"],
                    "page_id": page_id,
                    "content": entry["text"],
                }
                if entry.get("sub_pages"):
                    result["sub_pages"] = entry["sub_pages"]
                if entry["pdf_texts"]:
                    result["attached_pdfs"] = [
                        {"filename": p["filename"], "text": p["text"]}
                        for p in entry["pdf_texts"]
                    ]
                return result

        # Check if this is a menu entry (might need installationID → page fetch)
        menu_entry = None
        for me in self._menu_index:
            if page_id in (me.installation_id, me.menu_id):
                menu_entry = me
                break

        # If it's a folder, list its children
        if menu_entry and menu_entry.node_type == "folder":
            children = [
                {"title": e.title, "page_id": e.installation_id or e.menu_id, "type": e.node_type}
                for e in self._menu_index
                if len(e.path) > len(menu_entry.path)
                and e.path[:len(menu_entry.path)] == menu_entry.path
                and len(e.path) == len(menu_entry.path) + 1
            ]
            return {
                "title": menu_entry.title,
                "type": "folder",
                "page_id": page_id,
                "children": children,
            }

        # Live fetch the page
        client, adapter = self._get_client_adapter()

        actual_id = page_id
        if menu_entry and menu_entry.installation_id:
            actual_id = menu_entry.installation_id

        try:
            page_data = await client.get_page(actual_id)
        except Exception:
            if actual_id != page_id:
                try:
                    page_data = await client.get_page(page_id)
                except Exception as e:
                    return {"page_id": page_id, "error": str(e)}
            else:
                return {"page_id": page_id, "error": "Page not found"}

        contents = page_data.get("contents", {})
        localized = adapter.resolve_locale(contents)
        title = localized.get("title", "")
        content_html = localized.get("content", "")
        # Convert HTML to text with media markers inline at their original positions
        page_text = _html_to_text_with_media(content_html)

        base_url = self._config.base_url or os.environ.get("STAFFBASE_BASE_URL", "")
        result = {
            "title": title,
            "page_id": page_id,
            "content": page_text,
            "updated": page_data.get("updatedAt", ""),
        }
        if base_url:
            # Use menu node ID for the URL (what the website uses), not the installationID
            url_id = menu_entry.menu_id if menu_entry and menu_entry.menu_id else page_id
            result["url"] = f"{base_url.rstrip('/')}/content/page/{url_id}"

        # Extract and follow PDFs
        pdf_urls = _extract_pdf_urls(content_html)
        if pdf_urls:
            attached_pdfs = []
            for pdf_url in pdf_urls:
                try:
                    pdf_bytes = await client.download_media(pdf_url)
                    pdf_text = await _extract_pdf_text(pdf_bytes)
                    if pdf_text.strip():
                        attached_pdfs.append({
                            "filename": pdf_url.split("/")[-1],
                            "text": pdf_text,
                        })
                except Exception as e:
                    logger.warning(f"Failed to fetch PDF {pdf_url}: {e}")
                    attached_pdfs.append({
                        "filename": pdf_url.split("/")[-1],
                        "error": str(e),
                    })
            if attached_pdfs:
                result["attached_pdfs"] = attached_pdfs

        # If page is empty but it's a menu node, try listing children
        if not page_text and menu_entry:
            try:
                child_ids = await client.get_menu_page_ids(menu_entry.menu_id)
                if child_ids:
                    result["note"] = "This page has no text content. It may be a section with sub-pages."
                    result["child_page_ids"] = child_ids
            except Exception:
                pass

        return result

    async def _call_get_news(self, arguments: dict) -> str:
        channel_name = arguments.get("channel", "")
        limit = min(max(1, arguments.get("limit", 10)), 20)

        _, adapter = self._get_client_adapter()

        if channel_name:
            channel_lower = channel_name.lower()
            channel_id = None
            matched_name = ""
            for name, cid in self._channels.items():
                if name.lower() == channel_lower:
                    channel_id = cid
                    matched_name = name
                    break
            if not channel_id:
                available = ", ".join(self._channels.keys()) if self._channels else "(none configured)"
                return f"Channel '{channel_name}' not found. Available: {available}"

            articles = await adapter.get_channel_articles(
                channel_id, limit=limit, channel_name=matched_name
            )
        else:
            # No channel specified: fetch global + user's local channel
            # (same pattern as the hub news widget).
            import asyncio as _aio
            tasks = [adapter.get_global_articles(limit=limit)]
            # Include the user's regional local channel if configured
            local_cid = self._get_user_local_channel()
            if local_cid:
                tasks.append(adapter.get_channel_articles(
                    local_cid, limit=max(5, limit // 2), channel_name="Local",
                ))
            results = await _aio.gather(*tasks, return_exceptions=True)
            articles = []
            seen_ids = set()
            for r in results:
                if isinstance(r, Exception):
                    continue
                for a in (r if isinstance(r, list) else []):
                    aid = getattr(a, "id", None) or id(a)
                    if aid not in seen_ids:
                        seen_ids.add(aid)
                        articles.append(a)
            articles.sort(key=lambda a: getattr(a, "published", "") or "", reverse=True)
            articles = articles[:limit]

        result = [a.to_dict() for a in articles]
        return json.dumps(result, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Preview API router (FastAPI) — fetches Staffbase content for hover popups
# ---------------------------------------------------------------------------

def build_preview_router():
    """Build a FastAPI router with preview + media proxy endpoints.

    Endpoints:
      - ``GET /api/sb-preview?url=...`` — article/page metadata for hover popups
      - ``GET /api/mcp-media/grapevine/{type}/{encoded}`` — media proxy (images/videos)

    Uses the Staffbase SDK. Results are cached server-side.
    """
    from fastapi import APIRouter, Query
    from fastapi.responses import JSONResponse, Response
    from urllib.parse import urlparse
    import time as _time

    router = APIRouter()

    # Server-side cache: url → (timestamp, data_dict)
    _cache: dict[str, tuple[float, dict[str, Any]]] = {}
    _CACHE_TTL = 300  # 5 minutes
    # Media cache: key → (timestamp, bytes, content_type)  — images only
    _media_cache: dict[str, tuple[float, bytes, str]] = {}
    _media_cache_size: int = 0  # current total bytes in cache
    _MEDIA_CACHE_TTL = 3600  # 1 hour
    _MEDIA_CACHE_MAX_BYTES = 100 * 1024 * 1024  # 100 MB
    _client_adapter: list = []  # lazy [client, adapter]

    def _get_ca():
        if not _client_adapter:
            c, a = create_adapter()
            _client_adapter.extend([c, a])
        return _client_adapter[0], _client_adapter[1]

    def _extract_id_and_type(url: str) -> tuple[str, str]:
        """Extract content ID and type from a Staffbase URL.

        Returns (content_id, content_type) where type is 'article' or 'page'.
        """
        parsed = urlparse(url)
        path = parsed.path

        # /openlink/content/news/article/ID
        if "/news/article/" in path:
            parts = path.split("/news/article/")
            return parts[-1].strip("/"), "article"

        # /openlink/content/page/ID or /content/page/ID
        if "/content/page/" in path:
            parts = path.split("/content/page/")
            return parts[-1].strip("/"), "page"

        # /api/pages/ID
        if "/api/pages/" in path:
            parts = path.split("/api/pages/")
            return parts[-1].strip("/"), "page"

        return "", ""

    @router.get("/api/sb-preview")
    async def sb_preview(url: str = Query(...), request: _Request = None):
        # Auth — require valid session cookie
        if request and not request.session.get("id"):
            return JSONResponse({"error": "Not authenticated"}, status_code=401)
        # Check cache
        now = _time.time()
        cached = _cache.get(url)
        if cached and (now - cached[0]) < _CACHE_TTL:
            return JSONResponse(cached[1])

        content_id, content_type = _extract_id_and_type(url)
        if not content_id:
            return JSONResponse({"error": "unrecognized URL"}, status_code=400)

        client, adapter = _get_ca()

        try:
            if content_type == "article":
                # Fetch article via channel posts search is expensive;
                # use the direct post endpoint instead
                post = await client._get(f"/api/posts/{content_id}")
                localized = adapter.resolve_locale(post.get("contents", {}))
                title = localized.get("title", "")
                teaser = (localized.get("teaser", "") or "")[:500]
                if not teaser:
                    teaser = _html_to_text(localized.get("content", ""))[:500]

                # Extract image from locale-resolved content (same as adapter._extract_thumbnail)
                image = ""
                image_data = localized.get("image", {}) or {}
                for variant in ("compact", "original_scaled", "original"):
                    img = image_data.get(variant)
                    if img and img.get("url"):
                        image = img["url"]
                        break

                # Meta: date · author
                published = post.get("published", post.get("publishedAt", ""))
                meta_parts = []
                if published:
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                        meta_parts.append(dt.strftime("%d.%m.%Y"))
                    except Exception:
                        pass
                author = post.get("author", {}) or {}
                if author.get("firstName"):
                    meta_parts.append(f"{author['firstName']} {author.get('lastName', '')}".strip())

                result = {
                    "title": title,
                    "teaser": teaser,
                    "image": image,
                    "meta": " · ".join(meta_parts),
                }

            else:  # page
                page_data = await client.get_page(content_id)
                localized = adapter.resolve_locale(page_data.get("contents", {}))
                title = localized.get("title", "")
                content_html = localized.get("content", "")
                teaser = _html_to_text(content_html)[:500]

                result = {
                    "title": title,
                    "teaser": teaser,
                    "image": "",
                    "meta": "Page",
                }

        except Exception as e:
            logger.warning(f"[SB-PREVIEW] Failed to fetch {content_type} {content_id}: {e}")
            return JSONResponse({"error": "Failed to fetch content"}, status_code=502)

        _cache[url] = (now, result)
        return JSONResponse(result)

    # ── MCP media proxy ─────────────────────────────────────────────

    def _guess_content_type(url: str, media_type: str) -> str:
        lo = url.lower()
        if media_type == "v":
            return "video/mp4"
        if lo.endswith(".png"):
            return "image/png"
        if lo.endswith(".gif"):
            return "image/gif"
        if lo.endswith(".webp"):
            return "image/webp"
        if lo.endswith(".svg"):
            return "image/svg+xml"
        return "image/jpeg"

    def _decode_media_url(encoded: str) -> str:
        padded = encoded + "=" * (-len(encoded) % 4)
        return _b64.urlsafe_b64decode(padded).decode()

    @router.get("/api/mcp-media/{mcp_id}/{media_type}/{encoded}")
    async def mcp_media_proxy(mcp_id: str, media_type: str, encoded: str, request: _Request):
        # Auth — require valid session cookie
        if request and not request.session.get("id"):
            return Response(status_code=401, content="Not authenticated")
        if mcp_id != "grapevine":
            return Response(status_code=404, content="Unknown MCP")
        if media_type not in ("i", "v"):
            return Response(status_code=400, content="Invalid media type")

        try:
            original_url = _decode_media_url(encoded)
        except Exception:
            return Response(status_code=400, content="Invalid encoding")

        if "/media/" not in original_url:
            return Response(status_code=400, content="Invalid media URL")

        # SSRF guard — only allow URLs pointing to the configured Staffbase host
        _sb_url = os.environ.get("STAFFBASE_URL", "")
        if _sb_url:
            from urllib.parse import urlparse as _up
            allowed_host = _up(_sb_url).hostname
            url_host = _up(original_url).hostname
            if url_host and allowed_host and url_host != allowed_host:
                return Response(status_code=400, content="URL host not allowed")

        ct = _guess_content_type(original_url, media_type)

        # ── Images: small, cache in memory ──
        if media_type == "i":
            now = _time.time()
            cache_key = f"i:{encoded}"
            cached = _media_cache.get(cache_key)
            if cached and (now - cached[0]) < _MEDIA_CACHE_TTL:
                return Response(
                    content=cached[1], media_type=cached[2],
                    headers={"Cache-Control": "private, max-age=3600"},
                )
            client, _ = _get_ca()
            try:
                data = await client.download_media(original_url)
            except Exception as e:
                logger.warning(f"[MCP-MEDIA] Image fetch failed: {e}")
                return Response(status_code=502, content="Media fetch failed")
            if len(data) < 2_000_000:
                # Remove old entry size if replacing a stale/existing key
                old = _media_cache.pop(cache_key, None)
                if old:
                    _media_cache_size -= len(old[1])
                # Evict oldest entries until we're under the 100 MB cap
                while _media_cache and _media_cache_size + len(data) > _MEDIA_CACHE_MAX_BYTES:
                    oldest_key = min(_media_cache, key=lambda k: _media_cache[k][0])
                    _media_cache_size -= len(_media_cache.pop(oldest_key)[1])
                _media_cache[cache_key] = (now, data, ct)
                _media_cache_size += len(data)
            return Response(
                content=data, media_type=ct,
                headers={"Cache-Control": "private, max-age=3600"},
            )

        # ── Videos: stream through without buffering ──
        import httpx
        from starlette.responses import StreamingResponse

        # Build the upstream URL the same way StaffbaseClient does
        client, _ = _get_ca()
        upstream_client = client._ensure_client()

        media_path = original_url
        if media_path.startswith(("http://", "https://")):
            from urllib.parse import urlparse as _urlparse
            media_path = _urlparse(media_path).path
        if not media_path.startswith("/api/"):
            media_path = f"/api/{media_path}"

        # Forward range header from browser for seeking support
        req_headers: dict[str, str] = {}
        range_header = request.headers.get("range")
        if range_header:
            req_headers["Range"] = range_header

        try:
            upstream_req = upstream_client.build_request(
                "GET", media_path, headers=req_headers,
            )
            upstream_resp = await upstream_client.send(upstream_req, stream=True)
        except Exception as e:
            logger.warning(f"[MCP-MEDIA] Video stream failed: {e}")
            return Response(status_code=502, content="Media stream failed")

        # Mirror status (200 or 206) and relevant headers
        resp_headers: dict[str, str] = {"Cache-Control": "private, max-age=3600"}
        for h in ("content-length", "content-range", "accept-ranges"):
            val = upstream_resp.headers.get(h)
            if val:
                resp_headers[h] = val
        if "accept-ranges" not in resp_headers:
            resp_headers["Accept-Ranges"] = "bytes"

        async def _stream():
            try:
                async for chunk in upstream_resp.aiter_bytes(chunk_size=65536):
                    yield chunk
            finally:
                await upstream_resp.aclose()

        return StreamingResponse(
            _stream(),
            status_code=upstream_resp.status_code,
            media_type=ct,
            headers=resp_headers,
        )

    return router

