# grapevine-mcp

Unofficial MCP server for read-only access to [Staffbase](https://staffbase.com) intranet data.

Exposes Staffbase spaces, news posts (global and local channels), pages, and
search via the [Model Context Protocol](https://modelcontextprotocol.io/).

## Setup

```bash
poetry install
```

## Configuration

Set two environment variables:

| Variable | Description | Example |
|---|---|---|
| `STAFFBASE_URL` | Your Staffbase instance URL | `https://app.staffbase.com` |
| `STAFFBASE_API_KEY` | Base64-encoded API token | `NjlhMm...` |

The API token needs **Editor** access level to read local space content.

## Usage

Run as a stdio MCP server:

```bash
python -m grapevine_mcp
```

## Available Tools

| Tool | Description |
|---|---|
| `list_spaces` | List all spaces (locations / sub-instances) |
| `get_news` | Fetch global or local channel news posts |
| `list_channels` | List news channels in a space |
| `get_page` | Read a page by ID |
| `search` | Full-text search across content |

## License

Business Source License 1.1 (BSL-1.1) — see [LICENSE](LICENSE) for details.
