"""Entry point: python -m grapevine_mcp"""

import asyncio

from grapevine_mcp.server import main

asyncio.run(main())
