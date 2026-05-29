from __future__ import annotations

import asyncio
from unittest.mock import patch

from gnetcli_adapter.gnetcli_adapter import GnetcliFetcher


def test_make_api_uses_url_without_starter():
    fetcher = GnetcliFetcher(url="127.0.0.1:50050")

    async def run():
        with patch("gnetcli_adapter.gnetcli_adapter.GnetcliStarter") as starter:
            async with fetcher.make_api() as api:
                assert api._server == "127.0.0.1:50050"
            starter.assert_not_called()

    asyncio.run(run())
