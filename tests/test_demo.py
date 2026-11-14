"""The local demo must traverse a fixed graph without external services."""

from __future__ import annotations

import pytest

from scale_kg.demo import run_demo


@pytest.mark.asyncio
async def test_demo_traverses_fixture_without_external_services() -> None:
    output = await run_demo()
    assert "adapter=in_memory" in output
    assert "nodes=3 edges=2" in output
    assert (
        "customer:org:acme -> technology:lang:python -> product:sdk:scale-kg" in output
    )
