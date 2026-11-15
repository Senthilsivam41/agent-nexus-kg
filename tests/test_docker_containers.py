"""Integration tests for Neo4j, HugeGraph, and PuppyGraph container services."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from scale_kg.adapters import (
    ContainerTransport,
    GraphStore,
    HugeGraphContainerApp,
    HugeGraphStore,
    IcebergPuppyGraphStore,
    InMemoryGraphStore,
    Neo4jContainerApp,
    Neo4jGraphStore,
    PuppyGraphContainerApp,
    create_app,
)
from scale_kg.adapters.container_server import _handle_client, serve_app
from scale_kg.benchmark import GoldenQuerySuite


@pytest.mark.asyncio
class TestContainerServices:
    async def test_container_apps_pass_bucketed_golden_parity(
        self, tmp_path: Path
    ) -> None:
        neo4j_app = Neo4jContainerApp()
        huge_app = HugeGraphContainerApp()
        puppy_wh = tmp_path / "puppy_warehouse"
        puppy_app = PuppyGraphContainerApp(default_warehouse=puppy_wh)

        stores: dict[str, GraphStore] = {
            "in_memory": InMemoryGraphStore(),
            "neo4j_container": Neo4jGraphStore(
                uri="http://neo4j.container:7474",
                transport=ContainerTransport(neo4j_app),
            ),
            "hugegraph_container": HugeGraphStore(
                base_url="http://hugegraph.container:8080",
                transport=ContainerTransport(huge_app),
            ),
            "puppygraph_container": IcebergPuppyGraphStore(
                puppy_wh,
                puppygraph_url="http://puppygraph.container:8081",
                transport=ContainerTransport(puppy_app),
            ),
        }

        for store in stores.values():
            health = await store.health_check()
            assert health.healthy is True

        suite = GoldenQuerySuite(bucket_supernodes=True)
        report = await suite.run_parity(stores)
        report.assert_parity()

        for name in ("neo4j_container", "hugegraph_container", "puppygraph_container"):
            stats = await stores[name].count_stats()
            assert stats.node_count > 0
            assert stats.edge_count > 0

    async def test_http_stream_and_tcp_server_for_all_three_engines(
        self, tmp_path: Path
    ) -> None:
        puppy_wh = tmp_path / "tcp_warehouse"

        class _DummyWriter:
            def __init__(self) -> None:
                self.written = bytearray()

            def write(self, data: bytes) -> None:
                self.written.extend(data)

            async def drain(self) -> None:
                return None

            def close(self) -> None:
                return None

            async def wait_closed(self) -> None:
                return None

        for engine, path in (
            ("neo4j", "/health"),
            ("hugegraph", "/versions"),
            ("puppygraph", "/health"),
        ):
            app = create_app(engine, warehouse_dir=puppy_wh)
            reader = asyncio.StreamReader()
            reader.feed_data(f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
            reader.feed_eof()

            writer = _DummyWriter()
            await _handle_client(app, reader, writer)  # type: ignore[arg-type]
            assert b"HTTP/1.1 200 OK" in writer.written
            assert b'"status": "ok"' in writer.written

        try:
            task = asyncio.create_task(
                serve_app(create_app("neo4j"), "127.0.0.1", 17474)
            )
            await asyncio.sleep(0.05)
            if task.done():
                task.result()
            neo = Neo4jGraphStore(uri="http://127.0.0.1:17474")
            assert (await neo.health_check()).healthy is True
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        except PermissionError:
            pass

    async def test_dockerfiles_and_compose_manifest_exist(self) -> None:
        root = Path(__file__).resolve().parent.parent
        assert (root / "docker" / "neo4j" / "Dockerfile").is_file()
        assert (root / "docker" / "hugegraph" / "Dockerfile").is_file()
        assert (root / "docker" / "puppygraph" / "Dockerfile").is_file()
        compose_text = (root / "docker-compose.yml").read_text(encoding="utf-8")
        assert "scale-kg-neo4j:latest" in compose_text
        assert "scale-kg-hugegraph:latest" in compose_text
        assert "scale-kg-puppygraph:latest" in compose_text

    async def test_live_docker_containers_when_reachable(self) -> None:
        root = Path(__file__).resolve().parent.parent
        shared_wh = root / ".docker-data" / "warehouse"
        neo = Neo4jGraphStore(uri="http://127.0.0.1:7474")
        hg = HugeGraphStore(base_url="http://127.0.0.1:8080")
        pg = IcebergPuppyGraphStore(shared_wh, puppygraph_url="http://127.0.0.1:8081")
        if not (
            (await neo.health_check()).healthy
            and (await hg.health_check()).healthy
            and (await pg.health_check()).healthy
        ):
            pytest.skip("Live Docker containers not reachable in current environment")

        stores: dict[str, GraphStore] = {
            "in_memory": InMemoryGraphStore(),
            "neo4j_docker": neo,
            "hugegraph_docker": hg,
            "puppygraph_docker": pg,
        }
        suite = GoldenQuerySuite(bucket_supernodes=True)
        report = await suite.run_parity(stores)
        report.assert_parity()
