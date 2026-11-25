"""Tests for static Netlify bundle export, history merging, and PR delta summaries."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

import scale_kg.showcase.build as build_mod
from scale_kg.benchmark.generator import ScalePreset
from scale_kg.showcase import build_showcase_site as exported_build_showcase_site
from scale_kg.showcase.build import (
    build_showcase_site,
    extract_trend_entry,
    main,
    merge_history,
    render_pr_delta_markdown,
)
from scale_kg.showcase.snapshot import CommitTrendEntry, ShowcaseSnapshot


@pytest.mark.asyncio
async def test_build_showcase_site_produces_self_contained_static_bundle(
    tmp_path: Path,
) -> None:
    assert exported_build_showcase_site is build_showcase_site
    out_dir = tmp_path / "showcase"
    summary_path = tmp_path / "summary.md"

    snap = await build_showcase_site(
        out_dir=out_dir,
        presets=(ScalePreset.MICRO,),
        git_sha="abc123456789",
        git_ref="main",
        summary_out=summary_path,
        preview_url="https://pr-12--scale-kg.netlify.app",
    )

    assert (out_dir / "index.html").is_file()
    assert (out_dir / "snapshot.json").is_file()
    assert (out_dir / "history.json").is_file()
    for asset in ("styles.css", "app.js", "charts.js", "views.js"):
        assert (out_dir / "assets" / asset).is_file()

    html = (out_dir / "index.html").read_text(encoding="utf-8")
    assert 'data-mode="static"' in html
    assert 'data-mode="live"' not in html
    assert "http://" not in html and "https://" not in html
    assert str(tmp_path) not in html

    raw_snap = (out_dir / "snapshot.json").read_text(encoding="utf-8")
    parsed = ShowcaseSnapshot.model_validate_json(raw_snap)
    assert parsed.introspection.git_sha == "abc123456789"
    assert str(tmp_path) not in raw_snap

    raw_history = (out_dir / "history.json").read_text(encoding="utf-8")
    assert str(tmp_path) not in raw_history
    history = json.loads(raw_history)
    assert len(history) == 1
    assert history[0]["git_sha"] == "abc123456789"

    summary_md = summary_path.read_text(encoding="utf-8")
    assert "https://pr-12--scale-kg.netlify.app" in summary_md
    assert snap.benchmarks["micro"].winner_adapter in summary_md
    assert "(baseline)" in summary_md


def test_merge_history_deduplicates_sha_and_caps_at_100() -> None:
    entries = [
        CommitTrendEntry(
            git_sha=f"sha{i:04d}",
            git_ref="main",
            built_at="2026-11-24T00:00:00+00:00",
            config_fingerprint="abcd1234abcd1234",
            preset="micro",
            hop_1_p95_ms=1.0,
            hop_2_p95_ms=2.0,
            hop_3_p95_ms=3.0,
            peak_qps=100.0,
            winner_adapter="iceberg_puppygraph",
            winner_score=91.0,
            golden_parity_passed=True,
        )
        for i in range(105)
    ]
    merged = merge_history(entries[:-1], entries[-1], max_entries=100)
    assert len(merged) == 100
    assert merged[-1].git_sha == "sha0104"

    # Dedup same git_sha replaces previous record instead of duplicating
    updated = entries[-1].model_copy(update={"peak_qps": 222.0})
    deduped = merge_history(merged, updated, max_entries=100)
    assert len(deduped) == 100
    assert deduped[-1].peak_qps == 222.0

    delta_md = render_pr_delta_markdown(updated, merged[-2])
    assert "Peak QPS" in delta_md
    assert "222.0" in delta_md


def test_merge_history_rejects_non_positive_max_entries() -> None:
    entry = CommitTrendEntry(
        git_sha="sha0001",
        git_ref="main",
        built_at="2026-11-24T00:00:00+00:00",
        config_fingerprint="abcd1234abcd1234",
        preset="micro",
        hop_1_p95_ms=1.0,
        hop_2_p95_ms=2.0,
        hop_3_p95_ms=3.0,
        peak_qps=100.0,
        winner_adapter="iceberg_puppygraph",
        winner_score=91.0,
        golden_parity_passed=True,
    )
    with pytest.raises(ValueError, match="max_entries"):
        merge_history([], entry, max_entries=0)
    with pytest.raises(ValueError, match="max_entries"):
        merge_history([], entry, max_entries=-5)


@pytest.mark.asyncio
async def test_build_showcase_site_rejects_empty_presets(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="presets"):
        await build_showcase_site(out_dir=tmp_path / "out", presets=())


@pytest.mark.asyncio
async def test_build_showcase_site_missing_static_marker_raises_runtime_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_static = tmp_path / "fake_static"
    fake_static.mkdir()
    (fake_static / "index.html").write_text(
        "<html><body></body></html>", encoding="utf-8"
    )
    for asset in ("styles.css", "app.js", "charts.js", "views.js"):
        (fake_static / asset).write_text("", encoding="utf-8")

    monkeypatch.setattr(build_mod, "STATIC_DIR", fake_static)

    with pytest.raises(RuntimeError, match="data-mode"):
        await build_showcase_site(
            out_dir=tmp_path / "out",
            presets=(ScalePreset.MICRO,),
        )


@pytest.mark.asyncio
async def test_build_showcase_site_corrupt_history_falls_back_gracefully(
    tmp_path: Path,
) -> None:
    corrupt_json = tmp_path / "corrupt.json"
    corrupt_json.write_text("{not valid json", encoding="utf-8")

    out1 = tmp_path / "out1"
    await build_showcase_site(
        out_dir=out1,
        presets=(ScalePreset.MICRO,),
        history_in=corrupt_json,
        git_sha="corrupt00001",
        git_ref="main",
    )
    hist1 = json.loads((out1 / "history.json").read_text(encoding="utf-8"))
    assert len(hist1) == 1
    assert hist1[0]["git_sha"] == "corrupt00001"

    invalid_schema = tmp_path / "invalid_schema.json"
    invalid_schema.write_text(
        json.dumps([{"git_sha": "only_one_field"}]), encoding="utf-8"
    )

    out2 = tmp_path / "out2"
    await build_showcase_site(
        out_dir=out2,
        presets=(ScalePreset.MICRO,),
        history_in=invalid_schema,
        git_sha="corrupt00002",
        git_ref="main",
    )
    hist2 = json.loads((out2 / "history.json").read_text(encoding="utf-8"))
    assert len(hist2) == 1
    assert hist2[0]["git_sha"] == "corrupt00002"


@pytest.mark.asyncio
async def test_build_showcase_site_merges_valid_history_and_renders_delta(
    tmp_path: Path,
) -> None:
    history_in = tmp_path / "prev_history.json"
    prev = CommitTrendEntry(
        git_sha="prevsha0001",
        git_ref="main",
        built_at="2026-11-24T00:00:00+00:00",
        config_fingerprint="0000000000000000",
        preset="micro",
        hop_1_p95_ms=1.5,
        hop_2_p95_ms=2.5,
        hop_3_p95_ms=3.5,
        peak_qps=50.0,
        winner_adapter="iceberg_puppygraph",
        winner_score=85.0,
        golden_parity_passed=True,
    )
    history_in.write_text(json.dumps([prev.model_dump(mode="json")]), encoding="utf-8")

    out_dir = tmp_path / "site"
    summary_path = tmp_path / "delta.md"
    snap = await build_showcase_site(
        out_dir=out_dir,
        presets=(ScalePreset.MICRO,),
        history_in=history_in,
        git_sha="currsha0002",
        git_ref="feature/test",
        summary_out=summary_path,
    )

    trend = extract_trend_entry(snap)
    assert trend.git_sha == "currsha0002"
    assert trend.preset == "micro"

    history = json.loads((out_dir / "history.json").read_text(encoding="utf-8"))
    assert [h["git_sha"] for h in history] == ["prevsha0001", "currsha0002"]

    delta_md = summary_path.read_text(encoding="utf-8")
    assert "(baseline)" not in delta_md
    assert "(changed)" in delta_md


def test_cli_main_and_module_invocation(tmp_path: Path) -> None:
    out_main = tmp_path / "cli_main"
    main(
        [
            "--presets",
            "micro",
            "--out",
            str(out_main),
            "--git-sha",
            "climain0001",
            "--git-ref",
            "main",
        ]
    )
    assert (out_main / "index.html").is_file()
    assert (out_main / "snapshot.json").is_file()
    assert (out_main / "history.json").is_file()

    out_proc = tmp_path / "cli_proc"
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "scale_kg.showcase.build",
            "--presets",
            "micro",
            "--out",
            str(out_proc),
            "--git-sha",
            "cliproc0002",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert (out_proc / "index.html").is_file()
    assert (out_proc / "snapshot.json").is_file()
    hist = json.loads((out_proc / "history.json").read_text(encoding="utf-8"))
    assert hist[0]["git_sha"] == "cliproc0002"
