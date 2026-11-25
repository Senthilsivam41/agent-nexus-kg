"""Static showcase exporter CLI, rolling history merger, and PR delta renderer."""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
from collections.abc import Sequence
from pathlib import Path

from pydantic import ValidationError

from scale_kg.benchmark.generator import ScalePreset
from scale_kg.showcase.router import STATIC_DIR
from scale_kg.showcase.scenarios import build_showcase_snapshot
from scale_kg.showcase.snapshot import CommitTrendEntry, ShowcaseSnapshot

_STATIC_MODE_MARKER = 'data-mode="live"'
_STATIC_MODE_TARGET = 'data-mode="static"'
_ASSET_FILENAMES: tuple[str, ...] = ("styles.css", "app.js", "charts.js", "views.js")


def extract_trend_entry(
    snapshot: ShowcaseSnapshot,
    *,
    preset: str | None = None,
) -> CommitTrendEntry:
    """Extract a compact historical metric entry from a ShowcaseSnapshot."""
    chosen_key = preset or (
        "local" if "local" in snapshot.benchmarks else snapshot.default_preset
    )
    bundle = snapshot.benchmarks[chosen_key]
    winner_name = bundle.winner_adapter
    adapter_res = bundle.suite_result.adapters[winner_name]
    winner_card = next(c for c in bundle.scorecards if c.adapter_name == winner_name)
    by_depth = adapter_res.traversal_latency.by_depth
    parity_ok = (
        bundle.suite_result.golden_parity.passed
        if bundle.suite_result.golden_parity is not None
        else adapter_res.golden_parity_passed
    )
    return CommitTrendEntry(
        git_sha=snapshot.introspection.git_sha,
        git_ref=snapshot.introspection.git_ref,
        built_at=snapshot.introspection.built_at,
        config_fingerprint=snapshot.introspection.config_fingerprint,
        preset=chosen_key,
        hop_1_p95_ms=by_depth[1].p95_ms if 1 in by_depth else 0.0,
        hop_2_p95_ms=by_depth[2].p95_ms if 2 in by_depth else 0.0,
        hop_3_p95_ms=by_depth[3].p95_ms if 3 in by_depth else 0.0,
        peak_qps=adapter_res.concurrent_ramp.peak_qps,
        winner_adapter=winner_name,
        winner_score=winner_card.weighted_total,
        golden_parity_passed=parity_ok,
    )


def merge_history(
    existing: Sequence[CommitTrendEntry],
    new_entry: CommitTrendEntry,
    *,
    max_entries: int = 100,
) -> list[CommitTrendEntry]:
    """Append ``new_entry``, deduplicating by ``git_sha`` and capping length."""
    if max_entries < 1:
        raise ValueError("max_entries must be >= 1.")
    filtered = [e for e in existing if e.git_sha != new_entry.git_sha]
    filtered.append(new_entry)
    return filtered[-max_entries:]


def render_pr_delta_markdown(
    current: CommitTrendEntry,
    previous: CommitTrendEntry | None,
    *,
    preview_url: str | None = None,
) -> str:
    """Render a Markdown benchmark delta table for GitHub Actions job summary."""

    def _fmt_delta(curr: float, prev: float | None, unit: str = "") -> str:
        if prev is None:
            return f"{curr:.2f}{unit} (baseline)"
        diff = curr - prev
        sign = "+" if diff >= 0 else ""
        return f"{curr:.2f}{unit} ({sign}{diff:.2f}{unit})"

    lines = [
        "## Scale-KG Showcase Benchmark Delta",
        "",
        f"- **Commit:** `{current.git_sha}` (`{current.git_ref}`)",
        (
            f"- **Preset:** `{current.preset}` | **Gate 2 Winner:** "
            f"`{current.winner_adapter}` ({current.winner_score:.2f})"
        ),
        f"- **Config Fingerprint:** `{current.config_fingerprint}`"
        + (
            " (changed)"
            if previous is not None
            and previous.config_fingerprint != current.config_fingerprint
            else ""
        ),
    ]
    if preview_url:
        lines.append(f"- **Live Netlify Preview:** {preview_url}")

    lines.extend(
        [
            "",
            "| Metric | Value (Delta vs Previous) |",
            "| :--- | :--- |",
            (
                "| 1-Hop P95 | "
                f"{_fmt_delta(current.hop_1_p95_ms, previous.hop_1_p95_ms if previous else None, ' ms')} |"
            ),
            (
                "| 2-Hop P95 | "
                f"{_fmt_delta(current.hop_2_p95_ms, previous.hop_2_p95_ms if previous else None, ' ms')} |"
            ),
            (
                "| 3-Hop P95 | "
                f"{_fmt_delta(current.hop_3_p95_ms, previous.hop_3_p95_ms if previous else None, ' ms')} |"
            ),
            (
                "| Peak QPS | "
                f"{_fmt_delta(current.peak_qps, previous.peak_qps if previous else None)} |"
            ),
            (
                "| Gate 2 Winner Score | "
                f"{_fmt_delta(current.winner_score, previous.winner_score if previous else None)} |"
            ),
            f"| Golden Query Parity | {'PASS' if current.golden_parity_passed else 'FAIL'} |",
            "",
        ]
    )
    return "\n".join(lines)


def _render_static_index_html() -> str:
    """Load the dashboard template and rewrite its mode attribute to static."""
    index_html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    if _STATIC_MODE_MARKER not in index_html:
        raise RuntimeError(
            f"Showcase index.html template is missing {_STATIC_MODE_MARKER!r} marker."
        )
    static_html = index_html.replace(_STATIC_MODE_MARKER, _STATIC_MODE_TARGET, 1)
    if _STATIC_MODE_TARGET not in static_html:
        raise RuntimeError(
            f"Failed to inject {_STATIC_MODE_TARGET!r} into showcase index.html."
        )
    return static_html


def _load_prior_history(history_in: Path | None) -> list[CommitTrendEntry]:
    """Load prior CommitTrendEntry records, skipping individually invalid entries."""
    if history_in is None or not history_in.is_file():
        return []
    try:
        raw_hist = json.loads(history_in.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if not isinstance(raw_hist, list):
        return []
    entries: list[CommitTrendEntry] = []
    for item in raw_hist:
        try:
            entries.append(CommitTrendEntry.model_validate(item))
        except ValidationError:
            continue
    return entries


async def build_showcase_site(
    *,
    out_dir: Path,
    presets: Sequence[ScalePreset] = (ScalePreset.MICRO, ScalePreset.LOCAL),
    history_in: Path | None = None,
    git_sha: str | None = None,
    git_ref: str | None = None,
    summary_out: Path | None = None,
    preview_url: str | None = None,
) -> ShowcaseSnapshot:
    """Build a self-contained static showcase bundle suitable for Netlify deploy."""
    if not presets:
        raise ValueError("presets must not be empty.")

    static_html = _render_static_index_html()
    snapshot = await build_showcase_snapshot(
        presets=presets,
        git_sha=git_sha,
        git_ref=git_ref,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    assets_dir = out_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "index.html").write_text(static_html, encoding="utf-8")
    for filename in _ASSET_FILENAMES:
        shutil.copy2(STATIC_DIR / filename, assets_dir / filename)

    (out_dir / "snapshot.json").write_text(
        snapshot.model_dump_json(indent=2),
        encoding="utf-8",
    )

    prior_entries = _load_prior_history(history_in)
    current_entry = extract_trend_entry(snapshot)
    previous_entry = (
        next(
            (e for e in reversed(prior_entries) if e.git_sha != current_entry.git_sha),
            None,
        )
        if prior_entries
        else None
    )
    merged = merge_history(prior_entries, current_entry, max_entries=100)
    (out_dir / "history.json").write_text(
        json.dumps([e.model_dump(mode="json") for e in merged], indent=2),
        encoding="utf-8",
    )

    if summary_out is not None:
        summary_out.parent.mkdir(parents=True, exist_ok=True)
        summary_out.write_text(
            render_pr_delta_markdown(
                current_entry,
                previous_entry,
                preview_url=preview_url,
            ),
            encoding="utf-8",
        )

    return snapshot


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entrypoint for building the static showcase site."""
    parser = argparse.ArgumentParser(
        description="Build static Scale-KG showcase dashboard & benchmark snapshot."
    )
    parser.add_argument("--presets", default="micro,local")
    parser.add_argument("--out", default="dist/showcase")
    parser.add_argument("--history-in", default=None)
    parser.add_argument("--git-sha", default=None)
    parser.add_argument("--git-ref", default=None)
    parser.add_argument("--summary-out", default=None)
    parser.add_argument("--preview-url", default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)

    preset_enums = tuple(
        ScalePreset(p.strip()) for p in args.presets.split(",") if p.strip()
    )
    asyncio.run(
        build_showcase_site(
            out_dir=Path(args.out),
            presets=preset_enums,
            history_in=Path(args.history_in) if args.history_in else None,
            git_sha=args.git_sha,
            git_ref=args.git_ref,
            summary_out=Path(args.summary_out) if args.summary_out else None,
            preview_url=args.preview_url,
        )
    )


if __name__ == "__main__":
    main()
