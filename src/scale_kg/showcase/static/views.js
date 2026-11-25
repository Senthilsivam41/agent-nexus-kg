// 5 self-explanatory dashboard tab renderers (strict CSP, zero external URLs, XSS-escaped).

import {
  escapeHtml,
  renderLatencyBarChart,
  renderLineChart,
  renderMeterBar,
  renderSparkline,
  renderWaterfallChart,
  safeNum,
} from './charts.js';

const AVAILABILITY_META = {
  verified_local: { label: 'Verified Local', cls: 'pill-ok' },
  live_remote: { label: 'Live Remote', cls: 'pill-live' },
  offline_remote: { label: 'Offline', cls: 'pill-warn' },
  contract_stub: { label: 'Contract Stub', cls: 'pill-stub' },
};

const SCALE_EDGE_COUNTS = {
  '100M': 100_000_000,
  '1B': 1_000_000_000,
  '10B': 10_000_000_000,
};

function fmtMs(val) {
  return `${safeNum(val, 0).toFixed(2)} ms`;
}

function fmtNum(val, digits = 0) {
  return safeNum(val, 0).toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function fmtUsd(val) {
  const n = safeNum(val, 0);
  if (n <= 0) {
    return '$0 / mo';
  }
  return `$${n.toLocaleString('en-US', { maximumFractionDigits: 0 })} / mo`;
}

function fmtBytes(bytes) {
  const b = safeNum(bytes, 0);
  if (b >= 1_048_576) {
    return `${(b / 1_048_576).toFixed(2)} MB`;
  }
  if (b >= 1024) {
    return `${(b / 1024).toFixed(1)} KB`;
  }
  return `${b} B`;
}

function formatTimestamp(iso) {
  if (!iso) {
    return '—';
  }
  return String(iso).replace('T', ' ').slice(0, 19) + 'Z';
}

function getActiveBundle(snapshot, state) {
  const benchmarks = snapshot?.benchmarks || {};
  const preset = state.activePreset || snapshot?.default_preset || 'micro';
  if (benchmarks[preset]) {
    return { preset, bundle: benchmarks[preset] };
  }
  const firstKey = Object.keys(benchmarks)[0] || 'micro';
  return { preset: firstKey, bundle: benchmarks[firstKey] || null };
}

function budgetsToMap(introspection) {
  const map = {};
  for (const b of introspection?.latency_budgets || []) {
    map[b.name] = safeNum(b.value_ms, 0);
  }
  return map;
}

function availabilityPill(availability) {
  const meta = AVAILABILITY_META[availability] || {
    label: availability || 'Unknown',
    cls: 'pill-stub',
  };
  return `<span class="pill ${meta.cls}">${escapeHtml(meta.label)}</span>`;
}

function buildLayerDefinitions(introspection) {
  const guardrails = introspection?.guardrails || [];
  const guardrailSummary = guardrails
    .map((g) => `${g.name}=${g.value} ${g.unit}`.trim())
    .join(' · ');
  const domains = (introspection?.domains || []).join(', ');
  const verifiedCount = (introspection?.adapters || []).filter(
    (a) => a.availability === 'verified_local' || a.availability === 'live_remote'
  ).length;

  return [
    {
      id: 'layer-1',
      index: 'Layer 1',
      name: 'Hybrid GraphRAG & Query API',
      badge: `${(introspection?.api_routes || []).length} routes`,
      badgeCls: 'pill-live',
      summary:
        'FastAPI + gRPC surface orchestrating 5-stage Seed-Then-Expand Vector + KG + RRF retrieval and selective top-N hydration.',
      modules: [
        'scale_kg.api.app',
        'scale_kg.routing.hybrid_rag.HybridGraphRAGRouter',
        'scale_kg.showcase.router',
      ],
      howItHandles:
        'Accepts bounded 1-to-3 hop traversal and hybrid retrieval requests, runs vector seed discovery before bounded graph expansion, and returns grounded citations.',
      howItSwitches:
        'Defers heavy document reads until post-RRF top-N selection, switching from graph-only structural IDs to partition-pruned Parquet batch hydration.',
      limits: [
        'Seed-Then-Expand RRF fusion (k=60)',
        `Domains indexed: ${domains || 'customer, knowledge, technology, product'}`,
      ],
      routes: introspection?.api_routes || [],
    },
    {
      id: 'layer-2',
      index: 'Layer 2',
      name: 'Routing, Sharding & Guardrails',
      badge: `${introspection?.num_buckets ?? 16}-way buckets`,
      badgeCls: 'pill-ok',
      summary:
        'Enforces Lean Graph write discipline, Hop-0 O(1) prefix routing, Ghost/Proxy federation, 16-way supernode bucketing, and hot-path promotion.',
      modules: [
        'scale_kg.core.guardrails.GuardrailEnforcer',
        'scale_kg.core.supernode.SupernodeBucketer',
        'scale_kg.routing.directory.EntityDirectory',
        'scale_kg.routing.coordinator.QueryCoordinator',
      ],
      howItHandles:
        'Rejects heavy text blobs on write (HTTP 422), bounds frontier expansion before OOM (aborted=True), and routes canonical <domain>:<type>:<slug> IDs in O(1).',
      howItSwitches:
        `Flips direct hub edges to ${introspection?.num_buckets ?? 16} CRC32 buckets at degree >= ${introspection?.supernode_degree_threshold ?? 1000}, and promotes derived edges to materialized hot paths after ${introspection?.hot_path_threshold ?? 3} hits.`,
      limits: [guardrailSummary || 'max_depth=3 · max_results=5000 · max_expanded_nodes=50000 · max_execution_seconds=2.0'],
      routes: [],
    },
    {
      id: 'layer-3',
      index: 'Layer 3',
      name: 'Pluggable GraphStore Adapters',
      badge: `${verifiedCount}/${(introspection?.adapters || []).length} live/local`,
      badgeCls: 'pill-accent',
      summary:
        'Engine-agnostic async GraphStore protocol verified by 7 frozen Golden Queries for zero-code-change backend switching.',
      modules: [
        'scale_kg.adapters.base.GraphStore',
        'scale_kg.adapters.in_memory.InMemoryGraphStore',
        'scale_kg.adapters.iceberg_puppygraph.IcebergPuppyGraphStore',
        'scale_kg.adapters.hugegraph.HugeGraphStore',
        'scale_kg.adapters.neo4j.Neo4jGraphStore',
      ],
      howItHandles:
        'Executes identical 0-1 BFS bucket-unwrapping traversal semantics across in-memory, Zero-ETL Iceberg/Parquet, and remote distributed engines.',
      howItSwitches:
        'Switches backend purely via config (`SCALE_KG_BACKEND`) guided by empirical Gate 2 weighted scorecards across 100M → 1B → 10B scale ladders.',
      limits: [
        `${(introspection?.golden_queries || []).length} frozen Golden Queries (GQ-01..GQ-07) enforce 100% node/edge/path parity`,
      ],
      routes: [],
    },
    {
      id: 'layer-4',
      index: 'Layer 4',
      name: 'Parquet / Iceberg Lakehouse Tier',
      badge: 'Zero-ETL',
      badgeCls: 'pill-ok',
      summary:
        'Stores heavy document bodies and columnar graph tables in domain-partitioned Parquet/Iceberg storage referenced by lakehouse_ref URIs.',
      modules: [
        'scale_kg.lakehouse.store.LakehouseContentStore',
        'scale_kg.adapters.iceberg_puppygraph.IcebergPuppyGraphStore',
      ],
      howItHandles:
        'Keeps the online graph lean (routing metadata only) while serving partition-pruned batch hydration (`hydrate_batch`) for final top-N hits.',
      howItSwitches:
        'Eliminates dual-write ETL pipelines by letting Zero-ETL query engines scan Iceberg/Parquet tables directly when scaling from 100M to 10B+ edges.',
      limits: [
        'Lean Graph Rule: Document/Article/Chunk/Transcript nodes require lakehouse_ref URI',
      ],
      routes: [],
    },
  ];
}

/**
 * TAB 1: System Map — Interactive 4-layer architecture + live introspected switch rules.
 */
export function renderSystemMap(snapshot, state) {
  const intro = snapshot.introspection || {};
  const layers = buildLayerDefinitions(intro);
  const selectedId = state.selectedLayer || 'layer-2';
  const activeLayer = layers.find((l) => l.id === selectedId) || layers[0];

  const verifiedAdapters = (intro.adapters || []).filter(
    (a) => a.availability === 'verified_local' || a.availability === 'live_remote'
  ).length;
  const totalAdapters = (intro.adapters || []).length;

  const kpis = `
    <section class="kpi-grid" aria-label="Live introspected system thresholds">
      <div class="kpi-card">
        <div class="kpi-label">Supernode Switch</div>
        <div class="kpi-value tone-accent">&ge; ${escapeHtml(fmtNum(intro.supernode_degree_threshold))}</div>
        <div class="kpi-note">${escapeHtml(intro.num_buckets)}-way CRC32 bucketing</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Hot-Path Promotion</div>
        <div class="kpi-value tone-ok">${escapeHtml(intro.hot_path_threshold)} hits</div>
        <div class="kpi-note">Derived &rarr; materialized edge</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Semantic Domains</div>
        <div class="kpi-value">${escapeHtml((intro.domains || []).length)}</div>
        <div class="kpi-note">${escapeHtml((intro.domains || []).join(' · '))}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">GraphStore Adapters</div>
        <div class="kpi-value">${escapeHtml(verifiedAdapters)} / ${escapeHtml(totalAdapters)}</div>
        <div class="kpi-note">Verified local + remote/stubs</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Golden Parity Suite</div>
        <div class="kpi-value tone-ok">${escapeHtml((intro.golden_queries || []).length)} GQs</div>
        <div class="kpi-note">Frozen cross-engine invariants</div>
      </div>
    </section>
  `;

  const layerCardsHtml = layers
    .map((layer) => {
      const isSelected = layer.id === activeLayer.id;
      return `
        <button
          type="button"
          class="layer-card ${isSelected ? 'is-selected' : ''}"
          data-action="select-layer"
          data-layer="${escapeHtml(layer.id)}"
          aria-pressed="${isSelected ? 'true' : 'false'}"
        >
          <div class="layer-card-top">
            <span class="layer-index">${escapeHtml(layer.index)}</span>
            <span class="pill ${layer.badgeCls}">${escapeHtml(layer.badge)}</span>
          </div>
          <div class="layer-name">${escapeHtml(layer.name)}</div>
          <p class="layer-desc">${escapeHtml(layer.summary)}</p>
        </button>
      `;
    })
    .join('');

  const drawerHtml = `
    <aside class="layer-drawer" aria-label="Selected architecture layer details">
      <div class="layer-card-top">
        <span class="layer-index">${escapeHtml(activeLayer.index)} Inspector</span>
        <span class="pill ${activeLayer.badgeCls}">${escapeHtml(activeLayer.badge)}</span>
      </div>
      <h3 class="panel-title">${escapeHtml(activeLayer.name)}</h3>
      <p class="panel-subtitle">${escapeHtml(activeLayer.summary)}</p>

      <div class="drawer-section">
        <div class="drawer-label">How It Handles Load</div>
        <div>${escapeHtml(activeLayer.howItHandles)}</div>
      </div>

      <div class="drawer-section">
        <div class="drawer-label">How It Switches Under Pressure</div>
        <div>${escapeHtml(activeLayer.howItSwitches)}</div>
      </div>

      <div class="drawer-section">
        <div class="drawer-label">Active Code Modules</div>
        <div class="chip-list">
          ${activeLayer.modules.map((m) => `<span class="code-chip">${escapeHtml(m)}</span>`).join('')}
        </div>
      </div>

      <div class="drawer-section">
        <div class="drawer-label">Introspected Limits &amp; Guarantees</div>
        <div class="chip-list">
          ${activeLayer.limits.map((lim) => `<span class="meta-chip">${escapeHtml(lim)}</span>`).join('')}
        </div>
      </div>

      ${
        activeLayer.routes.length > 0
          ? `<div class="drawer-section">
              <div class="drawer-label">Public API Surface (${escapeHtml(activeLayer.routes.length)})</div>
              <div class="chip-list">
                ${activeLayer.routes.map((r) => `<span class="code-chip">${escapeHtml(r)}</span>`).join('')}
              </div>
            </div>`
          : ''
      }
    </aside>
  `;

  const switchRulesHtml = (intro.switch_rules || [])
    .map(
      (rule) => `
      <article class="rule-card">
        <div class="rule-head">
          <h3 class="rule-title">${escapeHtml(rule.title)}</h3>
          <span class="pill pill-live">${escapeHtml(rule.layer)}</span>
        </div>
        <div class="rule-module">${escapeHtml(rule.module)}</div>
        <div class="rule-threshold">TRIGGER: ${escapeHtml(rule.threshold)}</div>
        <div class="rule-flow">
          <div class="rule-step"><span class="rule-step-tag">Baseline</span>${escapeHtml(rule.from_behavior)}</div>
          <div class="rule-step rule-step-to"><span class="rule-step-tag">Switched</span>${escapeHtml(rule.to_behavior)}</div>
        </div>
      </article>
    `
    )
    .join('');

  const guardrailsRows = (intro.guardrails || [])
    .map(
      (g) => `
      <tr>
        <td class="mono">${escapeHtml(g.name)}</td>
        <td class="num align-right">${escapeHtml(g.value)} ${escapeHtml(g.unit)}</td>
        <td class="muted">${escapeHtml(g.description)}</td>
      </tr>
    `
    )
    .join('');

  const budgetRows = (intro.latency_budgets || [])
    .map(
      (b) => `
      <tr>
        <td class="mono">${escapeHtml(b.name)}</td>
        <td class="num align-right text-ok">${escapeHtml(fmtMs(b.value_ms))}</td>
        <td class="muted">${escapeHtml(b.description)}</td>
      </tr>
    `
    )
    .join('');

  return `
    ${kpis}

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">4-Layer Scale Architecture Map</h2>
          <p class="panel-subtitle">Click any layer to inspect its live Python modules, active config bounds, and runtime switching behavior.</p>
        </div>
        <span class="pill pill-ok">Zero-Drift Introspection v${escapeHtml(intro.package_version || '0.1.0')}</span>
      </div>
      <div class="arch-grid">
        <div class="layer-stack">${layerCardsHtml}</div>
        ${drawerHtml}
      </div>
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Runtime Switch Thresholds &amp; Adaptation Rules</h2>
          <p class="panel-subtitle">Exact thresholds read from live <code>ScaleKGConfig</code> proving how the graph adapts automatically under load.</p>
        </div>
      </div>
      <div class="rule-grid">${switchRulesHtml}</div>
    </section>

    <div class="grid-2">
      <section class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Active Query &amp; Write Guardrails</h2>
            <p class="panel-subtitle">Hard caps enforced pre-flight and at runtime to eliminate runaway traversals.</p>
          </div>
        </div>
        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr><th>Parameter</th><th class="align-right">Active Cap</th><th>Guarantee</th></tr>
            </thead>
            <tbody>${guardrailsRows}</tbody>
          </table>
        </div>
      </section>

      <section class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Gate 2 Latency SLA Budgets</h2>
            <p class="panel-subtitle">P95 latency targets evaluated across every GraphStore adapter.</p>
          </div>
        </div>
        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr><th>Budget</th><th class="align-right">SLA Limit</th><th>Scope</th></tr>
            </thead>
            <tbody>${budgetRows}</tbody>
          </table>
        </div>
      </section>
    </div>
  `;
}

/**
 * TAB 2: Load Escalator — Supernode bucketing, hot-path promotion, federation & guardrails.
 */
export function renderLoadEscalator(snapshot, state) {
  const intro = snapshot.introspection || {};
  const scenarios = snapshot.scenarios || {};
  const supernode = scenarios.supernode_ramp || {};
  const hotPath = state.liveHotPath || scenarios.hot_path || {};
  const federation = scenarios.federation || {};
  const probes = {
    ...(scenarios.guardrail_probes || {}),
    ...(state.liveGuardrails || {}),
  };

  const { preset: activePreset, bundle: activeBundle } = getActiveBundle(snapshot, state);
  const availablePresets = ['micro', 'local'];

  // Supernode degree ramp chart + table
  const snPoints = (supernode.points || []).map((pt) => ({
    ...pt,
    label: `d=${pt.degree}`,
  }));
  const snChart = renderLineChart(snPoints, {
    xKey: 'degree',
    yKey: 'traversal_ms',
    thresholdX: supernode.switch_triggered_at_degree ?? supernode.threshold ?? 32,
    thresholdLabel: `${supernode.num_buckets ?? 16}-way bucketing activates (deg ≥ ${supernode.threshold ?? 32})`,
    title: 'Supernode Traversal Latency vs Hub Degree',
    yUnit: 'ms',
    xLabel: 'Hub Degree (direct edges before bucketing)',
    color: '#38bdf8',
  });

  const snRows = (supernode.points || [])
    .map((pt) => {
      const isSwitch = pt.degree === supernode.switch_triggered_at_degree;
      return `
        <tr class="${isSwitch ? 'row-highlight' : ''}">
          <td class="num">${escapeHtml(pt.degree)}${isSwitch ? ' <span class="pill pill-ok">SWITCH</span>' : ''}</td>
          <td>${pt.bucketed ? '<span class="pill pill-ok">Bucketed</span>' : '<span class="pill pill-stub">Direct</span>'}</td>
          <td class="num align-right">${escapeHtml(pt.bucket_count)}</td>
          <td class="num align-right">${escapeHtml(fmtMs(pt.traversal_ms))}</td>
          <td class="num align-right">${escapeHtml(pt.expanded_nodes)}</td>
          <td class="num align-right">${escapeHtml(pt.bucket_hops_unwrapped)}</td>
          <td class="num align-right ${pt.raw_bucket_leaks === 0 ? 'text-ok' : 'text-danger'}">${escapeHtml(pt.raw_bucket_leaks)}</td>
          <td class="num align-right">${escapeHtml(pt.logical_neighbors_returned)}</td>
        </tr>
      `;
    })
    .join('');

  // Hot-path stepper
  const hpSteps = hotPath.steps || [];
  const hpCursor = Math.min(
    Math.max(1, safeNum(state.hotPathStep, hpSteps.length || 1)),
    Math.max(1, hpSteps.length)
  );
  const currentStep = hpSteps[hpCursor - 1] || null;
  const stepperCards = hpSteps
    .map((step) => {
      const reached = step.hit_number <= hpCursor;
      const isCurr = step.hit_number === hpCursor;
      const isPromoted = reached && step.materialized;
      const cls = [
        'step-card',
        reached ? 'is-reached' : '',
        isCurr ? 'is-current' : '',
        isPromoted ? 'is-promoted' : '',
      ]
        .filter(Boolean)
        .join(' ');
      return `
        <div class="${cls}">
          <div class="layer-card-top">
            <span class="mono">Hit #${escapeHtml(step.hit_number)}</span>
            ${
              step.materialized
                ? '<span class="pill pill-ok">Materialized</span>'
                : '<span class="pill pill-warn">On-the-fly</span>'
            }
          </div>
          <div class="kpi-value">${escapeHtml(fmtMs(step.execution_ms))}</div>
          <div class="kpi-note">Derived edges: ${escapeHtml(step.derived_edges_returned)} · Promoted: ${escapeHtml(step.promoted_hot_paths)}</div>
        </div>
      `;
    })
    .join('');

  // Federation locality chart
  const fedPoints = (federation.points || []).map((pt) => ({
    ...pt,
    sharePct: Math.round(safeNum(pt.cross_domain_share, 0) * 100),
    localityPct: Number((safeNum(pt.single_domain_locality_ratio, 0) * 100).toFixed(1)),
    label: `${Math.round(safeNum(pt.cross_domain_share, 0) * 100)}%`,
  }));
  const fedChart = renderLineChart(fedPoints, {
    xKey: 'sharePct',
    yKey: 'localityPct',
    title: 'Single-Domain Locality Ratio vs Cross-Domain Edge Share',
    yUnit: '% local',
    xLabel: 'Cross-Domain Edge Share (%)',
    color: '#818cf8',
  });
  const fedRows = (federation.points || [])
    .map(
      (pt) => `
      <tr>
        <td class="num">${escapeHtml(Math.round(safeNum(pt.cross_domain_share, 0) * 100))}%</td>
        <td class="num align-right">${escapeHtml(pt.ghost_proxies_created)}</td>
        <td class="num align-right">${escapeHtml(pt.cross_domain_hops)}</td>
        <td class="num align-right text-ok">${escapeHtml((safeNum(pt.single_domain_locality_ratio, 0) * 100).toFixed(1))}%</td>
        <td class="num align-right">${escapeHtml(fmtMs(pt.traversal_ms))}</td>
      </tr>
    `
    )
    .join('');

  // Guardrail probes
  const probeOrder = [
    {
      id: 'forbidden_payload',
      label: 'Lean Graph Blob Write',
      sub: 'Inject heavy content key → HTTP 422',
    },
    {
      id: 'missing_lakehouse_ref',
      label: 'Missing Lakehouse Pointer',
      sub: 'Document without URI → HTTP 422',
    },
    {
      id: 'preflight_depth_cap',
      label: 'Pre-Flight Depth=4 Block',
      sub: 'Exceeds max_depth=3 → HTTP 422',
    },
    {
      id: 'runtime_expansion_abort',
      label: 'Runtime Expansion Abort',
      sub: 'Tight cap → partial aborted=True',
    },
  ];
  const selectedProbeId = state.selectedProbe || 'forbidden_payload';
  const activeProbe = probes[selectedProbeId] || null;

  const probeButtonsHtml = probeOrder
    .map((p) => {
      const result = probes[p.id];
      const isSelected = p.id === selectedProbeId;
      const badge = result
        ? result.blocked
          ? '<span class="pill pill-danger">HTTP 422 Blocked</span>'
          : '<span class="pill pill-warn">aborted=True</span>'
        : '<span class="pill pill-stub">Ready</span>';
      return `
        <button
          type="button"
          class="btn probe-btn ${isSelected ? 'is-selected' : ''}"
          data-action="run-guardrail"
          data-probe="${escapeHtml(p.id)}"
          ${state.busyAction === 'guardrail' ? 'disabled' : ''}
        >
          <span class="layer-card-top">
            <span class="probe-btn-title">${escapeHtml(p.label)}</span>
            ${badge}
          </span>
          <span class="probe-btn-meta">${escapeHtml(p.sub)}</span>
        </button>
      `;
    })
    .join('');

  const probePayloadHtml = activeProbe
    ? `
      <div class="payload-box">
        <div class="layer-card-top">
          <strong>${escapeHtml(activeProbe.title)}</strong>
          <div class="chip-list">
            <span class="pill ${activeProbe.blocked ? 'pill-danger' : 'pill-warn'}">HTTP ${escapeHtml(activeProbe.http_status)}</span>
            <span class="pill ${activeProbe.blocked ? 'pill-danger' : 'pill-ok'}">blocked=${escapeHtml(String(activeProbe.blocked))}</span>
            <span class="pill ${activeProbe.aborted ? 'pill-warn' : 'pill-stub'}">aborted=${escapeHtml(String(activeProbe.aborted))}</span>
          </div>
        </div>
        <p class="panel-subtitle">${escapeHtml(activeProbe.summary)}</p>
        <pre class="payload-pre">${escapeHtml(JSON.stringify(activeProbe, null, 2))}</pre>
      </div>
    `
    : '<p class="muted">Select a guardrail probe to inspect its live response payload.</p>';

  // Scale Preset overview & Coalesced Ramp status
  const presetInfo = (intro.presets || []).find((p) => p.preset === activePreset);
  const rampMeta = state.lastRampMeta
    ? `<span class="pill pill-live">${state.lastRampMeta.cache_hit ? 'TTL Cache Hit' : state.lastRampMeta.coalesced ? 'Coalesced Single-Flight' : 'Fresh Run'}</span>`
    : '';
  const activeAdapterCount = Object.keys(activeBundle?.suite_result?.adapters || {}).length;

  return `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Scale Preset Ladder &amp; Coalesced Benchmark Ramp</h2>
          <p class="panel-subtitle">Switch empirical dataset presets or trigger a live single-flight coalesced 7-workload ramp.</p>
        </div>
        <div class="control-bar">
          <div class="segmented" role="group" aria-label="Scale preset">
            ${availablePresets
              .map(
                (p) => `
              <button
                type="button"
                class="seg-btn ${activePreset === p ? 'is-active' : ''}"
                data-action="select-preset"
                data-preset="${escapeHtml(p)}"
                ${state.busyAction === 'ramp' ? 'disabled' : ''}
              >${escapeHtml(p.toUpperCase())}</button>
            `
              )
              .join('')}
          </div>
          <label class="control-group" for="ramp-seed-input">
            <span class="control-label">Seed</span>
            <input id="ramp-seed-input" class="number-input" type="number" min="1" max="10000" value="${escapeHtml(state.rampSeed ?? 42)}" />
          </label>
          <button
            type="button"
            class="btn btn-primary"
            data-action="run-ramp"
            ${state.busyAction === 'ramp' ? 'disabled' : ''}
          >${state.busyAction === 'ramp' ? 'Running Ramp…' : state.mode === 'live' ? 'Run Coalesced Ramp' : 'Replay Preset Snapshot'}</button>
          ${rampMeta}
        </div>
      </div>

      <div class="kpi-grid">
        <div class="kpi-card">
          <div class="kpi-label">Active Preset</div>
          <div class="kpi-value tone-accent">${escapeHtml(activePreset.toUpperCase())}</div>
          <div class="kpi-note">${escapeHtml(presetInfo ? presetInfo.scale_label : 'Benchmark dataset')}</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Synthetic Nodes / Supernode Fanout</div>
          <div class="kpi-value">${escapeHtml(presetInfo ? fmtNum(presetInfo.node_count) : '—')}</div>
          <div class="kpi-note">Hub fanout: ${escapeHtml(presetInfo ? fmtNum(presetInfo.supernode_fanout) : '—')} · Avg deg: ${escapeHtml(presetInfo ? presetInfo.avg_degree : '—')}</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Evaluated Adapters</div>
          <div class="kpi-value">${escapeHtml(activeAdapterCount)}</div>
          <div class="kpi-note">Winner: ${escapeHtml(activeBundle?.winner_adapter || '—')}</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Concurrency Levels</div>
          <div class="kpi-value">${escapeHtml(presetInfo ? presetInfo.concurrency_levels.join(' / ') : '—')}</div>
          <div class="kpi-note">Parallel query ramp tiers</div>
        </div>
      </div>
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">1. Supernode Degree Ramp (16-Way Bucketing Switch)</h2>
          <p class="panel-subtitle">Hub <code>${escapeHtml(supernode.hub_id || 'technology:hub:demo-core')}</code> crossing degree threshold ${escapeHtml(supernode.threshold ?? 32)} with 0-1 BFS transparent bucket unwrapping.</p>
        </div>
        <span class="pill pill-ok">0 Raw Bucket Leaks</span>
      </div>

      <div class="callout callout-ok">
        <strong>How it switches:</strong> Below degree ${escapeHtml(supernode.threshold ?? 32)}, edges attach directly to the hub. Once hub degree reaches <strong>${escapeHtml(supernode.switch_triggered_at_degree ?? supernode.threshold ?? 32)}</strong>, <code>SupernodeBucketer</code> deterministically shards edges across <strong>${escapeHtml(supernode.num_buckets ?? 16)}</strong> virtual <code>::bucket::0..15</code> nodes and unwraps them transparently so callers see exact logical neighbors with <strong>0 leaks</strong>.
      </div>

      <div class="chart-box">${snChart}</div>

      <div class="table-wrap mt-md">
        <table class="data-table">
          <thead>
            <tr>
              <th>Hub Degree</th>
              <th>Mode</th>
              <th class="align-right">Buckets</th>
              <th class="align-right">Latency</th>
              <th class="align-right">Expanded Nodes</th>
              <th class="align-right">Bucket Hops Unwrapped</th>
              <th class="align-right">Raw Bucket Leaks</th>
              <th class="align-right">Logical Neighbors</th>
            </tr>
          </thead>
          <tbody>${snRows}</tbody>
        </table>
      </div>
    </section>

    <div class="grid-2">
      <section class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">2. Derived-Edge Hot-Path Stepper</h2>
            <p class="panel-subtitle">Rule <code>${escapeHtml(hotPath.rule_name || 'similar_to_shared_neighbor')}</code> &rarr; <code>${escapeHtml(hotPath.derived_edge_type || 'SIMILAR_TO')}</code></p>
          </div>
          <div class="control-bar">
            <button type="button" class="btn" data-action="hotpath-next">Next Hit (${escapeHtml(hpCursor)}/${escapeHtml(hpSteps.length || 4)})</button>
            <button type="button" class="btn" data-action="hotpath-reset">Reset</button>
            <button type="button" class="btn btn-primary" data-action="hotpath-rerun" ${state.busyAction === 'hotpath' ? 'disabled' : ''}>${state.mode === 'live' ? 'Run Live' : 'Replay All'}</button>
          </div>
        </div>

        <div class="callout">
          <strong>Promotion trigger:</strong> Hits 1..${escapeHtml(Math.max(1, (hotPath.hot_path_threshold ?? 3) - 1))} evaluate derived edges on the fly. At <strong>Hit #${escapeHtml(hotPath.promoted_at_hit ?? hotPath.hot_path_threshold ?? 3)}</strong>, <code>QueryCoordinator</code> auto-promotes <code>materialized_hot_path=True</code>.
          ${currentStep ? ` Currently viewing <strong>Hit #${escapeHtml(currentStep.hit_number)}</strong> (${currentStep.materialized ? 'Materialized fast path' : 'Dynamic inference'}).` : ''}
        </div>

        <div class="stepper-grid">${stepperCards}</div>
      </section>

      <section class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">3. Cross-Domain Federation &amp; Hop-0 Routing</h2>
            <p class="panel-subtitle">Canonical ID prefix parsing + Ghost/Proxy cross-shard locality.</p>
          </div>
          <span class="pill pill-live">O(1) Prefix Route</span>
        </div>

        <div class="canonical-strip">
          <span class="control-label">Canonical ID:</span>
          <span class="code-chip">${escapeHtml(federation.sample_canonical_id || 'customer:org:acme')}</span>
          <span class="muted">&rarr;</span>
          <span class="meta-chip">domain: ${escapeHtml(federation.parsed_domain || 'customer')}</span>
          <span class="meta-chip">type: ${escapeHtml(federation.parsed_entity_type || 'org')}</span>
          <span class="meta-chip">slug: ${escapeHtml(federation.parsed_slug || 'acme')}</span>
        </div>

        <div class="chart-box">${fedChart}</div>

        <div class="table-wrap mt-sm">
          <table class="data-table">
            <thead>
              <tr>
                <th>Cross-Domain Share</th>
                <th class="align-right">Ghost Proxies</th>
                <th class="align-right">Cross Hops</th>
                <th class="align-right">Locality Ratio</th>
                <th class="align-right">Latency</th>
              </tr>
            </thead>
            <tbody>${fedRows}</tbody>
          </table>
        </div>
      </section>
    </div>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">4. Guardrail Stress Triggers (Live / Real Probe Output)</h2>
          <p class="panel-subtitle">Trigger real Lean-Graph write rejections (HTTP 422) and bounded runtime expansion aborts (<code>aborted=True</code>).</p>
        </div>
        <span class="pill pill-danger">Zero Unbounded Queries</span>
      </div>
      <div class="probe-grid">${probeButtonsHtml}</div>
      ${probePayloadHtml}
    </section>
  `;
}

/**
 * Recompute projected Gate 2 weighted scorecard at selected scale target (100M / 1B / 10B)
 * mirroring src/scale_kg/benchmark/report.py weights (35 / 25 / 15 / 15 / 10).
 */
function computeScaleScorecard(baseCard, adapterResult, scaleLabel, budgets) {
  if (!adapterResult) {
    return {
      ...baseCard,
      projected2Hop: null,
      projected3Hop: null,
      projectedCost: null,
      perfAtScale: baseCard.performance_score,
      costAtScale: baseCard.cost_score,
      scaleScore: baseCard.weighted_total,
    };
  }
  const ladder = adapterResult.cost_extrapolation?.scale_ladder || [];
  const step = ladder.find((s) => s.scale_label === scaleLabel) || ladder[1] || ladder[0];
  if (!step) {
    return {
      ...baseCard,
      projected2Hop: null,
      projected3Hop: null,
      projectedCost: null,
      perfAtScale: baseCard.performance_score,
      costAtScale: baseCard.cost_score,
      scaleScore: baseCard.weighted_total,
    };
  }

  const b1 = safeNum(budgets.hop_1_p95_ms, 25);
  const b2 = safeNum(budgets.hop_2_p95_ms, 100);
  const b3 = safeNum(budgets.hop_3_p95_ms, 300);

  const d1 = safeNum(adapterResult.traversal_latency?.by_depth?.['1']?.p95_ms, 0);
  const d2 = safeNum(step.projected_2hop_p95_ms, 0);
  const d3 = safeNum(step.projected_3hop_p95_ms, 0);

  const scores = [
    Math.max(0, 100 - (d1 / Math.max(0.001, b1)) * 45),
    Math.max(0, 100 - (d2 / Math.max(0.001, b2)) * 45),
    Math.max(0, 100 - (d3 / Math.max(0.001, b3)) * 45),
  ];
  const avgLat = scores.reduce((a, b) => a + b, 0) / scores.length;
  const snBonus = adapterResult.supernode?.within_guardrails ? 10 : -25;
  const qpsBonus = Math.min(10, safeNum(adapterResult.concurrent_ramp?.peak_qps, 0) / 50);
  const perfAtScale = Math.max(0, Math.min(100, avgLat + snBonus + qpsBonus));

  // Scale-adjusted cost score normalized relative to 1B reference tier
  const monthlyUsd = safeNum(step.projected_monthly_cost_usd, 0);
  const per1bNormalized = monthlyUsd / Math.max(0.1, safeNum(step.edge_count, 1e9) / 1e9);
  const costScore =
    per1bNormalized <= 0 ? 100 : Math.max(0, Math.min(100, 100 - per1bNormalized / 40));

  const scaleTotal = Number(
    (
      perfAtScale * 0.35 +
      safeNum(baseCard.scalability_resilience_score, 0) * 0.25 +
      costScore * 0.15 +
      safeNum(baseCard.operability_score, 0) * 0.15 +
      safeNum(baseCard.graphrag_fit_score, 0) * 0.1
    ).toFixed(2)
  );

  return {
    ...baseCard,
    projected2Hop: d2,
    projected3Hop: d3,
    projectedCost: monthlyUsd,
    perfAtScale: Number(perfAtScale.toFixed(1)),
    costAtScale: Number(costScore.toFixed(1)),
    scaleScore: scaleTotal,
  };
}

/**
 * TAB 3: Adapter Switchboard — Catalog, Golden Query comparator, 7-workload benchmarks & 100M/1B/10B Gate 2 scorecard.
 */
export function renderAdapterSwitchboard(snapshot, state) {
  const intro = snapshot.introspection || {};
  const budgets = budgetsToMap(intro);
  const { preset: activePreset, bundle } = getActiveBundle(snapshot, state);
  const suite = bundle?.suite_result || {};
  const adapterMap = suite.adapters || {};
  const adapterNames = Object.keys(adapterMap);
  const selectedBenchAdapter =
    (state.selectedBenchAdapter && adapterMap[state.selectedBenchAdapter]
      ? state.selectedBenchAdapter
      : adapterNames[0]) || null;
  const activeBench = selectedBenchAdapter ? adapterMap[selectedBenchAdapter] : null;
  const scaleTarget = SCALE_EDGE_COUNTS[state.scaleTarget] ? state.scaleTarget : '1B';
  const targetEdges = SCALE_EDGE_COUNTS[scaleTarget];

  // 1. Engine Catalog Table
  const catalogRows = (intro.adapters || [])
    .map((entry) => {
      const cp = entry.cost_profile || {};
      const scaledCost =
        (targetEdges / 1_000_000_000) *
        safeNum(cp.estimated_cost_per_billion_edges_usd_month, 0);
      return `
        <tr>
          <td class="mono">${escapeHtml(entry.key)}</td>
          <td>${escapeHtml(cp.engine || entry.class_name)}</td>
          <td>${availabilityPill(entry.availability)}</td>
          <td class="mono">${escapeHtml(cp.tier || '—')}</td>
          <td>${escapeHtml(cp.storage_backend || '—')}</td>
          <td class="mono">${escapeHtml((cp.query_languages || []).join(', '))}</td>
          <td>${cp.supports_zero_etl ? '<span class="pill pill-ok">Yes</span>' : '<span class="pill pill-stub">No</span>'}</td>
          <td>${cp.supports_cross_shard_edges ? '<span class="pill pill-ok">Yes</span>' : '<span class="pill pill-warn">No</span>'}</td>
          <td class="num align-right">${escapeHtml(fmtUsd(scaledCost))}</td>
        </tr>
      `;
    })
    .join('');

  // 2. Interactive Golden Query Comparator
  const goldenList = intro.golden_queries || [];
  const selectedQueryId = state.selectedGoldenQuery || goldenList[2]?.query_id || 'GQ-03';

  // Resolve comparator result from live state OR pre-computed golden_parity comparison
  let switchComparison = state.liveAdapterSwitch || null;
  if (!switchComparison || switchComparison.query_id !== selectedQueryId) {
    const pre = (suite.golden_parity?.comparisons || []).find(
      (c) => c.query_id === selectedQueryId
    );
    if (pre) {
      switchComparison = {
        query_id: pre.query_id,
        query_name: pre.query_name,
        parity_matched: pre.passed,
        source: 'snapshot',
        results: Object.values(pre.adapter_results || {}).map((r) => ({
          adapter: r.adapter_name,
          node_count: (r.node_ids || []).length,
          edge_count: (r.edge_keys || []).length,
          path_count: (r.paths || []).length,
          bucket_hops_unwrapped: r.bucket_hops_unwrapped,
          execution_ms: r.execution_time_ms,
          node_ids: r.node_ids || [],
        })),
      };
    }
  }

  const compareCardsHtml = switchComparison
    ? `
      <div class="callout ${switchComparison.parity_matched ? 'callout-ok' : 'callout-warn'} mt-sm">
        <strong>${escapeHtml(switchComparison.query_id)} (${escapeHtml(switchComparison.query_name)}):</strong>
        ${
          switchComparison.parity_matched
            ? '100% deterministic node/path parity across adapters — zero application code change needed to switch engines.'
            : 'Parity mismatch detected between compared adapters.'
        }
      </div>
      <div class="compare-grid">
        ${(switchComparison.results || [])
          .map(
            (item) => `
          <div class="compare-card">
            <div class="layer-card-top">
              <strong class="mono">${escapeHtml(item.adapter)}</strong>
              <span class="pill pill-ok">${escapeHtml(fmtMs(item.execution_ms))}</span>
            </div>
            <div class="chip-list mt-sm">
              <span class="meta-chip">Nodes: ${escapeHtml(item.node_count)}</span>
              <span class="meta-chip">Edges: ${escapeHtml(item.edge_count)}</span>
              <span class="meta-chip">Paths: ${escapeHtml(item.path_count)}</span>
              <span class="meta-chip">Unwrapped: ${escapeHtml(item.bucket_hops_unwrapped)}</span>
            </div>
            <div class="drawer-section">
              <div class="drawer-label">Returned Logical Node IDs</div>
              <div class="chip-list">
                ${(item.node_ids || []).map((id) => `<span class="code-chip">${escapeHtml(id)}</span>`).join('')}
              </div>
            </div>
          </div>
        `
          )
          .join('')}
      </div>
    `
    : '<p class="muted mt-sm">Click Compare Adapters to evaluate identical logical GraphRAG output across engines.</p>';

  // Parity matrix
  const parityMatrixRows = (suite.golden_parity?.comparisons || [])
    .map((comp) => {
      const adaptersSummary = Object.values(comp.adapter_results || {})
        .map((r) => `${r.adapter_name}: ${(r.node_ids || []).length}n (${safeNum(r.execution_time_ms).toFixed(2)}ms)`)
        .join(' · ');
      return `
        <tr>
          <td class="mono">${escapeHtml(comp.query_id)}</td>
          <td>${escapeHtml(comp.query_name)}</td>
          <td class="mono">${escapeHtml(comp.category)}</td>
          <td>${comp.passed ? '<span class="pill pill-ok">PARITY PASS</span>' : '<span class="pill pill-danger">MISMATCH</span>'}</td>
          <td class="mono muted">${escapeHtml(adaptersSummary)}</td>
        </tr>
      `;
    })
    .join('');

  // 3. 7-Workload Benchmark Charts + Table
  const latencyChart = activeBench
    ? renderLatencyBarChart(activeBench.traversal_latency?.by_depth || {}, budgets)
    : '<p class="muted">No benchmark data available.</p>';

  const qpsPoints = (activeBench?.concurrent_ramp?.levels || []).map((lvl) => ({
    ...lvl,
    label: `c=${lvl.concurrency}`,
  }));
  const qpsChart = renderLineChart(qpsPoints, {
    xKey: 'concurrency',
    yKey: 'qps',
    title: `Concurrent QPS Ramp (${selectedBenchAdapter || 'adapter'})`,
    yUnit: 'QPS',
    xLabel: 'Concurrent Workers',
    color: '#34d399',
  });

  const workloadRows = adapterNames
    .map((name) => {
      const r = adapterMap[name];
      const byD = r.traversal_latency?.by_depth || {};
      return `
        <tr>
          <td class="mono">${escapeHtml(name)}</td>
          <td class="num align-right">${escapeHtml(safeNum(byD['1']?.p95_ms).toFixed(2))} ms</td>
          <td class="num align-right">${escapeHtml(safeNum(byD['2']?.p95_ms).toFixed(2))} ms</td>
          <td class="num align-right">${escapeHtml(safeNum(byD['3']?.p95_ms).toFixed(2))} ms</td>
          <td class="num align-right">${escapeHtml(safeNum(r.supernode?.p99_ms).toFixed(2))} ms (${escapeHtml(r.supernode?.raw_bucket_leaks ?? 0)} leaks)</td>
          <td class="num align-right">${escapeHtml(fmtNum(r.concurrent_ramp?.peak_qps, 1))}</td>
          <td class="num align-right">${escapeHtml(fmtNum(r.ingestion?.bulk_edges_per_sec, 0))}/s</td>
          <td class="num align-right">${escapeHtml(safeNum(r.cross_partition?.worst_latency_ratio).toFixed(2))}x</td>
          <td class="num align-right">${escapeHtml(safeNum(r.ha_failure?.recovery_time_ms).toFixed(2))} ms</td>
          <td>${r.ha_failure?.recovered_cleanly && r.ingestion?.write_discipline_passed ? '<span class="pill pill-ok">PASS</span>' : '<span class="pill pill-warn">CHECK</span>'}</td>
        </tr>
      `;
    })
    .join('');

  // 4. Interactive 100M / 1B / 10B Scale Ladder & Gate 2 Scorecard
  const rawScorecards = bundle?.scorecards || [];
  const scaledCards = rawScorecards
    .map((card) => computeScaleScorecard(card, adapterMap[card.adapter_name], scaleTarget, budgets))
    .sort((a, b) => {
      if (a.eligible_for_production !== b.eligible_for_production) {
        return a.eligible_for_production ? -1 : 1;
      }
      return b.scaleScore - a.scaleScore;
    });

  const scorecardRows = scaledCards
    .map((card, idx) => {
      const isWinner = card.adapter_name === bundle?.winner_adapter;
      return `
        <tr class="${isWinner ? 'row-highlight' : ''}">
          <td class="num">#${escapeHtml(idx + 1)}</td>
          <td class="mono">${escapeHtml(card.adapter_name)} ${isWinner ? '<span class="pill pill-ok">WINNER</span>' : ''}</td>
          <td class="mono">${escapeHtml(card.tier)}${card.is_stub ? ' (stub)' : ''}</td>
          <td class="num align-right">${card.projected2Hop !== null ? escapeHtml(fmtMs(card.projected2Hop)) : '—'}</td>
          <td class="num align-right">${card.projected3Hop !== null ? escapeHtml(fmtMs(card.projected3Hop)) : '—'}</td>
          <td class="num align-right">${card.projectedCost !== null ? escapeHtml(fmtUsd(card.projectedCost)) : '—'}</td>
          <td class="num align-right">${escapeHtml(card.perfAtScale ?? card.performance_score)}</td>
          <td class="num align-right">${escapeHtml(card.scalability_resilience_score)}</td>
          <td class="num align-right">${escapeHtml(card.costAtScale ?? card.cost_score)}</td>
          <td class="num align-right">${escapeHtml(card.operability_score)}</td>
          <td class="num align-right"><strong>${escapeHtml(card.scaleScore.toFixed(2))}</strong></td>
          <td>${
            card.eligible_for_production
              ? '<span class="pill pill-ok">Eligible</span>'
              : `<span class="pill pill-stub" title="${escapeHtml((card.disqualified_reasons || []).join('; '))}">Non-Prod</span>`
          }</td>
        </tr>
      `;
    })
    .join('');

  return `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">GraphStore Engine Catalog (${escapeHtml(scaleTarget)} Edge Cost Projection)</h2>
          <p class="panel-subtitle">All 6 registered adapters behind the engine-agnostic <code>GraphStore</code> protocol with honest availability badges.</p>
        </div>
        <div class="segmented" role="group" aria-label="Target edge scale">
          ${['100M', '1B', '10B']
            .map(
              (sc) => `
            <button
              type="button"
              class="seg-btn ${scaleTarget === sc ? 'is-active' : ''}"
              data-action="select-scale"
              data-scale="${escapeHtml(sc)}"
            >${escapeHtml(sc)} Edges</button>
          `
            )
            .join('')}
        </div>
      </div>
      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Adapter Key</th>
              <th>Engine</th>
              <th>Availability</th>
              <th>Tier</th>
              <th>Storage Backend</th>
              <th>Query Languages</th>
              <th>Zero-ETL</th>
              <th>Cross-Shard</th>
              <th class="align-right">Est. Cost (${escapeHtml(scaleTarget)})</th>
            </tr>
          </thead>
          <tbody>${catalogRows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Golden Query Live Adapter Switch Comparator</h2>
          <p class="panel-subtitle">Execute any frozen Golden Query (<code>GQ-01</code>..<code>GQ-07</code>) across adapters to verify identical logical results.</p>
        </div>
        <div class="control-bar">
          <label class="control-group" for="golden-query-select">
            <span class="control-label">Query</span>
            <select id="golden-query-select" class="select-input">
              ${goldenList
                .map(
                  (q) => `
                <option value="${escapeHtml(q.query_id)}" ${q.query_id === selectedQueryId ? 'selected' : ''}>
                  ${escapeHtml(q.query_id)} — ${escapeHtml(q.name)} (depth ${escapeHtml(q.max_depth)})
                </option>
              `
                )
                .join('')}
            </select>
          </label>
          <button
            type="button"
            class="btn btn-primary"
            data-action="compare-adapters"
            ${state.busyAction === 'compare' ? 'disabled' : ''}
          >${state.busyAction === 'compare' ? 'Comparing…' : 'Compare Adapters'}</button>
        </div>
      </div>

      ${compareCardsHtml}

      <div class="table-wrap mt-md">
        <table class="data-table">
          <thead>
            <tr><th>ID</th><th>Golden Query</th><th>Category</th><th>Parity</th><th>Observed Outputs</th></tr>
          </thead>
          <tbody>${parityMatrixRows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">7-Workload Empirical Benchmark (${escapeHtml(activePreset.toUpperCase())} Preset)</h2>
          <p class="panel-subtitle">Measured 1/2/3-hop latency vs Gate 2 SLA budgets, concurrent QPS ramp, ingestion, cross-partition cost, and HA failover.</p>
        </div>
        <div class="segmented" role="group" aria-label="Select benchmarked adapter">
          ${adapterNames
            .map(
              (name) => `
            <button
              type="button"
              class="seg-btn ${selectedBenchAdapter === name ? 'is-active' : ''}"
              data-action="select-bench-adapter"
              data-adapter="${escapeHtml(name)}"
            >${escapeHtml(name)}</button>
          `
            )
            .join('')}
        </div>
      </div>

      <div class="grid-2">
        <div class="chart-box">${latencyChart}</div>
        <div class="chart-box">${qpsChart}</div>
      </div>

      <div class="table-wrap mt-md">
        <table class="data-table">
          <thead>
            <tr>
              <th>Adapter</th>
              <th class="align-right">1-Hop P95</th>
              <th class="align-right">2-Hop P95</th>
              <th class="align-right">3-Hop P95</th>
              <th class="align-right">Supernode P99</th>
              <th class="align-right">Peak QPS</th>
              <th class="align-right">Bulk Ingest</th>
              <th class="align-right">Cross-Part Ratio</th>
              <th class="align-right">HA Recovery</th>
              <th>Discipline</th>
            </tr>
          </thead>
          <tbody>${workloadRows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Gate 2 Weighted Scorecard &amp; Scale Extrapolation (${escapeHtml(scaleTarget)} Target)</h2>
          <p class="panel-subtitle">Weights: Performance 35% · Scalability/HA 25% · Cost/1B 15% · Operability 15% · GraphRAG Fit 10%.</p>
        </div>
        <div class="segmented" role="group" aria-label="Scorecard target edge scale">
          ${['100M', '1B', '10B']
            .map(
              (sc) => `
            <button
              type="button"
              class="seg-btn ${scaleTarget === sc ? 'is-active' : ''}"
              data-action="select-scale"
              data-scale="${escapeHtml(sc)}"
            >${escapeHtml(sc)}</button>
          `
            )
            .join('')}
        </div>
      </div>

      <div class="callout callout-ok">
        <strong>Gate 2 Recommendation:</strong> <code>${escapeHtml(bundle?.winner_adapter || 'iceberg_puppygraph')}</code> leads production-eligible engines at <strong>${escapeHtml(scaleTarget)} edges</strong> while <code>in_memory</code> serves as the non-persistent reference baseline.
      </div>

      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Rank</th>
              <th>Adapter</th>
              <th>Tier</th>
              <th class="align-right">${escapeHtml(scaleTarget)} 2-Hop P95</th>
              <th class="align-right">${escapeHtml(scaleTarget)} 3-Hop P95</th>
              <th class="align-right">${escapeHtml(scaleTarget)} Monthly Cost</th>
              <th class="align-right">Perf (35%)</th>
              <th class="align-right">Scale (25%)</th>
              <th class="align-right">Cost (15%)</th>
              <th class="align-right">Ops (15%)</th>
              <th class="align-right">Weighted Total</th>
              <th>Prod Gate</th>
            </tr>
          </thead>
          <tbody>${scorecardRows}</tbody>
        </table>
      </div>

      ${
        bundle?.adr_markdown
          ? `<details class="adr-details">
              <summary>View Generated Gate 2 ADR Rationale (ADR-0002)</summary>
              <pre class="payload-pre">${escapeHtml(bundle.adr_markdown)}</pre>
            </details>`
          : ''
      }
    </section>
  `;
}

/**
 * TAB 4: Hybrid GraphRAG — 5-stage waterfall, selective Parquet hydration savings & citations.
 */
export function renderHybridRag(snapshot, state) {
  const rag = state.liveHybridRag || snapshot.scenarios?.hybrid_rag || {};
  const stages = rag.stage_timings_ms || {};
  const totalMs = Object.values(stages).reduce((acc, v) => acc + safeNum(v, 0), 0);

  const naiveBytes = Math.max(1, safeNum(rag.naive_full_graph_bytes, 1));
  const hydratedBytes = Math.max(0, safeNum(rag.hydrated_bytes, 0));
  const hydratedPct = Math.min(100, (hydratedBytes / naiveBytes) * 100);
  const savingsPct = safeNum(rag.payload_savings_pct, 0);

  const waterfallSvg = renderWaterfallChart(stages);

  const citationsHtml = (rag.top_hit_ids || [])
    .map((id, idx) => {
      const [domain = 'unknown', entityType = 'entity', slug = id] = String(id).split(':');
      return `
        <li class="citation-item">
          <div>
            <span class="pill pill-live">#${escapeHtml(idx + 1)}</span>
            <span class="code-chip">${escapeHtml(id)}</span>
          </div>
          <div class="chip-list">
            <span class="meta-chip">domain=${escapeHtml(domain)}</span>
            <span class="meta-chip">type=${escapeHtml(entityType)}</span>
            <span class="meta-chip">slug=${escapeHtml(slug)}</span>
            <span class="pill pill-ok">Grounded Citation</span>
          </div>
        </li>
      `;
    })
    .join('');

  return `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">5-Stage Seed-Then-Expand Hybrid GraphRAG Pipeline</h2>
          <p class="panel-subtitle">Query: <strong>&ldquo;${escapeHtml(rag.query_text || 'How does Kinetic-V scale GraphRAG across supernodes?')}&rdquo;</strong></p>
        </div>
        <button
          type="button"
          class="btn btn-primary"
          data-action="rerun-hybrid"
          ${state.busyAction === 'hybrid' ? 'disabled' : ''}
        >${state.busyAction === 'hybrid' ? 'Tracing…' : state.mode === 'live' ? 'Re-run Live Trace' : 'Replay Trace'}</button>
      </div>

      <div class="callout callout-ok">
        <strong>Why Lean Graph + Selective Hydration matters:</strong> Vector search finds <strong>${escapeHtml(rag.seed_count ?? 0)}</strong> seeds, bounded graph traversal expands <strong>${escapeHtml(rag.graph_candidates ?? 0)}</strong> structural candidates with zero text bloat, RRF fuses rankings, and only the final <strong>${escapeHtml(rag.hydrated_documents ?? 0)} of ${escapeHtml(rag.total_lakehouse_documents ?? 0)}</strong> Lakehouse Parquet documents are hydrated — saving <strong>${escapeHtml(savingsPct.toFixed(1))}%</strong> payload I/O.
      </div>

      <div class="kpi-grid">
        <div class="kpi-card">
          <div class="kpi-label">End-to-End Trace</div>
          <div class="kpi-value tone-accent">${escapeHtml(fmtMs(totalMs))}</div>
          <div class="kpi-note">Across all 5 pipeline stages</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Vector Seeds &rarr; Graph Candidates</div>
          <div class="kpi-value">${escapeHtml(rag.seed_count ?? 0)} &rarr; ${escapeHtml(rag.graph_candidates ?? 0)}</div>
          <div class="kpi-note">Bounded 2-hop structural expansion</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Selective Hydration</div>
          <div class="kpi-value tone-ok">${escapeHtml(rag.hydrated_documents ?? 0)} / ${escapeHtml(rag.total_lakehouse_documents ?? 0)}</div>
          <div class="kpi-note">Parquet docs hydrated post-RRF</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Payload I/O Saved</div>
          <div class="kpi-value tone-ok">${escapeHtml(savingsPct.toFixed(1))}%</div>
          <div class="kpi-note">${escapeHtml(fmtBytes(hydratedBytes))} vs ${escapeHtml(fmtBytes(naiveBytes))} naive</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Grounded Citations</div>
          <div class="kpi-value">${escapeHtml(rag.citation_count ?? 0)}</div>
          <div class="kpi-note">Entity + Lakehouse provenance</div>
        </div>
      </div>
    </section>

    <div class="grid-2">
      <section class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Stage-by-Stage Execution Waterfall</h2>
            <p class="panel-subtitle">Observed latency per stage from <code>HybridGraphRAGRouter.retrieve()</code>.</p>
          </div>
        </div>
        <div class="chart-box">${waterfallSvg}</div>
      </section>

      <section class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Selective Top-N Parquet Hydration vs Naive Full-Graph Read</h2>
            <p class="panel-subtitle">Payload bytes transferred when hydrating post-RRF top-N pointers vs storing blobs in graph vertices.</p>
          </div>
          <span class="pill pill-ok">-${escapeHtml(savingsPct.toFixed(1))}% bytes</span>
        </div>

        <div class="hydration-bars">
          <div class="hydration-row">
            <div class="hydration-row-head">
              <strong>Selective Top-N Lakehouse Hydration</strong>
              <span class="num text-ok">${escapeHtml(fmtBytes(hydratedBytes))} (${escapeHtml(hydratedPct.toFixed(1))}%)</span>
            </div>
            ${renderMeterBar(hydratedPct, { color: '#34d399', label: 'Selective Lakehouse Hydration' })}
          </div>

          <div class="hydration-row">
            <div class="hydration-row-head">
              <strong>Naive Full-Graph Inline Document Blobs</strong>
              <span class="num text-danger">${escapeHtml(fmtBytes(naiveBytes))} (100.0%)</span>
            </div>
            ${renderMeterBar(100, { color: '#fb7185', label: 'Naive Full-Graph Read' })}
          </div>
        </div>

        <div class="drawer-section">
          <div class="drawer-label">Hydration Savings Breakdown</div>
          <div class="chip-list">
            <span class="meta-chip">Avoided: ${escapeHtml(fmtBytes(Math.max(0, naiveBytes - hydratedBytes)))}</span>
            <span class="meta-chip">Docs skipped: ${escapeHtml(Math.max(0, safeNum(rag.total_lakehouse_documents) - safeNum(rag.hydrated_documents)))}</span>
            <span class="meta-chip">Format: Apache Parquet</span>
          </div>
        </div>
      </section>
    </div>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Grounded Top Hits &amp; Entity Citations</h2>
          <p class="panel-subtitle">Canonical entities returned after Reciprocal Rank Fusion and selective Parquet hydration.</p>
        </div>
      </div>
      <ul class="citation-list">${citationsHtml}</ul>
    </section>
  `;
}

/**
 * TAB 5: Commit Trends — Multi-commit sparklines & build provenance table.
 */
export function renderCommitTrends(snapshot, history) {
  const intro = snapshot.introspection || {};
  const entries = Array.isArray(history) ? history : [];

  // Synthesize single-point baseline from current snapshot if history.json is empty
  let effectiveEntries = entries;
  let isBaselineOnly = false;
  if (effectiveEntries.length === 0) {
    isBaselineOnly = true;
    const defaultPreset = snapshot.default_preset || 'micro';
    const bundle = snapshot.benchmarks?.[defaultPreset] || Object.values(snapshot.benchmarks || {})[0];
    const winnerName = bundle?.winner_adapter || Object.keys(bundle?.suite_result?.adapters || {})[0] || 'in_memory';
    const winnerBench = bundle?.suite_result?.adapters?.[winnerName];
    const winnerCard = (bundle?.scorecards || []).find((c) => c.adapter_name === winnerName) || bundle?.scorecards?.[0];
    const byD = winnerBench?.traversal_latency?.by_depth || {};
    effectiveEntries = [
      {
        git_sha: intro.git_sha || 'local-dev',
        git_ref: intro.git_ref || 'local',
        built_at: intro.built_at || '',
        config_fingerprint: intro.config_fingerprint || '—',
        preset: defaultPreset,
        hop_1_p95_ms: safeNum(byD['1']?.p95_ms, 0),
        hop_2_p95_ms: safeNum(byD['2']?.p95_ms, 0),
        hop_3_p95_ms: safeNum(byD['3']?.p95_ms, 0),
        peak_qps: safeNum(winnerBench?.concurrent_ramp?.peak_qps, 0),
        winner_adapter: winnerName,
        winner_score: safeNum(winnerCard?.weighted_total, 0),
        golden_parity_passed: bundle?.suite_result?.golden_parity?.passed !== false,
      },
    ];
  }

  const latest = effectiveEntries[effectiveEntries.length - 1];
  const parityStreak = effectiveEntries.reduceRight((acc, item) => {
    if (acc.stopped) {
      return acc;
    }
    if (item.golden_parity_passed) {
      return { count: acc.count + 1, stopped: false };
    }
    return { count: acc.count, stopped: true };
  }, { count: 0, stopped: false }).count;

  const sparkCards = [
    {
      label: '1-Hop P95 Latency',
      latest: fmtMs(latest.hop_1_p95_ms),
      svg: renderSparkline(
        effectiveEntries.map((e) => e.hop_1_p95_ms),
        { label: '1-Hop P95', unit: 'ms', color: '#38bdf8' }
      ),
    },
    {
      label: '2-Hop P95 Latency',
      latest: fmtMs(latest.hop_2_p95_ms),
      svg: renderSparkline(
        effectiveEntries.map((e) => e.hop_2_p95_ms),
        { label: '2-Hop P95', unit: 'ms', color: '#818cf8' }
      ),
    },
    {
      label: '3-Hop P95 Latency',
      latest: fmtMs(latest.hop_3_p95_ms),
      svg: renderSparkline(
        effectiveEntries.map((e) => e.hop_3_p95_ms),
        { label: '3-Hop P95', unit: 'ms', color: '#fbbf24' }
      ),
    },
    {
      label: 'Peak Concurrent QPS',
      latest: `${fmtNum(latest.peak_qps, 1)} QPS`,
      svg: renderSparkline(
        effectiveEntries.map((e) => e.peak_qps),
        { label: 'Peak QPS', unit: 'QPS', color: '#34d399' }
      ),
    },
    {
      label: 'Gate 2 Winner Score',
      latest: `${safeNum(latest.winner_score, 0).toFixed(2)} / 100`,
      svg: renderSparkline(
        effectiveEntries.map((e) => e.winner_score),
        { label: 'Gate 2 Winner Score', unit: 'pts', color: '#34d399' }
      ),
    },
    {
      label: 'Golden Parity Streak',
      latest: `${parityStreak} / ${effectiveEntries.length} PASS`,
      svg: renderSparkline(
        effectiveEntries.map((e) => (e.golden_parity_passed ? 100 : 0)),
        { label: 'Golden Parity Pass Rate', unit: '%', color: '#38bdf8' }
      ),
    },
  ];

  const sparkGridHtml = sparkCards
    .map(
      (c) => `
      <article class="sparkline-card">
        <div class="sparkline-top">
          <span class="kpi-label">${escapeHtml(c.label)}</span>
          <span class="num">${escapeHtml(c.latest)}</span>
        </div>
        ${c.svg}
      </article>
    `
    )
    .join('');

  const historyRows = [...effectiveEntries]
    .reverse()
    .map(
      (e) => `
      <tr>
        <td class="mono">${escapeHtml(e.git_sha)}</td>
        <td class="mono">${escapeHtml(e.git_ref)}</td>
        <td class="mono muted">${escapeHtml(formatTimestamp(e.built_at))}</td>
        <td class="mono">${escapeHtml(e.config_fingerprint)}</td>
        <td class="mono">${escapeHtml(e.preset)}</td>
        <td class="num align-right">${escapeHtml(safeNum(e.hop_1_p95_ms).toFixed(2))} ms</td>
        <td class="num align-right">${escapeHtml(safeNum(e.hop_2_p95_ms).toFixed(2))} ms</td>
        <td class="num align-right">${escapeHtml(safeNum(e.hop_3_p95_ms).toFixed(2))} ms</td>
        <td class="num align-right">${escapeHtml(fmtNum(e.peak_qps, 1))}</td>
        <td class="mono">${escapeHtml(e.winner_adapter)} (${escapeHtml(safeNum(e.winner_score).toFixed(1))})</td>
        <td>${e.golden_parity_passed ? '<span class="pill pill-ok">PASS</span>' : '<span class="pill pill-danger">FAIL</span>'}</td>
      </tr>
    `
    )
    .join('');

  return `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Commit-by-Commit Telemetry Trends</h2>
          <p class="panel-subtitle">Rolling CI benchmark history tracking latency regressions, peak throughput, Gate 2 scores, and Golden Query parity.</p>
        </div>
        <span class="pill ${isBaselineOnly ? 'pill-warn' : 'pill-ok'}">${escapeHtml(effectiveEntries.length)} ${effectiveEntries.length === 1 ? 'Snapshot' : 'Commits'}</span>
      </div>

      ${
        isBaselineOnly
          ? '<div class="callout callout-warn"><strong>Baseline Snapshot Active:</strong> Showing current build metrics as the initial data point. Subsequent CI builds append to rolling <code>history.json</code> (up to 50 commits).</div>'
          : ''
      }

      <div class="grid-3">${sparkGridHtml}</div>
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Build Provenance &amp; Commit Audit Log</h2>
          <p class="panel-subtitle">Exact git SHA, branch, timestamp, and config fingerprint linking every chart back to committed code.</p>
        </div>
      </div>

      <div class="table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Git SHA</th>
              <th>Branch / Ref</th>
              <th>Built At (UTC)</th>
              <th>Config Fingerprint</th>
              <th>Preset</th>
              <th class="align-right">1-Hop P95</th>
              <th class="align-right">2-Hop P95</th>
              <th class="align-right">3-Hop P95</th>
              <th class="align-right">Peak QPS</th>
              <th>Gate 2 Winner</th>
              <th>Parity</th>
            </tr>
          </thead>
          <tbody>${historyRows}</tbody>
        </table>
      </div>
    </section>
  `;
}
