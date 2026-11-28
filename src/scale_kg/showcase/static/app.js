// Zero-build live/static dashboard controller (strict CSP, zero external URLs).

import { escapeHtml } from './charts.js';
import {
  renderAdapterSwitchboard,
  renderCommitTrends,
  renderHybridRag,
  renderLoadEscalator,
  renderSystemMap,
} from './views.js';

const TAB_ORDER = [
  'system-map',
  'load-escalator',
  'adapter-switchboard',
  'hybrid-rag',
  'commit-trends',
];

const state = {
  mode: document.body.dataset.mode === 'static' ? 'static' : 'live',
  activeTab: 'system-map',
  snapshot: null,
  history: [],
  selectedLayer: 'layer-2',
  activePreset: 'micro',
  rampSeed: 42,
  lastRampMeta: null,
  hotPathStep: 1,
  liveHotPath: null,
  selectedProbe: 'forbidden_payload',
  liveGuardrails: {},
  selectedGoldenQuery: 'GQ-03',
  liveAdapterSwitch: null,
  selectedBenchAdapter: null,
  scaleTarget: '1B',
  liveHybridRag: null,
  busyAction: null,
};

const dom = {
  appContent: document.getElementById('app-content'),
  statusBanner: document.getElementById('status-banner'),
  tablist: document.getElementById('main-tablist'),
  modeBadge: document.getElementById('mode-badge'),
  metaSha: document.getElementById('meta-sha'),
  metaRef: document.getElementById('meta-ref'),
  metaBuilt: document.getElementById('meta-built'),
  metaFp: document.getElementById('meta-fp'),
  footerMeta: document.getElementById('footer-meta'),
};

function apiBase() {
  return state.mode === 'live' ? '/showcase/api' : '.';
}

function showStatus(message, tone = 'info') {
  if (!dom.statusBanner) {
    return;
  }
  dom.statusBanner.className = `status-banner status-${tone}`;
  dom.statusBanner.textContent = message;
}

function clearStatus() {
  if (!dom.statusBanner) {
    return;
  }
  dom.statusBanner.className = 'status-banner is-hidden';
  dom.statusBanner.textContent = '';
}

function formatUtcShort(iso) {
  if (!iso) {
    return '—';
  }
  return String(iso).replace('T', ' ').slice(0, 19) + 'Z';
}

function updateProvenanceHeader() {
  const intro = state.snapshot?.introspection || {};
  if (dom.modeBadge) {
    dom.modeBadge.textContent = state.mode === 'live' ? 'LIVE API' : 'STATIC SNAPSHOT';
    dom.modeBadge.className = `pill ${state.mode === 'live' ? 'pill-live' : 'pill-accent'}`;
  }
  if (dom.metaSha) {
    dom.metaSha.textContent = `SHA: ${intro.git_sha || '—'}`;
  }
  if (dom.metaRef) {
    dom.metaRef.textContent = `REF: ${intro.git_ref || '—'}`;
  }
  if (dom.metaBuilt) {
    dom.metaBuilt.textContent = `BUILT: ${formatUtcShort(intro.built_at)}`;
  }
  if (dom.metaFp) {
    dom.metaFp.textContent = `CFG: ${intro.config_fingerprint || '—'}`;
  }
  if (dom.footerMeta) {
    dom.footerMeta.textContent = `v${intro.package_version || '0.1.0'} · Python ${intro.python_version || '3.12'} · Schema ${state.snapshot?.schema_version || '1.0.0'}`;
  }
}

async function fetchJson(url, options = undefined) {
  const response = await fetch(url, options);
  if (response.status === 429) {
    const retryAfter = response.headers.get('Retry-After') || '5';
    let detail = 'Live benchmark queue is busy.';
    try {
      const body = await response.json();
      if (body && body.detail) {
        detail = body.detail;
      }
    } catch {
      // keep default detail
    }
    const err = new Error(`${detail} Retry after ${retryAfter}s.`);
    err.status = 429;
    throw err;
  }
  if (response.status === 504) {
    const err = new Error('Live simulation exceeded the 2.0s execution budget (HTTP 504). Please retry.');
    err.status = 504;
    throw err;
  }
  if (!response.ok) {
    const err = new Error(`Request failed with HTTP ${response.status}.`);
    err.status = response.status;
    throw err;
  }
  return response.json();
}

async function postLive(path, body = undefined) {
  const opts = {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
  };
  if (body !== undefined) {
    opts.body = JSON.stringify(body);
  }
  return fetchJson(`${apiBase()}${path}`, opts);
}

async function loadInitialData() {
  try {
    if (state.mode === 'live') {
      try {
        const [snapshot, history] = await Promise.all([
          fetchJson(`${apiBase()}/snapshot`),
          fetchJson(`${apiBase()}/history`).catch(() => []),
        ]);
        state.snapshot = snapshot;
        state.history = Array.isArray(history) ? history : [];
      } catch {
        // Fallback cleanly to static JSON bundle if live API is unavailable
        const [snapshot, history] = await Promise.all([
          fetchJson('./snapshot.json'),
          fetchJson('./history.json').catch(() => []),
        ]);
        state.mode = 'static';
        state.snapshot = snapshot;
        state.history = Array.isArray(history) ? history : [];
      }
    } else {
      const [snapshot, history] = await Promise.all([
        fetchJson('./snapshot.json'),
        fetchJson('./history.json').catch(() => []),
      ]);
      state.snapshot = snapshot;
      state.history = Array.isArray(history) ? history : [];
    }

    state.activePreset = state.snapshot?.default_preset || 'micro';
    const hpSteps = state.snapshot?.scenarios?.hot_path?.steps || [];
    state.hotPathStep = hpSteps.length > 0 ? hpSteps.length : 1;

    updateProvenanceHeader();
    renderActiveTab();
  } catch (err) {
    if (dom.appContent) {
      dom.appContent.innerHTML = `
        <section class="panel loading-panel">
          <p class="loading-title text-danger">Unable to load showcase snapshot</p>
          <p class="muted">${escapeHtml(err?.message || 'Unknown error loading telemetry data.')}</p>
        </section>
      `;
    }
    showStatus('Failed to initialize telemetry snapshot.', 'danger');
  }
}

function setActiveTab(tabId) {
  if (!TAB_ORDER.includes(tabId)) {
    return;
  }
  state.activeTab = tabId;
  const buttons = dom.tablist ? dom.tablist.querySelectorAll('[role="tab"]') : [];
  for (const btn of buttons) {
    const selected = btn.dataset.tab === tabId;
    btn.classList.toggle('is-active', selected);
    btn.setAttribute('aria-selected', selected ? 'true' : 'false');
    btn.setAttribute('tabindex', selected ? '0' : '-1');
  }
  if (dom.appContent) {
    dom.appContent.setAttribute('aria-labelledby', `tab-${tabId}`);
  }
  renderActiveTab();
}

function renderActiveTab() {
  if (!dom.appContent || !state.snapshot) {
    return;
  }
  let html = '';
  switch (state.activeTab) {
    case 'system-map':
      html = renderSystemMap(state.snapshot, state);
      break;
    case 'load-escalator':
      html = renderLoadEscalator(state.snapshot, state);
      break;
    case 'adapter-switchboard':
      html = renderAdapterSwitchboard(state.snapshot, state);
      break;
    case 'hybrid-rag':
      html = renderHybridRag(state.snapshot, state);
      break;
    case 'commit-trends':
      html = renderCommitTrends(state.snapshot, state.history);
      break;
    default:
      html = renderSystemMap(state.snapshot, state);
  }
  dom.appContent.innerHTML = html;
}

async function handleRunRamp() {
  const seedInput = document.getElementById('ramp-seed-input');
  const parsedSeed = seedInput ? Number.parseInt(seedInput.value, 10) : state.rampSeed;
  const seed = Number.isFinite(parsedSeed)
    ? Math.max(1, Math.min(10000, parsedSeed))
    : 42;
  state.rampSeed = seed;

  if (state.mode === 'static') {
    const hasPreset = Boolean(state.snapshot?.benchmarks?.[state.activePreset]);
    if (hasPreset) {
      showStatus(
        `Static replay: loaded pre-computed ${state.activePreset.toUpperCase()} 7-workload benchmark bundle (zero network calls).`,
        'ok'
      );
    } else {
      const fallback = state.snapshot?.default_preset || 'micro';
      state.activePreset = fallback;
      showStatus(
        `Static bundle includes pre-computed ${fallback.toUpperCase()} preset. Run local FastAPI server for on-demand ramps.`,
        'info'
      );
    }
    renderActiveTab();
    return;
  }

  const preset = state.activePreset;
  state.busyAction = 'ramp';
  renderActiveTab();
  showStatus(
    `Executing coalesced ${preset.toUpperCase()} benchmark ramp (seed=${seed})…`,
    'info'
  );
  try {
    const res = await postLive('/ramp/run', {
      preset,
      adapters: ['in_memory', 'iceberg_puppygraph'],
      seed,
    });
    if (!state.snapshot.benchmarks) {
      state.snapshot.benchmarks = {};
    }
    state.snapshot.benchmarks[preset] = res.bundle;
    state.lastRampMeta = {
      cache_hit: Boolean(res.cache_hit),
      coalesced: Boolean(res.coalesced),
    };
    const modeNote = res.cache_hit
      ? 'served from 300s TTL cache'
      : res.coalesced
        ? 'coalesced onto active single-flight job'
        : 'freshly computed';
    showStatus(
      `Ramp complete for ${preset.toUpperCase()} (${modeNote}). Winner: ${res.bundle?.winner_adapter || '—'}.`,
      'ok'
    );
  } catch (err) {
    showStatus(err.message || 'Ramp execution failed.', err.status === 429 || err.status === 504 ? 'warn' : 'danger');
  } finally {
    state.busyAction = null;
    renderActiveTab();
  }
}

async function handleRunGuardrail(probeId) {
  state.selectedProbe = probeId;
  if (state.mode === 'static') {
    const probe = state.snapshot?.scenarios?.guardrail_probes?.[probeId];
    if (probe) {
      showStatus(
        `Static probe replay (${probe.probe_id}): HTTP ${probe.http_status} · ${probe.summary}`,
        probe.blocked ? 'warn' : 'info'
      );
    }
    renderActiveTab();
    return;
  }

  state.busyAction = 'guardrail';
  renderActiveTab();
  showStatus(`Executing live guardrail stress probe: ${probeId}…`, 'info');
  try {
    const res = await postLive('/simulate/guardrail', { probe_id: probeId });
    state.liveGuardrails[probeId] = res;
    showStatus(
      `Live probe ${res.probe_id}: HTTP ${res.http_status} (blocked=${res.blocked}, aborted=${res.aborted}).`,
      res.blocked ? 'warn' : 'ok'
    );
  } catch (err) {
    showStatus(err.message || 'Guardrail probe failed.', err.status === 504 ? 'warn' : 'danger');
  } finally {
    state.busyAction = null;
    renderActiveTab();
  }
}

async function handleHotPathRerun() {
  const threshold = state.snapshot?.introspection?.hot_path_threshold || 3;
  if (state.mode === 'static') {
    const steps = state.snapshot?.scenarios?.hot_path?.steps || [];
    state.hotPathStep = steps.length || 1;
    showStatus(
      `Replayed deterministic ${steps.length}-hit derived-edge promotion sequence (promoted at hit #${state.snapshot?.scenarios?.hot_path?.promoted_at_hit ?? threshold}).`,
      'ok'
    );
    renderActiveTab();
    return;
  }

  state.busyAction = 'hotpath';
  renderActiveTab();
  showStatus(`Running live derived-edge hot-path promotion (threshold=${threshold})…`, 'info');
  try {
    const res = await postLive('/simulate/hot-path', {
      hits: Math.max(4, threshold + 1),
      threshold,
    });
    state.liveHotPath = res;
    state.hotPathStep = (res.steps || []).length || 1;
    showStatus(
      `Live hot-path trace complete: ${res.derived_edge_type} auto-promoted at hit #${res.promoted_at_hit ?? threshold}.`,
      'ok'
    );
  } catch (err) {
    showStatus(err.message || 'Hot-path simulation failed.', err.status === 504 ? 'warn' : 'danger');
  } finally {
    state.busyAction = null;
    renderActiveTab();
  }
}

async function handleCompareAdapters() {
  const selectEl = document.getElementById('golden-query-select');
  const queryId = selectEl ? selectEl.value : state.selectedGoldenQuery;
  state.selectedGoldenQuery = queryId;

  if (state.mode === 'static') {
    state.liveAdapterSwitch = null;
    showStatus(
      `Static parity replay: showing pre-computed cross-adapter results for ${queryId}.`,
      'ok'
    );
    renderActiveTab();
    return;
  }

  state.busyAction = 'compare';
  renderActiveTab();
  showStatus(`Running live Golden Query ${queryId} across in_memory and iceberg_puppygraph…`, 'info');
  try {
    const res = await postLive('/simulate/adapter-switch', {
      query_id: queryId,
      adapters: ['in_memory', 'iceberg_puppygraph'],
    });
    state.liveAdapterSwitch = res;
    showStatus(
      `Live comparison ${res.query_id}: ${res.parity_matched ? '100% PARITY MATCHED' : 'MISMATCH'} across ${(res.results || []).length} adapters.`,
      res.parity_matched ? 'ok' : 'danger'
    );
  } catch (err) {
    showStatus(err.message || 'Adapter comparison failed.', err.status === 504 ? 'warn' : 'danger');
  } finally {
    state.busyAction = null;
    renderActiveTab();
  }
}

async function handleRerunHybrid() {
  if (state.mode === 'static') {
    const rag = state.snapshot?.scenarios?.hybrid_rag;
    showStatus(
      `Static trace replay: ${rag?.hydrated_documents ?? 0}/${rag?.total_lakehouse_documents ?? 0} Parquet docs hydrated (${rag?.payload_savings_pct ?? 0}% payload savings).`,
      'ok'
    );
    renderActiveTab();
    return;
  }

  state.busyAction = 'hybrid';
  renderActiveTab();
  showStatus('Running live 5-stage Seed-Then-Expand Hybrid GraphRAG trace…', 'info');
  try {
    const res = await postLive('/simulate/hybrid');
    state.liveHybridRag = res;
    showStatus(
      `Live Hybrid GraphRAG trace complete: ${res.hydrated_documents}/${res.total_lakehouse_documents} docs hydrated (${res.payload_savings_pct}% byte savings).`,
      'ok'
    );
  } catch (err) {
    showStatus(err.message || 'Hybrid GraphRAG trace failed.', err.status === 504 ? 'warn' : 'danger');
  } finally {
    state.busyAction = null;
    renderActiveTab();
  }
}

function wireEvents() {
  if (dom.tablist) {
    dom.tablist.addEventListener('click', (event) => {
      const btn = event.target.closest('[role="tab"]');
      if (!btn || !btn.dataset.tab) {
        return;
      }
      clearStatus();
      setActiveTab(btn.dataset.tab);
    });

    dom.tablist.addEventListener('keydown', (event) => {
      const btn = event.target.closest('[role="tab"]');
      if (!btn) {
        return;
      }
      const idx = TAB_ORDER.indexOf(btn.dataset.tab);
      if (idx < 0) {
        return;
      }
      let nextIdx = null;
      if (event.key === 'ArrowRight') {
        nextIdx = (idx + 1) % TAB_ORDER.length;
      } else if (event.key === 'ArrowLeft') {
        nextIdx = (idx - 1 + TAB_ORDER.length) % TAB_ORDER.length;
      } else if (event.key === 'Home') {
        nextIdx = 0;
      } else if (event.key === 'End') {
        nextIdx = TAB_ORDER.length - 1;
      }
      if (nextIdx !== null) {
        event.preventDefault();
        const nextTab = TAB_ORDER[nextIdx];
        clearStatus();
        setActiveTab(nextTab);
        const nextBtn = document.getElementById(`tab-${nextTab}`);
        if (nextBtn) {
          nextBtn.focus();
        }
      }
    });
  }

  if (dom.appContent) {
    dom.appContent.addEventListener('click', (event) => {
      const target = event.target.closest('[data-action]');
      if (!target) {
        return;
      }
      const action = target.dataset.action;

      if (action === 'select-layer') {
        state.selectedLayer = target.dataset.layer || 'layer-2';
        renderActiveTab();
        return;
      }

      if (action === 'select-preset') {
        const chosen = target.dataset.preset || 'micro';
        state.activePreset = chosen;
        if (!state.snapshot?.benchmarks?.[chosen]) {
          if (state.mode === 'live') {
            handleRunRamp();
          } else {
            showStatus(
              `Preset ${chosen.toUpperCase()} is not bundled in this static snapshot; displaying available pre-computed data.`,
              'info'
            );
            renderActiveTab();
          }
          return;
        }
        showStatus(`Switched active benchmark view to ${chosen.toUpperCase()} preset.`, 'info');
        renderActiveTab();
        return;
      }

      if (action === 'run-ramp') {
        handleRunRamp();
        return;
      }

      if (action === 'hotpath-next') {
        const hp = state.liveHotPath || state.snapshot?.scenarios?.hot_path || {};
        const maxSteps = Math.max(1, (hp.steps || []).length);
        state.hotPathStep = state.hotPathStep >= maxSteps ? 1 : state.hotPathStep + 1;
        renderActiveTab();
        return;
      }

      if (action === 'hotpath-reset') {
        state.hotPathStep = 1;
        renderActiveTab();
        return;
      }

      if (action === 'hotpath-rerun') {
        handleHotPathRerun();
        return;
      }

      if (action === 'run-guardrail') {
        const probeId = target.dataset.probe || 'forbidden_payload';
        handleRunGuardrail(probeId);
        return;
      }

      if (action === 'compare-adapters') {
        handleCompareAdapters();
        return;
      }

      if (action === 'select-bench-adapter') {
        state.selectedBenchAdapter = target.dataset.adapter || null;
        renderActiveTab();
        return;
      }

      if (action === 'select-scale') {
        state.scaleTarget = target.dataset.scale || '1B';
        showStatus(
          `Updated Gate 2 latency & infrastructure cost projections to ${state.scaleTarget} edge scale target.`,
          'info'
        );
        renderActiveTab();
        return;
      }

      if (action === 'rerun-hybrid') {
        handleRerunHybrid();
      }
    });

    dom.appContent.addEventListener('change', (event) => {
      if (event.target && event.target.id === 'golden-query-select') {
        state.selectedGoldenQuery = event.target.value;
        state.liveAdapterSwitch = null;
        renderActiveTab();
      } else if (event.target && event.target.id === 'ramp-seed-input') {
        const parsed = Number.parseInt(event.target.value, 10);
        state.rampSeed = Number.isFinite(parsed) ? Math.max(1, Math.min(10000, parsed)) : 42;
        event.target.value = String(state.rampSeed);
      }
    });

    dom.appContent.addEventListener('input', (event) => {
      if (event.target && event.target.id === 'ramp-seed-input') {
        const parsed = Number.parseInt(event.target.value, 10);
        if (Number.isFinite(parsed)) {
          state.rampSeed = Math.max(1, Math.min(10000, parsed));
        }
      }
    });
  }
}

wireEvents();
loadInitialData();
