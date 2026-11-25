// Zero-dependency inline SVG chart generators (strict CSP & XSS safe).

const MONO_FONT = 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace';
const SANS_FONT = "ui-sans-serif, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";

export function escapeHtml(value) {
  if (value === null || value === undefined) {
    return '';
  }
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

export function safeNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(val, min, max) {
  return Math.max(min, Math.min(max, val));
}

function fmtAxis(val) {
  const n = safeNum(val, 0);
  if (n >= 1000) {
    return `${(n / 1000).toFixed(1)}k`;
  }
  if (n >= 100) {
    return n.toFixed(0);
  }
  if (n >= 10) {
    return n.toFixed(1);
  }
  return n.toFixed(2);
}

/**
 * Grouped 1/2/3-hop P50/P95/P99 latency bar chart with dashed Gate 2 SLA markers.
 */
export function renderLatencyBarChart(byDepth = {}, budgets = {}) {
  const depths = ['1', '2', '3'];
  const defaultBudgets = {
    '1': safeNum(budgets.hop_1_p95_ms ?? budgets['1'], 25),
    '2': safeNum(budgets.hop_2_p95_ms ?? budgets['2'], 100),
    '3': safeNum(budgets.hop_3_p95_ms ?? budgets['3'], 300),
  };

  const rows = depths.map((d) => {
    const stat = byDepth[d] || byDepth[Number(d)] || {};
    const budget = safeNum(stat.budget_p95_ms, defaultBudgets[d]);
    return {
      depth: d,
      label: `${d}-Hop`,
      p50: Math.max(0, safeNum(stat.p50_ms, 0)),
      p95: Math.max(0, safeNum(stat.p95_ms, 0)),
      p99: Math.max(0, safeNum(stat.p99_ms, 0)),
      budget,
      within: stat.within_budget !== false,
    };
  });

  const maxObserved = Math.max(
    1,
    ...rows.flatMap((r) => [r.p50, r.p95, r.p99])
  );
  // Use a clear scale: show observed bars legibly while keeping budget markers visible.
  const maxBudget = Math.max(...rows.map((r) => r.budget));
  const useLogHint = maxObserved * 8 < maxBudget;
  const yMax = useLogHint ? Math.max(maxObserved * 1.6, 5) : Math.max(maxObserved, maxBudget) * 1.15;

  const width = 620;
  const height = 250;
  const pad = { top: 32, right: 20, bottom: 44, left: 56 };
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;

  const yFor = (val) => {
    const ratio = clamp(safeNum(val, 0) / yMax, 0, 1);
    return pad.top + plotH - ratio * plotH;
  };

  const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => yMax * f);
  const gridLines = ticks
    .map((val) => {
      const y = yFor(val).toFixed(1);
      return `
        <line x1="${pad.left}" y1="${y}" x2="${width - pad.right}" y2="${y}" stroke="#22324a" stroke-width="1" />
        <text x="${pad.left - 8}" y="${(Number(y) + 4).toFixed(1)}" text-anchor="end" fill="#8ea3be" font-size="10" font-family="${MONO_FONT}">${escapeHtml(fmtAxis(val))}</text>
      `;
    })
    .join('');

  const groupW = plotW / rows.length;
  const barW = 24;
  const gap = 8;

  const groupsSvg = rows
    .map((row, idx) => {
      const gx = pad.left + idx * groupW;
      const center = gx + groupW / 2;
      const startX = center - (barW * 3 + gap * 2) / 2;

      const bars = [
        { key: 'P50', val: row.p50, fill: '#38bdf8' },
        { key: 'P95', val: row.p95, fill: '#818cf8' },
        { key: 'P99', val: row.p99, fill: '#f59e0b' },
      ];

      const barsMarkup = bars
        .map((b, bIdx) => {
          const bx = startX + bIdx * (barW + gap);
          const by = yFor(b.val);
          const bh = Math.max(2, pad.top + plotH - by);
          return `
            <rect x="${bx.toFixed(1)}" y="${by.toFixed(1)}" width="${barW}" height="${bh.toFixed(1)}" rx="3" fill="${escapeHtml(b.fill)}" />
            <text x="${(bx + barW / 2).toFixed(1)}" y="${Math.max(pad.top + 10, by - 5).toFixed(1)}" text-anchor="middle" fill="#dbe6f4" font-size="9.5" font-family="${MONO_FONT}">${escapeHtml(fmtAxis(b.val))}</text>
          `;
        })
        .join('');

      const budgetInRange = row.budget <= yMax;
      const budgetY = budgetInRange ? yFor(row.budget) : pad.top + 4;
      const slaLine = budgetInRange
        ? `<line x1="${(gx + 14).toFixed(1)}" y1="${budgetY.toFixed(1)}" x2="${(gx + groupW - 14).toFixed(1)}" y2="${budgetY.toFixed(1)}" stroke="#34d399" stroke-width="1.6" stroke-dasharray="5 4" />
           <text x="${(gx + groupW - 14).toFixed(1)}" y="${Math.max(pad.top + 10, budgetY - 4).toFixed(1)}" text-anchor="end" fill="#34d399" font-size="9.5" font-family="${MONO_FONT}">SLA ${escapeHtml(row.budget)}ms</text>`
        : `<rect x="${(center - 46).toFixed(1)}" y="${(pad.top + 2).toFixed(1)}" width="92" height="16" rx="4" fill="#063529" stroke="#34d399" stroke-width="0.8" />
           <text x="${center.toFixed(1)}" y="${(pad.top + 13).toFixed(1)}" text-anchor="middle" fill="#34d399" font-size="9.5" font-family="${MONO_FONT}">SLA ≤ ${escapeHtml(row.budget)}ms ✓</text>`;

      return `
        ${barsMarkup}
        ${slaLine}
        <text x="${center.toFixed(1)}" y="${height - 20}" text-anchor="middle" fill="#eef3f9" font-size="11.5" font-weight="700" font-family="${SANS_FONT}">${escapeHtml(row.label)}</text>
        <text x="${center.toFixed(1)}" y="${height - 7}" text-anchor="middle" fill="${row.within ? '#34d399' : '#fb7185'}" font-size="9.5" font-family="${MONO_FONT}">${row.within ? 'WITHIN BUDGET' : 'OVER BUDGET'}</text>
      `;
    })
    .join('');

  return `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="1, 2, and 3-hop latency distribution vs Gate 2 SLA budgets">
      <title>1, 2, and 3-hop P50, P95, and P99 latency (ms) vs Gate 2 SLA budgets</title>
      ${gridLines}
      <line x1="${pad.left}" y1="${pad.top}" x2="${pad.left}" y2="${pad.top + plotH}" stroke="#344a6b" stroke-width="1" />
      <line x1="${pad.left}" y1="${pad.top + plotH}" x2="${width - pad.right}" y2="${pad.top + plotH}" stroke="#344a6b" stroke-width="1" />
      ${groupsSvg}
      <g>
        <rect x="${pad.left}" y="8" width="10" height="10" rx="2" fill="#38bdf8" />
        <text x="${pad.left + 14}" y="17" fill="#c7d4e5" font-size="10.5" font-family="${SANS_FONT}">P50</text>
        <rect x="${pad.left + 52}" y="8" width="10" height="10" rx="2" fill="#818cf8" />
        <text x="${pad.left + 66}" y="17" fill="#c7d4e5" font-size="10.5" font-family="${SANS_FONT}">P95</text>
        <rect x="${pad.left + 104}" y="8" width="10" height="10" rx="2" fill="#f59e0b" />
        <text x="${pad.left + 118}" y="17" fill="#c7d4e5" font-size="10.5" font-family="${SANS_FONT}">P99</text>
        <line x1="${pad.left + 158}" y1="13" x2="${pad.left + 176}" y2="13" stroke="#34d399" stroke-width="2" stroke-dasharray="4 3" />
        <text x="${pad.left + 182}" y="17" fill="#34d399" font-size="10.5" font-family="${SANS_FONT}">Gate 2 P95 SLA (ms)</text>
      </g>
    </svg>
  `;
}

/**
 * Responsive SVG line chart with optional vertical switch-threshold annotation.
 */
export function renderLineChart(
  points = [],
  {
    xKey = 'x',
    yKey = 'y',
    thresholdX = null,
    thresholdLabel = '16-way bucketing activates',
    title = 'Metric trend',
    yUnit = 'ms',
    xLabel = '',
    color = '#38bdf8',
  } = {}
) {
  const safePoints = Array.isArray(points) ? points : [];
  const width = 620;
  const height = 235;
  const pad = { top: 30, right: 24, bottom: 42, left: 58 };
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;

  if (safePoints.length === 0) {
    return `
      <svg class="chart-svg" viewBox="0 0 ${width} 120" role="img" aria-label="${escapeHtml(title)}">
        <title>${escapeHtml(title)}</title>
        <text x="${width / 2}" y="64" text-anchor="middle" fill="#8ea3be" font-size="12" font-family="${SANS_FONT}">No measurements available.</text>
      </svg>
    `;
  }

  const parsed = safePoints.map((pt, index) => ({
    xRaw: safeNum(pt[xKey], index),
    yVal: Math.max(0, safeNum(pt[yKey], 0)),
    label: pt.label !== undefined ? String(pt.label) : String(pt[xKey] ?? index),
    bucketed: Boolean(pt.bucketed),
  }));

  const yMaxRaw = Math.max(...parsed.map((p) => p.yVal), 0.1);
  const yMax = yMaxRaw * 1.22;

  const xForIndex = (idx) => {
    if (parsed.length === 1) {
      return pad.left + plotW / 2;
    }
    return pad.left + (idx / (parsed.length - 1)) * plotW;
  };
  const yFor = (val) => pad.top + plotH - clamp(val / yMax, 0, 1) * plotH;

  const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => yMax * f);
  const grid = ticks
    .map((val) => {
      const y = yFor(val).toFixed(1);
      return `
        <line x1="${pad.left}" y1="${y}" x2="${width - pad.right}" y2="${y}" stroke="#22324a" stroke-width="1" />
        <text x="${pad.left - 8}" y="${(Number(y) + 4).toFixed(1)}" text-anchor="end" fill="#8ea3be" font-size="10" font-family="${MONO_FONT}">${escapeHtml(fmtAxis(val))}</text>
      `;
    })
    .join('');

  const coords = parsed.map((p, idx) => ({
    x: xForIndex(idx),
    y: yFor(p.yVal),
    ...p,
  }));

  const pathD = coords
    .map((c, idx) => `${idx === 0 ? 'M' : 'L'} ${c.x.toFixed(1)} ${c.y.toFixed(1)}`)
    .join(' ');
  const areaD = `${pathD} L ${coords[coords.length - 1].x.toFixed(1)} ${(pad.top + plotH).toFixed(1)} L ${coords[0].x.toFixed(1)} ${(pad.top + plotH).toFixed(1)} Z`;

  let thresholdSvg = '';
  if (thresholdX !== null && thresholdX !== undefined) {
    const tVal = safeNum(thresholdX, NaN);
    if (Number.isFinite(tVal)) {
      let tx = null;
      const exactIdx = parsed.findIndex((p) => p.xRaw >= tVal);
      if (exactIdx >= 0) {
        tx = xForIndex(exactIdx);
      }
      if (tx !== null) {
        const alignLeft = tx > width - 190;
        const textAnchor = alignLeft ? 'end' : 'start';
        const labelX = alignLeft ? tx - 6 : tx + 6;
        thresholdSvg = `
          <line x1="${tx.toFixed(1)}" y1="${pad.top}" x2="${tx.toFixed(1)}" y2="${pad.top + plotH}" stroke="#34d399" stroke-width="1.8" stroke-dasharray="5 4" />
          <text x="${labelX.toFixed(1)}" y="${pad.top + 12}" text-anchor="${textAnchor}" fill="#34d399" font-size="10" font-weight="700" font-family="${SANS_FONT}">${escapeHtml(thresholdLabel)}</text>
        `;
      }
    }
  }

  const dotsSvg = coords
    .map((c) => {
      const fill = c.bucketed ? '#34d399' : color;
      return `
        <circle cx="${c.x.toFixed(1)}" cy="${c.y.toFixed(1)}" r="4.5" fill="${escapeHtml(fill)}" stroke="#0a0f17" stroke-width="1.5" />
        <text x="${c.x.toFixed(1)}" y="${Math.max(pad.top + 10, c.y - 9).toFixed(1)}" text-anchor="middle" fill="#eef3f9" font-size="9.5" font-family="${MONO_FONT}">${escapeHtml(fmtAxis(c.yVal))}</text>
        <text x="${c.x.toFixed(1)}" y="${height - 20}" text-anchor="middle" fill="#c7d4e5" font-size="10.5" font-family="${MONO_FONT}">${escapeHtml(c.label)}</text>
      `;
    })
    .join('');

  return `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(title)}">
      <title>${escapeHtml(title)}</title>
      ${grid}
      <line x1="${pad.left}" y1="${pad.top}" x2="${pad.left}" y2="${pad.top + plotH}" stroke="#344a6b" stroke-width="1" />
      <line x1="${pad.left}" y1="${pad.top + plotH}" x2="${width - pad.right}" y2="${pad.top + plotH}" stroke="#344a6b" stroke-width="1" />
      ${thresholdSvg}
      <path d="${areaD}" fill="${escapeHtml(color)}" opacity="0.12" />
      <path d="${pathD}" fill="none" stroke="${escapeHtml(color)}" stroke-width="2.4" stroke-linejoin="round" stroke-linecap="round" />
      ${dotsSvg}
      <text x="${pad.left}" y="15" fill="#c7d4e5" font-size="11" font-weight="700" font-family="${SANS_FONT}">${escapeHtml(title)} (${escapeHtml(yUnit)})</text>
      ${xLabel ? `<text x="${(pad.left + plotW / 2).toFixed(1)}" y="${height - 5}" text-anchor="middle" fill="#8ea3be" font-size="10" font-family="${SANS_FONT}">${escapeHtml(xLabel)}</text>` : ''}
    </svg>
  `;
}

/**
 * 5-stage Hybrid GraphRAG latency waterfall chart.
 */
export function renderWaterfallChart(stageTimingsMs = {}) {
  const stageMeta = [
    { key: 'vector_seed', label: '1. Vector Seed', color: '#38bdf8' },
    { key: 'graph_expand', label: '2. Graph Expand', color: '#818cf8' },
    { key: 'rrf_fusion', label: '3. RRF Fusion', color: '#a78bfa' },
    { key: 'lakehouse_hydrate', label: '4. Lakehouse Hydrate', color: '#34d399' },
    { key: 'context_assembly', label: '5. Context Assembly', color: '#fbbf24' },
  ];

  const rows = stageMeta.map((m) => ({
    ...m,
    ms: Math.max(0, safeNum(stageTimingsMs[m.key], 0)),
  }));
  const totalMs = Math.max(
    0.001,
    rows.reduce((sum, r) => sum + r.ms, 0)
  );

  const width = 640;
  const rowH = 34;
  const pad = { top: 26, right: 105, bottom: 26, left: 162 };
  const height = pad.top + pad.bottom + rows.length * rowH;
  const plotW = width - pad.left - pad.right;

  let cursor = 0;
  const barsSvg = rows
    .map((r, idx) => {
      const y = pad.top + idx * rowH;
      const startX = pad.left + (cursor / totalMs) * plotW;
      const segW = Math.max(4, (r.ms / totalMs) * plotW);
      const pct = ((r.ms / totalMs) * 100).toFixed(1);
      cursor += r.ms;
      return `
        <line x1="${pad.left}" y1="${y + rowH - 2}" x2="${width - pad.right}" y2="${y + rowH - 2}" stroke="#1d2c42" stroke-width="1" />
        <text x="${pad.left - 10}" y="${y + 20}" text-anchor="end" fill="#dbe6f4" font-size="11.5" font-weight="600" font-family="${SANS_FONT}">${escapeHtml(r.label)}</text>
        <rect x="${startX.toFixed(1)}" y="${y + 7}" width="${segW.toFixed(1)}" height="18" rx="4" fill="${escapeHtml(r.color)}" />
        <text x="${width - pad.right + 8}" y="${y + 20}" text-anchor="start" fill="#c7d4e5" font-size="10.5" font-family="${MONO_FONT}">${escapeHtml(r.ms.toFixed(2))}ms (${escapeHtml(pct)}%)</text>
      `;
    })
    .join('');

  return `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="5-stage Hybrid GraphRAG latency waterfall">
      <title>5-Stage Seed-Then-Expand Hybrid GraphRAG latency waterfall (${escapeHtml(totalMs.toFixed(2))} ms total)</title>
      <text x="${pad.left}" y="16" fill="#8ea3be" font-size="10.5" font-family="${MONO_FONT}">Cumulative Pipeline Execution — Total: ${escapeHtml(totalMs.toFixed(2))} ms</text>
      <line x1="${pad.left}" y1="${pad.top}" x2="${pad.left}" y2="${height - pad.bottom}" stroke="#344a6b" stroke-width="1" />
      ${barsSvg}
    </svg>
  `;
}

/**
 * Compact multi-commit trend SVG sparkline.
 */
export function renderSparkline(
  values = [],
  { label = 'Trend', unit = '', color = '#38bdf8' } = {}
) {
  const nums = (Array.isArray(values) ? values : []).map((v) => safeNum(v, 0));
  const width = 220;
  const height = 56;
  const padX = 8;
  const padY = 8;

  if (nums.length === 0) {
    return `
      <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(label)} sparkline">
        <title>${escapeHtml(label)} sparkline (no data)</title>
        <line x1="${padX}" y1="${height / 2}" x2="${width - padX}" y2="${height / 2}" stroke="#334155" stroke-dasharray="3 3" />
      </svg>
    `;
  }

  const minVal = Math.min(...nums);
  const maxVal = Math.max(...nums);
  const span = maxVal - minVal || 1;

  const pts = nums.map((val, idx) => {
    const x =
      nums.length === 1
        ? width / 2
        : padX + (idx / (nums.length - 1)) * (width - padX * 2);
    const y =
      nums.length === 1
        ? height / 2
        : padY + (1 - (val - minVal) / span) * (height - padY * 2);
    return { x, y, val };
  });

  const d = pts
    .map((p, idx) => `${idx === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`)
    .join(' ');
  const last = pts[pts.length - 1];
  const latestText = `${fmtAxis(last.val)}${unit ? ` ${unit}` : ''}`;

  return `
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(label)}: ${escapeHtml(latestText)}">
      <title>${escapeHtml(label)} trend — latest ${escapeHtml(latestText)}</title>
      <path d="${d}" fill="none" stroke="${escapeHtml(color)}" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" />
      ${pts.map((p) => `<circle cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="2.8" fill="${escapeHtml(color)}" />`).join('')}
      <circle cx="${last.x.toFixed(1)}" cy="${last.y.toFixed(1)}" r="4.2" fill="${escapeHtml(color)}" stroke="#0a0f17" stroke-width="1.5" />
    </svg>
  `;
}

/**
 * CSP-safe horizontal SVG bar meter (avoids inline styling attributes).
 */
export function renderMeterBar(percent, { color = '#38bdf8', label = 'Progress' } = {}) {
  const clamped = clamp(safeNum(percent, 0), 0, 100);
  return `
    <svg class="bar-meter-svg" viewBox="0 0 100 10" preserveAspectRatio="none" role="img" aria-label="${escapeHtml(label)}: ${escapeHtml(clamped.toFixed(1))}%">
      <title>${escapeHtml(label)}: ${escapeHtml(clamped.toFixed(1))}%</title>
      <rect x="0" y="0" width="100" height="10" rx="5" fill="#1b293d" />
      <rect x="0" y="0" width="${clamped.toFixed(1)}" height="10" rx="5" fill="${escapeHtml(color)}" />
    </svg>
  `;
}
