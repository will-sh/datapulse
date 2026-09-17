(function () {
  "use strict";

  const REFRESH_MS = 5000;
  let timer = null;
  let cachedSessions = [];
  let cachedSessionSummary = {};

  const els = {
    statusBar: document.getElementById("status-bar"),
    kpiGrid: document.getElementById("kpi-grid"),
    funnelPanel: document.getElementById("funnel-panel"),
    retentionPanel: document.getElementById("retention-panel"),
    sessionsPanel: document.getElementById("sessions-panel"),
    sessionFilter: document.getElementById("session-filter"),
    sessionCountLabel: document.getElementById("session-count-label"),
    convertersPanel: document.getElementById("converters-panel"),
    pagesPanel: document.getElementById("pages-panel"),
    eventsPanel: document.getElementById("events-panel"),
    autoRefresh: document.getElementById("auto-refresh"),
  };

  function fmtPct(value) {
    return `${Number(value || 0).toFixed(1)}%`;
  }

  function fmtTime(seconds) {
    if (!seconds) return "-";
    return new Date(seconds * 1000).toLocaleString("en-US");
  }

  function renderStatus(pipeline, note) {
    const activeClass = pipeline.stream_active ? "ok" : "err";
    els.statusBar.innerHTML = `
      <div>Stream: <span class="${activeClass}">${pipeline.spark_status}</span></div>
      <div>Buffer: ${pipeline.buffer_used}/${pipeline.buffer_size}</div>
      <div>Total received: <strong>${pipeline.total_received}</strong></div>
      <div>${note || ""}</div>
    `;
  }

  function renderKpis(kpis) {
    const cards = [
      ["Sessions", kpis.sessions, "Distinct session / anonymous keys"],
      ["Form submissions", kpis.form_submissions, "Lead + demo forms"],
      ["Conversion rate", fmtPct(kpis.conversion_rate_pct), "Form submit / pageview sessions"],
      ["Identified users", kpis.identified_users, "After identify() or user_id"],
      ["Marketplace engagement", fmtPct(kpis.marketplace_engagement_rate_pct), "Marketplace events / pageviews"],
      ["Multi-page sessions", fmtPct(kpis.multi_page_session_rate_pct), "2+ unique paths in session"],
    ];

    els.kpiGrid.innerHTML = cards
      .map(
        ([label, value, sub]) => `
        <article class="kpi-card">
          <p class="kpi-label">${label}</p>
          <p class="kpi-value">${value}</p>
          <p class="kpi-sub">${sub}</p>
        </article>`,
      )
      .join("");
  }

  function renderFunnel(funnel) {
    if (!funnel.length) {
      els.funnelPanel.innerHTML = '<p class="empty">No funnel data yet</p>';
      return;
    }

    const maxSessions = Math.max(...funnel.map((step) => step.sessions), 1);
    els.funnelPanel.innerHTML = funnel
      .map(
        (step) => `
        <div class="funnel-step">
          <div class="funnel-label-row">
            <span>${step.label}</span>
            <span><strong>${step.sessions}</strong> sessions · ${fmtPct(step.rate_pct)}</span>
          </div>
          <div class="funnel-bar-track">
            <div class="funnel-bar-fill" style="width:${Math.max(4, (step.sessions / maxSessions) * 100)}%"></div>
          </div>
        </div>`,
      )
      .join("");
  }

  function renderRetention(kpis, retention) {
    els.retentionPanel.innerHTML = `
      <div class="retention-row">
        <span>Multi-page sessions</span>
        <strong>${retention.multi_page_sessions} · ${fmtPct(kpis.multi_page_session_rate_pct)}</strong>
      </div>
      <div class="retention-row">
        <span>Engaged sessions (3+ events)</span>
        <strong>${retention.engaged_sessions} · ${fmtPct(kpis.engaged_session_rate_pct)}</strong>
      </div>
      <div class="retention-row">
        <span>Identified users</span>
        <strong>${kpis.identified_users}</strong>
      </div>
      <p class="hint">${retention.description}</p>
    `;
  }

  function filterSessions(rows) {
    const mode = els.sessionFilter?.value || "all";
    return rows.filter((row) => {
      if (mode === "converted") return row.converted;
      if (mode === "not_converted") return !row.converted;
      if (mode === "anonymous") return !row.user_display;
      if (mode === "identified") return Boolean(row.user_display);
      return true;
    });
  }

  function renderSessions(rows, summary) {
    const filtered = filterSessions(rows);
    if (els.sessionCountLabel) {
      els.sessionCountLabel.textContent = `${filtered.length} shown · ${summary.total || 0} total · ${summary.converted || 0} converted · ${summary.anonymous_only || 0} anonymous`;
    }

    if (!filtered.length) {
      els.sessionsPanel.innerHTML =
        '<p class="empty">No sessions match this filter. Browse or submit a form on the Producer site, then refresh.</p>';
      return;
    }

    els.sessionsPanel.innerHTML = `
      <table class="converters-table">
        <thead>
          <tr>
            <th>Last active</th>
            <th>Session</th>
            <th>Anonymous ID</th>
            <th>User ID</th>
            <th>Status</th>
            <th>Journey</th>
            <th>Events</th>
          </tr>
        </thead>
        <tbody>
          ${filtered
            .map((row) => {
              const status = row.converted
                ? `<span class="status-badge status-badge--ok">Converted</span>`
                : row.marketplace_engaged
                  ? `<span class="status-badge status-badge--warn">Engaged</span>`
                  : `<span class="status-badge status-badge--muted">Browsing</span>`;
              return `
            <tr>
              <td>${fmtTime(row.last_activity_at)}</td>
              <td class="mono-cell" title="${row.session_id || ""}">${row.session_id_short || "-"}</td>
              <td class="mono-cell" title="${row.anonymous_id || ""}">${row.anonymous_id_short || "-"}</td>
              <td>${row.user_display || "—"}</td>
              <td>${status}</td>
              <td>${(row.pages || []).map((path) => `<span class="path-chip">${path}</span>`).join("") || "-"}</td>
              <td>${row.event_count}</td>
            </tr>`;
            })
            .join("")}
        </tbody>
      </table>
    `;
  }

  function renderConverters(rows) {
    if (!rows.length) {
      els.convertersPanel.innerHTML =
        '<p class="empty">No form submissions yet. Submit Schedule a deep dive or a Playground form on the Producer site, then refresh.</p>';
      return;
    }

    els.convertersPanel.innerHTML = `
      <table class="converters-table">
        <thead>
          <tr>
            <th>Submitted</th>
            <th>Event</th>
            <th>User</th>
            <th>Journey</th>
            <th>Events</th>
          </tr>
        </thead>
        <tbody>
          ${rows
            .map(
              (row) => `
            <tr>
              <td>${fmtTime(row.submitted_at)}</td>
              <td><code>${row.event_name}</code></td>
              <td>${row.email_masked || row.user_id || "-"}</td>
              <td>${(row.pages || []).map((path) => `<span class="path-chip">${path}</span>`).join("") || "-"}</td>
              <td>${row.event_count}</td>
            </tr>`,
            )
            .join("")}
        </tbody>
      </table>
    `;
  }

  function renderSimpleStats(target, rows, labelKey, valueKey, emptyText) {
    if (!rows.length) {
      target.innerHTML = `<p class="empty">${emptyText}</p>`;
      return;
    }
    target.innerHTML = rows
      .map(
        (row) => `
        <div class="stat-row">
          <span>${row[labelKey]}</span>
          <strong>${row[valueKey]}</strong>
        </div>`,
      )
      .join("");
  }

  async function refresh() {
    try {
      const response = await fetch("/api/insights/summary");
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      renderStatus(data.pipeline, `Window: last ${data.window_seconds}s · ${data.note}`);
      renderKpis(data.kpis);
      renderFunnel(data.funnel);
      renderRetention(data.kpis, data.retention);
      cachedSessions = data.all_sessions || [];
      cachedSessionSummary = data.session_summary || {};
      renderSessions(cachedSessions, cachedSessionSummary);
      renderConverters(data.recent_converters);
      renderSimpleStats(els.pagesPanel, data.top_pages, "path", "views", "No page views yet");
      renderSimpleStats(els.eventsPanel, data.top_events, "name", "count", "No events yet");
    } catch (error) {
      els.statusBar.innerHTML = `<span class="err">Load failed: ${error.message}</span>`;
    }
  }

  function schedule() {
    if (timer) clearInterval(timer);
    if (els.autoRefresh.checked) {
      timer = setInterval(refresh, REFRESH_MS);
    }
  }

  els.autoRefresh.addEventListener("change", schedule);
  els.sessionFilter?.addEventListener("change", () => {
    renderSessions(cachedSessions, cachedSessionSummary);
  });
  refresh();
  schedule();
})();
