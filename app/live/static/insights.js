(function () {
  "use strict";

  const REFRESH_MS = 5000;
  let timer = null;

  const els = {
    statusBar: document.getElementById("status-bar"),
    kpiGrid: document.getElementById("kpi-grid"),
    funnelPanel: document.getElementById("funnel-panel"),
    retentionPanel: document.getElementById("retention-panel"),
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
    return new Date(seconds * 1000).toLocaleString("zh-CN");
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
      els.funnelPanel.innerHTML = '<p class="empty">暂无漏斗数据</p>';
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

  function renderConverters(rows) {
    if (!rows.length) {
      els.convertersPanel.innerHTML =
        '<p class="empty">暂无表单提交。在 Producer 站点提交 Schedule a deep dive 或 Playground 表单后刷新。</p>';
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
      renderConverters(data.recent_converters);
      renderSimpleStats(els.pagesPanel, data.top_pages, "path", "views", "暂无页面浏览");
      renderSimpleStats(els.eventsPanel, data.top_events, "name", "count", "暂无事件");
    } catch (error) {
      els.statusBar.innerHTML = `<span class="err">加载失败: ${error.message}</span>`;
    }
  }

  function schedule() {
    if (timer) clearInterval(timer);
    if (els.autoRefresh.checked) {
      timer = setInterval(refresh, REFRESH_MS);
    }
  }

  els.autoRefresh.addEventListener("change", schedule);
  refresh();
  schedule();
})();
