(function () {
  "use strict";

  const REFRESH_MS = 30000;
  const CHART_COLORS = {
    events: "#60a5fa",
    sessions: "#34d399",
    forms: "#fbbf24",
    blueprint: "#818cf8",
    pages: "#38bdf8",
    eventsBar: "#a78bfa",
    grid: "rgba(148, 163, 184, 0.12)",
    text: "#94a3b8",
  };

  let timer = null;
  let cachedSessions = [];
  let cachedSessionSummary = {};
  let activeTab = "sessions";
  const charts = {};

  const els = {
    statusBar: document.getElementById("status-bar"),
    kpiGrid: document.getElementById("kpi-grid"),
    funnelPanel: document.getElementById("funnel-panel"),
    sessionsPanel: document.getElementById("sessions-panel"),
    sessionFilter: document.getElementById("session-filter"),
    sessionCountLabel: document.getElementById("session-count-label"),
    convertersPanel: document.getElementById("converters-panel"),
    pathsPanel: document.getElementById("paths-panel"),
    autoRefresh: document.getElementById("auto-refresh"),
    windowDays: document.getElementById("window-days"),
    blueprintEmpty: document.getElementById("blueprint-empty"),
    pagesEmpty: document.getElementById("pages-empty"),
    eventsEmpty: document.getElementById("events-empty"),
    trendEmpty: document.getElementById("trend-empty"),
  };

  function fmtPct(value) {
    return `${Number(value || 0).toFixed(1)}%`;
  }

  function fmtTime(seconds) {
    if (!seconds) return "-";
    return new Date(seconds * 1000).toLocaleString("en-US");
  }

  function formatWindow(data) {
    if (data.window_days) {
      return `last ${data.window_days} day(s)`;
    }
    return `last ${data.window_seconds || 0}s`;
  }

  function formatSource(data) {
    if (data.data_source === "trino") {
      const table = data.warehouse?.qualified_table || "Lakehouse";
      return `Lakehouse · ${table}`;
    }
    return "Live buffer (fallback)";
  }

  function chartDefaults() {
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          labels: { color: CHART_COLORS.text, boxWidth: 12 },
        },
      },
      scales: {
        x: {
          ticks: { color: CHART_COLORS.text, maxRotation: 45, minRotation: 0 },
          grid: { color: CHART_COLORS.grid },
        },
        y: {
          ticks: { color: CHART_COLORS.text },
          grid: { color: CHART_COLORS.grid },
        },
      },
    };
  }

  function destroyChart(key) {
    if (charts[key]) {
      charts[key].destroy();
      delete charts[key];
    }
  }

  function setEmptyState(canvasId, emptyEl, hasData) {
    const canvas = document.getElementById(canvasId);
    if (canvas) canvas.style.display = hasData ? "block" : "none";
    if (emptyEl) emptyEl.classList.toggle("hidden", hasData);
  }

  function renderStatus(data) {
    const pipeline = data.pipeline || {};
    const activeClass = pipeline.stream_active ? "ok" : "err";
    const warehouse = data.warehouse || {};
    const rows = [
      `<div>Data source: <strong>${formatSource(data)}</strong></div>`,
      `<div>Window: <strong>${formatWindow(data)}</strong></div>`,
      `<div>Live stream: <span class="${activeClass}">${pipeline.spark_status || "unknown"}</span></div>`,
    ];
    if (data.data_source === "trino" && warehouse.events_after_dedupe != null) {
      rows.push(`<div>Events analyzed: <strong>${warehouse.events_after_dedupe}</strong></div>`);
    } else {
      rows.push(`<div>Buffer: ${pipeline.buffer_used || 0}/${pipeline.buffer_size || 0}</div>`);
    }
    if (data.note) {
      rows.push(`<div class="status-note">${data.note}</div>`);
    }
    els.statusBar.innerHTML = rows.join("");
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

  function renderTrendChart(trends) {
    const canvas = document.getElementById("trend-chart");
    if (!canvas || typeof Chart === "undefined") return;

    destroyChart("trend");
    const hasData = Boolean(trends?.labels?.length);
    setEmptyState("trend-chart", els.trendEmpty, hasData);
    if (!hasData) return;

    charts.trend = new Chart(canvas, {
      type: "line",
      data: {
        labels: trends.labels,
        datasets: [
          {
            label: "Events",
            data: trends.events,
            borderColor: CHART_COLORS.events,
            backgroundColor: "rgba(96, 165, 250, 0.15)",
            fill: true,
            tension: 0.3,
          },
          {
            label: "Sessions",
            data: trends.sessions,
            borderColor: CHART_COLORS.sessions,
            backgroundColor: "rgba(52, 211, 153, 0.08)",
            fill: true,
            tension: 0.3,
          },
          {
            label: "Form submissions",
            data: trends.form_submissions,
            borderColor: CHART_COLORS.forms,
            backgroundColor: "rgba(251, 191, 36, 0.08)",
            fill: true,
            tension: 0.3,
          },
        ],
      },
      options: {
        ...chartDefaults(),
        interaction: { mode: "index", intersect: false },
        plugins: {
          ...chartDefaults().plugins,
          title: {
            display: false,
          },
        },
      },
    });
  }

  function renderHorizontalBarChart(key, canvasId, emptyEl, rows, labelKey, valueKey, color) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || typeof Chart === "undefined") return;

    destroyChart(key);
    const hasData = rows.length > 0;
    setEmptyState(canvasId, emptyEl, hasData);
    if (!hasData) return;

    const labels = rows.map((row) => row[labelKey]).reverse();
    const values = rows.map((row) => row[valueKey]).reverse();

    charts[key] = new Chart(canvas, {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            data: values,
            backgroundColor: color,
            borderRadius: 6,
          },
        ],
      },
      options: {
        indexAxis: "y",
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
        },
        scales: {
          x: {
            ticks: { color: CHART_COLORS.text },
            grid: { color: CHART_COLORS.grid },
          },
          y: {
            ticks: { color: CHART_COLORS.text },
            grid: { display: false },
          },
        },
      },
    });
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

  function renderPaths(rows) {
    if (!rows.length) {
      els.pathsPanel.innerHTML =
        '<p class="empty">No multi-step navigation paths yet. Pageview journeys appear after users browse multiple pages.</p>';
      return;
    }

    els.pathsPanel.innerHTML = `
      <table class="converters-table">
        <thead>
          <tr>
            <th>Path (up to 3 pageviews)</th>
            <th>Sessions</th>
            <th>Converted</th>
            <th>Conv. rate</th>
          </tr>
        </thead>
        <tbody>
          ${rows
            .map(
              (row) => `
            <tr>
              <td>${(row.path || "")
                .split(" → ")
                .map((segment) => `<span class="path-chip">${segment}</span>`)
                .join('<span class="path-arrow">→</span>')}</td>
              <td>${row.sessions}</td>
              <td>${row.converted}</td>
              <td>${fmtPct(row.conversion_rate_pct)}</td>
            </tr>`,
            )
            .join("")}
        </tbody>
      </table>
    `;
  }

  function setActiveTab(tab) {
    activeTab = tab;
    document.querySelectorAll(".tab-btn").forEach((btn) => {
      const isActive = btn.dataset.tab === tab;
      btn.classList.toggle("is-active", isActive);
      btn.setAttribute("aria-selected", isActive ? "true" : "false");
    });
    document.querySelectorAll(".tab-content").forEach((panel) => {
      const isActive = panel.id === `tab-${tab}`;
      panel.classList.toggle("is-active", isActive);
      panel.hidden = !isActive;
    });
    document.querySelectorAll(".tab-panel").forEach((el) => {
      el.classList.toggle("tab-panel--sessions", tab === "sessions");
    });
  }

  function summaryUrl() {
    const params = new URLSearchParams({ source: "auto" });
    const days = Number(els.windowDays?.value || 7);
    if (days > 0) {
      params.set("window_days", String(days));
    }
    return `/api/insights/summary?${params.toString()}`;
  }

  async function refresh() {
    try {
      const response = await fetch(summaryUrl());
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      renderStatus(data);
      renderKpis(data.kpis);
      renderFunnel(data.funnel);
      renderTrendChart(data.activity_trends);
      renderHorizontalBarChart(
        "blueprint",
        "blueprint-chart",
        els.blueprintEmpty,
        data.blueprint_leaderboard || [],
        "blueprint",
        "interactions",
        CHART_COLORS.blueprint,
      );
      renderHorizontalBarChart(
        "pages",
        "pages-chart",
        els.pagesEmpty,
        data.top_pages || [],
        "path",
        "views",
        CHART_COLORS.pages,
      );
      renderHorizontalBarChart(
        "events",
        "events-chart",
        els.eventsEmpty,
        data.top_events || [],
        "name",
        "count",
        CHART_COLORS.eventsBar,
      );
      cachedSessions = data.all_sessions || [];
      cachedSessionSummary = data.session_summary || {};
      renderSessions(cachedSessions, cachedSessionSummary);
      renderConverters(data.recent_converters || []);
      renderPaths(data.top_paths || []);
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

  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => setActiveTab(btn.dataset.tab || "sessions"));
  });

  els.autoRefresh.addEventListener("change", schedule);
  els.windowDays?.addEventListener("change", () => {
    refresh();
    schedule();
  });
  els.sessionFilter?.addEventListener("change", () => {
    renderSessions(cachedSessions, cachedSessionSummary);
  });

  function initWhenReady() {
    if (typeof Chart === "undefined") {
      window.setTimeout(initWhenReady, 50);
      return;
    }
    refresh();
    schedule();
  }

  initWhenReady();
})();
