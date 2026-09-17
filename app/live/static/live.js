(function () {
  "use strict";

  const REFRESH_MS = 3000;
  let timer = null;
  let lastTopReceivedAt = 0;
  let knownIds = new Set();

  const els = {
    statusBar: document.getElementById("status-bar"),
    statsPanel: document.getElementById("stats-panel"),
    eventsList: document.getElementById("events-list"),
    filterName: document.getElementById("filter-name"),
    filterUser: document.getElementById("filter-user"),
    clearFilters: document.getElementById("clear-filters"),
    autoRefresh: document.getElementById("auto-refresh"),
  };

  function fmtTime(seconds) {
    if (!seconds) return "-";
    return new Date(seconds * 1000).toLocaleString("en-US");
  }

  function eventKey(event) {
    return [
      event.name,
      event.timestamp,
      event.received_at,
      event.user_id || "",
      event.page_path || "",
    ].join("|");
  }

  function buildQuery() {
    const params = new URLSearchParams({ limit: "50", offset: "0" });
    if (els.filterName.value) params.set("name", els.filterName.value);
    if (els.filterUser.value.trim()) params.set("user_id", els.filterUser.value.trim());
    return params.toString();
  }

  function renderStatus(pipeline, stats) {
    const activeClass = pipeline.stream_active ? "ok" : "err";
    els.statusBar.innerHTML = `
      <div>Stream: <span class="${activeClass}">${pipeline.spark_status}</span></div>
      <div>Total received: <strong>${pipeline.total_received}</strong></div>
      <div>Buffer: ${pipeline.buffer_used}/${pipeline.buffer_size}</div>
      <div>Rate: ~${stats.rate_per_minute}/min (${stats.window_seconds}s window)</div>
      ${pipeline.last_error ? `<div class="err">Error: ${pipeline.last_error}</div>` : ""}
    `;
  }

  function renderStats(stats) {
    if (!stats.by_name.length) {
      els.statsPanel.innerHTML = '<p class="empty">No stats yet</p>';
      return;
    }
    els.statsPanel.innerHTML = stats.by_name
      .map(
        (row) => `
        <div class="stat-row">
          <span>${row.name}</span>
          <strong>${row.count}</strong>
        </div>`,
      )
      .join("");
  }

  function renderNames(names) {
    const current = els.filterName.value;
    els.filterName.innerHTML =
      '<option value="">All</option>' +
      names.map((name) => `<option value="${name}">${name}</option>`).join("");
    els.filterName.value = current;
  }

  function renderEvents(events) {
    if (!events.length) {
      els.eventsList.innerHTML =
        '<p class="empty">No events yet. Trigger actions on the datapulse-app playground.</p>';
      return;
    }

    const nextKnown = new Set();
    els.eventsList.innerHTML = events
      .map((event) => {
        const key = eventKey(event);
        nextKnown.add(key);
        const isNew =
          event.received_at > lastTopReceivedAt && !knownIds.has(key);
        const propsJson = JSON.stringify(event.properties || {}, null, 2);
        const propsId = `props-${key.replace(/[^a-zA-Z0-9_-]/g, "_")}`;
        return `
          <article class="event-card${isNew ? " is-new" : ""}">
            <div class="event-head">
              <code class="event-name">${event.name}</code>
              <span class="event-time">${fmtTime(event.received_at)}</span>
            </div>
            <div class="event-meta">
              <span>user: <strong>${event.user_id || "-"}</strong></span>
              <span>path: <strong>${event.page_path || "-"}</strong></span>
              <span>component: <strong>${event.component || "-"}</strong></span>
            </div>
            <button type="button" class="props-toggle" data-target="${propsId}">
              View properties
            </button>
            <pre id="${propsId}" class="props-block hidden">${propsJson}</pre>
          </article>`;
      })
      .join("");

    if (events[0]) {
      lastTopReceivedAt = Math.max(lastTopReceivedAt, events[0].received_at);
    }
    knownIds = nextKnown;

    els.eventsList.querySelectorAll(".props-toggle").forEach((button) => {
      button.addEventListener("click", () => {
        const block = document.getElementById(button.dataset.target || "");
        block?.classList.toggle("hidden");
      });
    });
  }

  async function refresh() {
    try {
      const [snapshotRes, namesRes] = await Promise.all([
        fetch(`/api/live/snapshot?${buildQuery()}`),
        fetch("/api/live/names"),
      ]);
      const snapshot = await snapshotRes.json();
      const namesPayload = await namesRes.json();
      renderStatus(snapshot.pipeline, snapshot.stats);
      renderStats(snapshot.stats);
      renderNames(namesPayload.names || []);
      renderEvents(snapshot.events || []);
    } catch (error) {
      els.statusBar.innerHTML = `<div class="err">Load failed: ${error}</div>`;
    }
  }

  function scheduleRefresh() {
    if (timer) clearInterval(timer);
    if (els.autoRefresh.checked) {
      timer = setInterval(refresh, REFRESH_MS);
    }
  }

  els.filterName.addEventListener("change", refresh);
  els.filterUser.addEventListener("change", refresh);
  els.filterUser.addEventListener("keydown", (event) => {
    if (event.key === "Enter") refresh();
  });
  els.clearFilters.addEventListener("click", () => {
    els.filterName.value = "";
    els.filterUser.value = "";
    refresh();
  });
  els.autoRefresh.addEventListener("change", scheduleRefresh);

  refresh();
  scheduleRefresh();
})();
