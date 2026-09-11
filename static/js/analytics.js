(function () {
  "use strict";

  const config = window.DATAPULSE_CONFIG || {};
  const MAX_EVENTS = 50;
  const sessionStart = Date.now();
  let events = [];

  if (config.posthogEnabled && typeof posthog !== "undefined") {
    posthog.init(config.posthogKey, {
      api_host: config.posthogHost,
      person_profiles: "identified_only",
      capture_pageview: false,
      capture_pageleave: true,
    });
  }

  function formatTime(timestamp) {
    return new Date(timestamp).toLocaleTimeString("zh-CN", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  function updateBadge() {
    const badge = document.getElementById("posthog-badge");
    if (!badge) return;
    if (config.posthogEnabled) {
      badge.textContent = "PostHog 已连接";
      badge.className = "badge badge-primary";
    } else {
      badge.textContent = "本地模式";
      badge.className = "badge badge-secondary";
    }
  }

  function renderEvents() {
    const list = document.getElementById("event-list");
    const count = document.getElementById("event-count");
    if (!list || !count) return;

    const suffix = config.posthogEnabled
      ? ""
      : " · 配置 PostHog Key 后可同步到云端";
    count.textContent = `已采集 ${events.length} 条事件${suffix}`;

    if (events.length === 0) {
      list.innerHTML =
        '<p class="event-empty">在页面上操作，事件会实时出现在这里</p>';
      return;
    }

    list.innerHTML = events
      .map((event) => {
        const props =
          event.properties && Object.keys(event.properties).length > 0
            ? `<pre class="event-props">${JSON.stringify(event.properties, null, 2)}</pre>`
            : "";
        return `
          <div class="event-item">
            <div class="event-item-header">
              <code class="event-name">${event.name}</code>
              <span class="event-time">${formatTime(event.timestamp)}</span>
            </div>
            ${props}
          </div>
        `;
      })
      .join("");
  }

  function trackEvent(name, properties) {
    const event = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
      name,
      properties,
      timestamp: Date.now(),
      source: config.posthogEnabled ? "posthog" : "local",
    };

    events = [event, ...events].slice(0, MAX_EVENTS);
    renderEvents();
    updateHomeMetrics();

    if (config.posthogEnabled && typeof posthog !== "undefined") {
      posthog.capture(name, properties);
    }

    return event;
  }

  function identifyUser(userId, traits) {
    if (config.posthogEnabled && typeof posthog !== "undefined") {
      posthog.identify(userId, traits);
    }
    trackEvent("user_identified", { userId, ...traits });
  }

  function clearEvents() {
    events = [];
    renderEvents();
    updateHomeMetrics();
  }

  function updateHomeMetrics() {
    const pageviews = document.getElementById("metric-pageviews");
    const custom = document.getElementById("metric-custom");
    const status = document.getElementById("metric-status");
    const session = document.getElementById("metric-session");

    if (!pageviews) return;

    pageviews.textContent = events.filter((e) => e.name === "$pageview").length;
    custom.textContent = events.filter((e) => e.name !== "$pageview").length;

    if (status) {
      status.textContent = config.posthogEnabled ? "PostHog 云端" : "本地面板";
    }

    if (session) {
      const minutes = Math.max(
        1,
        Math.floor((Date.now() - sessionStart) / 60000),
      );
      session.textContent = `会话约 ${minutes} 分钟`;
    }
  }

  function bindGlobalTracking() {
    document.querySelectorAll("[data-track]").forEach((el) => {
      el.addEventListener("click", () => {
        const name = el.dataset.track;
        let props = {};
        if (el.dataset.trackProps) {
          try {
            props = JSON.parse(el.dataset.trackProps);
          } catch (_) {
            /* ignore */
          }
        }
        trackEvent(name, props);
      });
    });
  }

  function bindPanelControls() {
    const panel = document.getElementById("event-panel");
    const clearBtn = document.getElementById("clear-events-btn");
    const toggleBtn = document.getElementById("toggle-panel-btn");

    clearBtn?.addEventListener("click", clearEvents);

    toggleBtn?.addEventListener("click", () => {
      panel?.classList.toggle("expanded");
      toggleBtn.textContent = panel?.classList.contains("expanded") ? "▼" : "▲";
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    updateBadge();
    bindGlobalTracking();
    bindPanelControls();
    trackEvent("$pageview", { path: config.pagePath || window.location.pathname });
  });

  window.DataPulse = {
    trackEvent,
    identifyUser,
    clearEvents,
    updateHomeMetrics,
  };
})();
