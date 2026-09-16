(function () {
  "use strict";

  const config = window.DATAPULSE_CONFIG || {};
  const MAX_EVENTS = 50;
  const sessionStart = Date.now();
  let events = [];

  function formatTime(timestamp) {
    return new Date(timestamp).toLocaleTimeString("zh-CN", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  function transportLabel() {
    if (config.kafkaEnabled && config.posthogEnabled) return "Kafka + PostHog";
    if (config.kafkaEnabled) return "Kafka · AWC Pipeline";
    if (config.posthogEnabled) return "PostHog 已连接";
    return "本地模式";
  }

  function updateBadge() {
    const badge = document.getElementById("posthog-badge");
    if (!badge) return;
    badge.textContent = transportLabel();
    badge.className =
      config.kafkaEnabled || config.posthogEnabled
        ? "badge badge-primary"
        : "badge badge-secondary";
  }

  function renderEvents() {
    const list = document.getElementById("event-list");
    const count = document.getElementById("event-count");
    if (!list || !count) return;

    const suffix = config.kafkaEnabled
      ? " · 同步到 Kafka"
      : config.posthogEnabled
        ? ""
        : " · 本地采集模式";
    count.textContent = `已采集 ${events.length} 条 AWC 互动${suffix}`;

    if (events.length === 0) {
      list.innerHTML =
        '<p class="event-empty">浏览 Marketplace、选择 Blueprint 或 Launch Demo，事件将实时出现在这里</p>';
      return;
    }

    list.innerHTML = events
      .map((event) => {
        const props =
          event.properties && Object.keys(event.properties).length > 0
            ? `<pre class="event-props">${JSON.stringify(event.properties, null, 2)}</pre>`
            : "";
        const meta = [
          event.session_id ? `session: ${event.session_id.slice(0, 12)}…` : "",
          event.anonymous_id ? `anon: ${event.anonymous_id.slice(0, 12)}…` : "",
        ]
          .filter(Boolean)
          .join(" · ");
        return `
          <div class="event-item">
            <div class="event-item-header">
              <code class="event-name">${event.name}</code>
              <span class="event-time">${formatTime(event.timestamp)}</span>
            </div>
            ${meta ? `<p class="event-meta-line">${meta}</p>` : ""}
            ${props}
          </div>
        `;
      })
      .join("");
  }

  function rememberEvent(event) {
    events = [event, ...events].slice(0, MAX_EVENTS);
    renderEvents();
    updateHomeMetrics();
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
      if (config.kafkaEnabled) {
        status.textContent = "Kafka → Live Events";
      } else if (config.posthogEnabled) {
        status.textContent = "PostHog 云端";
      } else {
        status.textContent = "本地面板";
      }
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
        DataPulse.capture(name, props);
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

  function initSdk() {
    if (typeof DataPulse === "undefined" || typeof DataPulse.init !== "function") {
      console.warn("DataPulse SDK not loaded");
      return;
    }

    DataPulse.init({
      projectId: config.projectId || "awc-demo",
      endpoint: config.endpoint || "/v1/capture",
      transportEnabled: config.kafkaEnabled !== false,
      pagePath: config.pagePath || window.location.pathname,
      onCapture: rememberEvent,
      superProperties: {
        product: "anywhere_cloud",
        demo: "datapulse-app",
      },
    });

    if (config.posthogEnabled && typeof posthog !== "undefined") {
      posthog.init(config.posthogKey, {
        api_host: config.posthogHost,
        person_profiles: "identified_only",
        capture_pageview: false,
        capture_pageleave: true,
      });

      const originalCapture = DataPulse.capture.bind(DataPulse);
      DataPulse.capture = function (name, properties) {
        const event = originalCapture(name, properties);
        posthog.capture(name, properties);
        return event;
      };

      const originalIdentify = DataPulse.identify.bind(DataPulse);
      DataPulse.identify = function (userId, traits) {
        posthog.identify(userId, traits);
        return originalIdentify(userId, traits);
      };
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    initSdk();
    updateBadge();
    bindGlobalTracking();
    bindPanelControls();
    DataPulse.capture("$pageview", {
      path: config.pagePath || window.location.pathname,
    });
  });

  window.DataPulseDemo = {
    clearEvents,
    updateHomeMetrics,
    getEvents: () => events.slice(),
  };

  window.DataPulse.trackEvent = DataPulse.capture;
  window.DataPulse.identifyUser = DataPulse.identify;
  window.DataPulse.clearEvents = clearEvents;
  window.DataPulse.updateHomeMetrics = updateHomeMetrics;
})();
