(function (global) {
  "use strict";

  var STORAGE_ANON = "datapulse_anonymous_id";
  var STORAGE_USER = "datapulse_user_id";
  var STORAGE_SESSION = "datapulse_session_id";
  var STORAGE_SESSION_TS = "datapulse_session_started_at";

  var state = {
    initialized: false,
    projectId: "default",
    endpoint: "/v1/capture",
    transportEnabled: true,
    pagePath: null,
    sessionTimeoutMs: 30 * 60 * 1000,
    superProperties: {},
    onCapture: null,
  };

  function randomId(prefix) {
    if (global.crypto && typeof global.crypto.randomUUID === "function") {
      return prefix + global.crypto.randomUUID();
    }
    return (
      prefix +
      Date.now().toString(36) +
      "-" +
      Math.random().toString(36).slice(2, 10)
    );
  }

  function readStorage(storage, key) {
    try {
      return storage.getItem(key);
    } catch (_) {
      return null;
    }
  }

  function writeStorage(storage, key, value) {
    try {
      storage.setItem(key, value);
    } catch (_) {
      /* ignore */
    }
  }

  function getAnonymousId() {
    var existing = readStorage(global.localStorage, STORAGE_ANON);
    if (existing) return existing;
    var created = randomId("anon-");
    writeStorage(global.localStorage, STORAGE_ANON, created);
    return created;
  }

  function getUserId() {
    return readStorage(global.localStorage, STORAGE_USER);
  }

  function getSessionId() {
    var now = Date.now();
    var sessionId = readStorage(global.sessionStorage, STORAGE_SESSION);
    var startedAt = Number(readStorage(global.sessionStorage, STORAGE_SESSION_TS) || 0);
    if (sessionId && startedAt && now - startedAt < state.sessionTimeoutMs) {
      return sessionId;
    }
    sessionId = randomId("sess-");
    writeStorage(global.sessionStorage, STORAGE_SESSION, sessionId);
    writeStorage(global.sessionStorage, STORAGE_SESSION_TS, String(now));
    return sessionId;
  }

  function parseUtm() {
    var params = new URLSearchParams(global.location.search || "");
    var utm = {};
    ["utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"].forEach(
      function (key) {
        var value = params.get(key);
        if (value) utm[key.replace("utm_", "")] = value;
      },
    );
    return Object.keys(utm).length ? utm : null;
  }

  function buildContext() {
    var nav = global.navigator || {};
    return {
      browser: nav.userAgent || "",
      locale: nav.language || "",
      referrer: global.document ? global.document.referrer || "" : "",
      pathname: global.location ? global.location.pathname : "",
      utm: parseUtm(),
    };
  }

  function sendPayload(payload) {
    if (!state.transportEnabled || !state.endpoint) return Promise.resolve(false);

    var body = JSON.stringify(payload);
    var url = state.endpoint;

    if (global.navigator && typeof global.navigator.sendBeacon === "function") {
      try {
        var blob = new Blob([body], { type: "application/json" });
        if (global.navigator.sendBeacon(url, blob)) {
          return Promise.resolve(true);
        }
      } catch (_) {
        /* fall through */
      }
    }

    return fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body,
      keepalive: true,
      credentials: "same-origin",
    })
      .then(function (response) {
        return response.ok;
      })
      .catch(function () {
        return false;
      });
  }

  function buildEvent(name, properties) {
    var mergedProps = Object.assign({}, state.superProperties, properties || {});
    var pagePath =
      state.pagePath ||
      (global.location ? global.location.pathname : null);

    return {
      event_id: randomId("evt-"),
      project_id: state.projectId,
      name: name,
      properties: mergedProps,
      timestamp: Date.now(),
      source: "datapulse-sdk",
      user_id: getUserId(),
      anonymous_id: getAnonymousId(),
      session_id: getSessionId(),
      page_path: pagePath,
      context: buildContext(),
    };
  }

  function init(options) {
    options = options || {};
    state.projectId = options.projectId || state.projectId;
    state.endpoint = options.endpoint || state.endpoint;
    if (typeof options.transportEnabled === "boolean") {
      state.transportEnabled = options.transportEnabled;
    }
    if (options.pagePath) state.pagePath = options.pagePath;
    if (options.sessionTimeoutMs) state.sessionTimeoutMs = options.sessionTimeoutMs;
    if (typeof options.onCapture === "function") state.onCapture = options.onCapture;
    if (options.superProperties) {
      register(options.superProperties);
    }
    state.initialized = true;
    return api;
  }

  function capture(name, properties) {
    if (!name) return null;
    var event = buildEvent(name, properties);
    if (typeof state.onCapture === "function") {
      state.onCapture(event);
    }
    sendPayload(event);
    return event;
  }

  function identify(userId, traits) {
    if (!userId) return;
    writeStorage(global.localStorage, STORAGE_USER, String(userId));
    capture("user_identified", Object.assign({ userId: String(userId) }, traits || {}));
  }

  function register(properties) {
    state.superProperties = Object.assign({}, state.superProperties, properties || {});
    return state.superProperties;
  }

  function trackEvent(name, properties) {
    return capture(name, properties);
  }

  function identifyUser(userId, traits) {
    return identify(userId, traits);
  }

  var api = {
    init: init,
    capture: capture,
    identify: identify,
    register: register,
    trackEvent: trackEvent,
    identifyUser: identifyUser,
    getAnonymousId: getAnonymousId,
    getSessionId: getSessionId,
    getUserId: getUserId,
    getSuperProperties: function () {
      return Object.assign({}, state.superProperties);
    },
  };

  global.DataPulse = api;
})(window);
