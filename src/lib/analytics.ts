import posthog from "posthog-js";

export type TrackedEvent = {
  id: string;
  name: string;
  properties?: Record<string, unknown>;
  timestamp: number;
  source: "posthog" | "local";
};

type EventListener = (event: TrackedEvent) => void;

const listeners = new Set<EventListener>();

export function subscribeToEvents(listener: EventListener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function emitLocalEvent(event: TrackedEvent) {
  listeners.forEach((listener) => listener(event));
}

export function isPostHogConfigured() {
  return Boolean(process.env.NEXT_PUBLIC_POSTHOG_KEY);
}

export function trackEvent(
  name: string,
  properties?: Record<string, unknown>,
) {
  const event: TrackedEvent = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
    name,
    properties,
    timestamp: Date.now(),
    source: isPostHogConfigured() ? "posthog" : "local",
  };

  emitLocalEvent(event);

  if (isPostHogConfigured() && typeof window !== "undefined") {
    posthog.capture(name, properties);
  }

  return event;
}

export function trackPageView(path: string) {
  trackEvent("$pageview", { path });
}

export function identifyUser(
  userId: string,
  traits?: Record<string, unknown>,
) {
  if (isPostHogConfigured() && typeof window !== "undefined") {
    posthog.identify(userId, traits);
  }

  trackEvent("user_identified", { userId, ...traits });
}
