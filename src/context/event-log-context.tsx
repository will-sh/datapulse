"use client";

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
} from "react";
import posthog from "posthog-js";

import { isPostHogConfigured } from "@/lib/analytics";

export type TrackedEvent = {
  id: string;
  name: string;
  properties?: Record<string, unknown>;
  timestamp: number;
  source: "posthog" | "local";
};

type EventLogContextValue = {
  events: TrackedEvent[];
  clearEvents: () => void;
  posthogEnabled: boolean;
  trackEvent: (
    name: string,
    properties?: Record<string, unknown>,
  ) => TrackedEvent;
  identifyUser: (
    userId: string,
    traits?: Record<string, unknown>,
  ) => void;
};

const EventLogContext = createContext<EventLogContextValue | null>(null);

export function EventLogProvider({ children }: { children: React.ReactNode }) {
  const [events, setEvents] = useState<TrackedEvent[]>([]);

  const trackEvent = useCallback(
    (name: string, properties?: Record<string, unknown>) => {
      const event: TrackedEvent = {
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
        name,
        properties,
        timestamp: Date.now(),
        source: isPostHogConfigured() ? "posthog" : "local",
      };

      setEvents((prev) => [event, ...prev].slice(0, 50));

      if (isPostHogConfigured()) {
        posthog.capture(name, properties);
      }

      return event;
    },
    [],
  );

  const identifyUser = useCallback(
    (userId: string, traits?: Record<string, unknown>) => {
      if (isPostHogConfigured()) {
        posthog.identify(userId, traits);
      }
      trackEvent("user_identified", { userId, ...traits });
    },
    [trackEvent],
  );

  const clearEvents = useCallback(() => setEvents([]), []);

  const value = useMemo(
    () => ({
      events,
      clearEvents,
      posthogEnabled: isPostHogConfigured(),
      trackEvent,
      identifyUser,
    }),
    [events, clearEvents, trackEvent, identifyUser],
  );

  return (
    <EventLogContext.Provider value={value}>{children}</EventLogContext.Provider>
  );
}

export function useEventLog() {
  const context = useContext(EventLogContext);
  if (!context) {
    throw new Error("useEventLog must be used within EventLogProvider");
  }
  return context;
}

export function useAnalytics() {
  const { trackEvent, identifyUser } = useEventLog();
  return { trackEvent, identifyUser };
}
