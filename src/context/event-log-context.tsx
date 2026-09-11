"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  subscribeToEvents,
  TrackedEvent,
  isPostHogConfigured,
} from "@/lib/analytics";

type EventLogContextValue = {
  events: TrackedEvent[];
  clearEvents: () => void;
  posthogEnabled: boolean;
};

const EventLogContext = createContext<EventLogContextValue | null>(null);

export function EventLogProvider({ children }: { children: React.ReactNode }) {
  const [events, setEvents] = useState<TrackedEvent[]>([]);

  useEffect(() => {
    const unsubscribe = subscribeToEvents((event) => {
      setEvents((prev) => [event, ...prev].slice(0, 50));
    });
    return () => {
      unsubscribe();
    };
  }, []);

  const clearEvents = useCallback(() => setEvents([]), []);

  const value = useMemo(
    () => ({
      events,
      clearEvents,
      posthogEnabled: isPostHogConfigured(),
    }),
    [events, clearEvents],
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
