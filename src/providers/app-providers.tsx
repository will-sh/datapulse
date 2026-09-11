"use client";

import { Suspense } from "react";
import { PostHogProvider } from "@posthog/react";
import posthog from "posthog-js";

import { EventLogProvider } from "@/context/event-log-context";
import { PostHogPageView } from "@/providers/posthog-pageview";

export function AppProviders({ children }: { children: React.ReactNode }) {
  return (
    <PostHogProvider client={posthog}>
      <EventLogProvider>
        <Suspense fallback={null}>
          <PostHogPageView />
        </Suspense>
        {children}
      </EventLogProvider>
    </PostHogProvider>
  );
}
