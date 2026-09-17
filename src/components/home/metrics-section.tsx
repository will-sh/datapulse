"use client";

import { useState } from "react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useEventLog } from "@/context/event-log-context";

export function MetricsSection() {
  const { events, posthogEnabled } = useEventLog();
  const [sessionStart] = useState(() => Date.now());

  const pageviews = events.filter((e) => e.name === "$pageview").length;
  const customEvents = events.filter((e) => e.name !== "$pageview").length;
  const sessionMinutes = Math.max(
    1,
    Math.floor((Date.now() - sessionStart) / 60000),
  );

  return (
    <section className="mt-16 grid gap-4 sm:grid-cols-3">
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Page views
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-3xl font-bold">{pageviews}</p>
        </CardContent>
      </Card>
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Custom events
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-3xl font-bold">{customEvents}</p>
        </CardContent>
      </Card>
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Capture status
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-lg font-semibold">
            {posthogEnabled ? "PostHog cloud" : "Local panel"}
          </p>
          <p className="text-xs text-muted-foreground">
            Session ~{sessionMinutes} min
          </p>
        </CardContent>
      </Card>
    </section>
  );
}
