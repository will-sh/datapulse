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
            页面浏览
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-3xl font-bold">{pageviews}</p>
        </CardContent>
      </Card>
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            自定义事件
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-3xl font-bold">{customEvents}</p>
        </CardContent>
      </Card>
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            采集状态
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-lg font-semibold">
            {posthogEnabled ? "PostHog 云端" : "本地面板"}
          </p>
          <p className="text-xs text-muted-foreground">
            会话约 {sessionMinutes} 分钟
          </p>
        </CardContent>
      </Card>
    </section>
  );
}
