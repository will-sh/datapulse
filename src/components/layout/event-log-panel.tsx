"use client";

import { useState } from "react";
import { ChevronDown, ChevronUp, Radio, Trash2 } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { useEventLog } from "@/context/event-log-context";
import { cn } from "@/lib/utils";

function formatTime(timestamp: number) {
  return new Date(timestamp).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function EventLogPanel() {
  const { events, clearEvents, posthogEnabled } = useEventLog();
  const [expanded, setExpanded] = useState(true);

  return (
    <aside
      className={cn(
        "fixed bottom-4 right-4 z-50 flex w-[min(100vw-2rem,380px)] flex-col overflow-hidden rounded-xl border border-border bg-card shadow-2xl transition-all",
        expanded ? "h-[min(70vh,520px)]" : "h-auto",
      )}
    >
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <div className="flex items-center gap-2">
          <Radio className="size-4 text-indigo-500" />
          <span className="text-sm font-semibold">Event capture panel</span>
          <Badge variant={posthogEnabled ? "default" : "secondary"}>
            {posthogEnabled ? "PostHog connected" : "Local mode"}
          </Badge>
        </div>
        <div className="flex items-center gap-1">
          <Button
            variant="ghost"
            size="icon-sm"
            onClick={clearEvents}
            aria-label="Clear events"
          >
            <Trash2 className="size-4" />
          </Button>
          <Button
            variant="ghost"
            size="icon-sm"
            onClick={() => setExpanded((v) => !v)}
            aria-label={expanded ? "Collapse panel" : "Expand panel"}
          >
            {expanded ? (
              <ChevronDown className="size-4" />
            ) : (
              <ChevronUp className="size-4" />
            )}
          </Button>
        </div>
      </div>

      {expanded && (
        <>
          <div className="border-b border-border px-4 py-2 text-xs text-muted-foreground">
            {events.length} events captured
            {!posthogEnabled && " · configure a PostHog key to sync to the cloud"}
          </div>
          <ScrollArea className="flex-1 p-3">
            {events.length === 0 ? (
              <p className="px-2 py-8 text-center text-sm text-muted-foreground">
                Interact with the page — events appear here in real time
              </p>
            ) : (
              <ul className="space-y-2">
                {events.map((event) => (
                  <li
                    key={event.id}
                    className="rounded-lg border border-border/70 bg-muted/40 p-3"
                  >
                    <div className="flex items-start justify-between gap-2">
                      <code className="text-xs font-semibold text-indigo-600 dark:text-indigo-400">
                        {event.name}
                      </code>
                      <span className="shrink-0 text-[10px] text-muted-foreground">
                        {formatTime(event.timestamp)}
                      </span>
                    </div>
                    {event.properties &&
                      Object.keys(event.properties).length > 0 && (
                        <pre className="mt-2 overflow-x-auto text-[10px] leading-relaxed text-muted-foreground">
                          {JSON.stringify(event.properties, null, 2)}
                        </pre>
                      )}
                  </li>
                ))}
              </ul>
            )}
          </ScrollArea>
        </>
      )}
    </aside>
  );
}
