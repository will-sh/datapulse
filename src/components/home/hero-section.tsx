"use client";

import Link from "next/link";
import { ArrowRight, BarChart3, MousePointerClick, Zap } from "lucide-react";

import { useAnalytics } from "@/context/event-log-context";

const highlights = [
  { icon: MousePointerClick, label: "Click tracking" },
  { icon: BarChart3, label: "Page views" },
  { icon: Zap, label: "Custom events" },
];

export function HeroSection() {
  const { trackEvent } = useAnalytics();

  return (
    <section className="grid gap-10 lg:grid-cols-[1.2fr_1fr] lg:items-center">
      <div className="space-y-6">
        <div className="flex flex-wrap gap-2">
          <div className="inline-flex items-center rounded-full border border-indigo-200 bg-indigo-50 px-3 py-1 text-xs font-medium text-indigo-700 dark:border-indigo-900 dark:bg-indigo-950/50 dark:text-indigo-300">
            PostHog-based user behavior analytics demo
          </div>
          <div className="inline-flex items-center rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-medium text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/50 dark:text-emerald-300">
            Works in local mode without an API key
          </div>
        </div>
        <h1 className="text-4xl font-bold tracking-tight sm:text-5xl">
          DataPulse
          <span className="block text-indigo-600 dark:text-indigo-400">
            Insight into every user interaction
          </span>
        </h1>
        <p className="max-w-xl text-lg text-muted-foreground">
          DataPulse is a lightweight behavior analytics demo. Click buttons, switch pages, submit forms —
          every interaction appears in the event panel in real time. No PostHog account required; local mode covers the full demo.
        </p>
        <div className="flex flex-wrap gap-3">
          <Link
            href="/playground"
            onClick={() => trackEvent("hero_cta_clicked", { target: "playground" })}
            className="inline-flex h-9 items-center gap-1.5 rounded-lg bg-indigo-600 px-4 text-sm font-medium text-white transition-colors hover:bg-indigo-700"
          >
            Open behavior lab
            <ArrowRight className="size-4" />
          </Link>
          <Link
            href="/features"
            onClick={() => trackEvent("hero_secondary_clicked")}
            className="inline-flex h-9 items-center rounded-lg border border-border bg-background px-4 text-sm font-medium transition-colors hover:bg-muted"
          >
            Explore features
          </Link>
        </div>
      </div>

      <div className="rounded-2xl border border-border bg-card p-6 shadow-sm">
        <p className="mb-4 text-sm font-medium text-muted-foreground">
          Supported behavior types
        </p>
        <div className="grid gap-3">
          {highlights.map((item) => (
            <div
              key={item.label}
              className="flex items-center gap-3 rounded-xl border border-border/70 bg-muted/30 px-4 py-3"
            >
              <item.icon className="size-5 text-indigo-600" />
              <span className="font-medium">{item.label}</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
