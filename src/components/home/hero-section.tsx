"use client";

import Link from "next/link";
import { ArrowRight, BarChart3, MousePointerClick, Zap } from "lucide-react";

import { trackEvent } from "@/lib/analytics";

const highlights = [
  { icon: MousePointerClick, label: "点击追踪" },
  { icon: BarChart3, label: "页面浏览" },
  { icon: Zap, label: "自定义事件" },
];

export function HeroSection() {
  return (
    <section className="grid gap-10 lg:grid-cols-[1.2fr_1fr] lg:items-center">
      <div className="space-y-6">
        <div className="flex flex-wrap gap-2">
          <div className="inline-flex items-center rounded-full border border-indigo-200 bg-indigo-50 px-3 py-1 text-xs font-medium text-indigo-700 dark:border-indigo-900 dark:bg-indigo-950/50 dark:text-indigo-300">
            基于 PostHog 的用户行为分析 Demo
          </div>
          <div className="inline-flex items-center rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-medium text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/50 dark:text-emerald-300">
            无需 API Key，本地模式即可体验
          </div>
        </div>
        <h1 className="text-4xl font-bold tracking-tight sm:text-5xl">
          DataPulse
          <span className="block text-indigo-600 dark:text-indigo-400">
            洞察每一次用户互动
          </span>
        </h1>
        <p className="max-w-xl text-lg text-muted-foreground">
          DataPulse 是一个轻量级行为分析应用演示。点击按钮、切换页面、提交表单——
          所有互动都会实时出现在右下角的事件面板中。没有 PostHog 账号也没关系，本地模式已覆盖全部演示功能。
        </p>
        <div className="flex flex-wrap gap-3">
          <Link
            href="/playground"
            onClick={() => trackEvent("hero_cta_clicked", { target: "playground" })}
            className="inline-flex h-9 items-center gap-1.5 rounded-lg bg-indigo-600 px-4 text-sm font-medium text-white transition-colors hover:bg-indigo-700"
          >
            进入行为实验室
            <ArrowRight className="size-4" />
          </Link>
          <Link
            href="/features"
            onClick={() => trackEvent("hero_secondary_clicked")}
            className="inline-flex h-9 items-center rounded-lg border border-border bg-background px-4 text-sm font-medium transition-colors hover:bg-muted"
          >
            了解功能
          </Link>
        </div>
      </div>

      <div className="rounded-2xl border border-border bg-card p-6 shadow-sm">
        <p className="mb-4 text-sm font-medium text-muted-foreground">
          支持采集的行为类型
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
