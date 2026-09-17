"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Activity } from "lucide-react";

import { useAnalytics } from "@/context/event-log-context";
import { cn } from "@/lib/utils";

const navItems = [
  { href: "/", label: "Home" },
  { href: "/features", label: "Features" },
  { href: "/playground", label: "Behavior lab" },
  { href: "/pricing", label: "Pricing" },
];

export function SiteHeader() {
  const pathname = usePathname();
  const { trackEvent } = useAnalytics();

  return (
    <header className="sticky top-0 z-40 border-b border-border/60 bg-background/80 backdrop-blur-md">
      <div className="mx-auto flex h-16 max-w-6xl items-center justify-between px-4 sm:px-6">
        <Link
          href="/"
          className="flex items-center gap-2 font-semibold tracking-tight"
          onClick={() => trackEvent("nav_logo_clicked")}
        >
          <span className="flex size-8 items-center justify-center rounded-lg bg-indigo-600 text-white">
            <Activity className="size-4" />
          </span>
          <span>DataPulse</span>
        </Link>

        <nav className="hidden items-center gap-1 md:flex">
          {navItems.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              onClick={() =>
                trackEvent("nav_link_clicked", {
                  label: item.label,
                  href: item.href,
                })
              }
              className={cn(
                "rounded-md px-3 py-2 text-sm transition-colors hover:bg-muted",
                pathname === item.href
                  ? "bg-muted font-medium text-foreground"
                  : "text-muted-foreground",
              )}
            >
              {item.label}
            </Link>
          ))}
        </nav>

        <Link
          href="/playground"
          onClick={() => trackEvent("cta_header_clicked")}
          className="inline-flex h-7 items-center rounded-lg bg-indigo-600 px-3 text-sm font-medium text-white transition-colors hover:bg-indigo-700"
        >
          Get started
        </Link>
      </div>
    </header>
  );
}
