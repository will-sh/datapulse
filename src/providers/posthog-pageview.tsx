"use client";

import { useEffect } from "react";
import { usePathname, useSearchParams } from "next/navigation";

import { useAnalytics } from "@/context/event-log-context";

export function PostHogPageView() {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { trackEvent } = useAnalytics();

  useEffect(() => {
    if (!pathname) return;
    const url =
      pathname +
      (searchParams?.toString() ? `?${searchParams.toString()}` : "");
    trackEvent("$pageview", { path: url });
  }, [pathname, searchParams, trackEvent]);

  return null;
}
