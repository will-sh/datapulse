export type { TrackedEvent } from "@/context/event-log-context";

export function isPostHogConfigured() {
  return Boolean(process.env.NEXT_PUBLIC_POSTHOG_KEY);
}
