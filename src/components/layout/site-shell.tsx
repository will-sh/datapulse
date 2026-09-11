import { SiteHeader } from "@/components/layout/site-header";
import { EventLogPanel } from "@/components/layout/event-log-panel";

export function SiteShell({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen bg-gradient-to-b from-indigo-50/50 via-background to-background dark:from-indigo-950/20">
      <SiteHeader />
      <main className="mx-auto max-w-6xl px-4 pb-28 pt-8 sm:px-6 sm:pb-8">
        {children}
      </main>
      <EventLogPanel />
    </div>
  );
}
