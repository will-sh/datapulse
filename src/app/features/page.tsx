"use client";

import { useState } from "react";
import {
  Bell,
  Filter,
  LineChart,
  Share2,
  Shield,
  Users,
} from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { useAnalytics } from "@/context/event-log-context";

const features = [
  {
    id: "realtime",
    icon: LineChart,
    title: "Real-time event stream",
    description: "Capture clicks, views, and conversions in milliseconds; show them instantly in the side panel.",
  },
  {
    id: "funnels",
    icon: Filter,
    title: "Funnel analysis",
    description: "Track the full path from visit to conversion and find drop-off points.",
  },
  {
    id: "cohorts",
    icon: Users,
    title: "User cohorts",
    description: "Segment users by behavior and focus on high-value groups.",
  },
  {
    id: "alerts",
    icon: Bell,
    title: "Anomaly alerts",
    description: "Get notified when key metrics shift so you can respond quickly.",
  },
  {
    id: "privacy",
    icon: Shield,
    title: "Privacy compliance",
    description: "Support data masking and GDPR-oriented configuration to protect user privacy.",
  },
  {
    id: "export",
    icon: Share2,
    title: "Data export",
    description: "Export event data in one click for BI tools and data warehouses.",
  },
];

export default function FeaturesPage() {
  const { trackEvent } = useAnalytics();
  const [interested, setInterested] = useState<string[]>([]);

  function toggleInterest(id: string, title: string) {
    setInterested((prev) => {
      const next = prev.includes(id)
        ? prev.filter((x) => x !== id)
        : [...prev, id];
      trackEvent("feature_interest_toggled", {
        feature_id: id,
        feature_title: title,
        interested: !prev.includes(id),
      });
      return next;
    });
  }

  return (
    <div className="space-y-8">
      <div>
        <Badge className="mb-3 bg-indigo-600">Feature showcase</Badge>
        <h1 className="text-3xl font-bold tracking-tight">DataPulse core capabilities</h1>
        <p className="mt-2 max-w-2xl text-muted-foreground">
          Click a feature card to mark interest — each action fires a{" "}
          <code className="text-sm">feature_interest_toggled</code> event.
        </p>
      </div>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {features.map((feature) => {
          const active = interested.includes(feature.id);
          return (
            <Card
              key={feature.id}
              className={
                active
                  ? "border-indigo-300 ring-2 ring-indigo-200 dark:border-indigo-800 dark:ring-indigo-900"
                  : ""
              }
            >
              <CardHeader>
                <div className="mb-2 flex size-10 items-center justify-center rounded-lg bg-indigo-100 text-indigo-700 dark:bg-indigo-950 dark:text-indigo-300">
                  <feature.icon className="size-5" />
                </div>
                <CardTitle className="text-lg">{feature.title}</CardTitle>
                <CardDescription>{feature.description}</CardDescription>
              </CardHeader>
              <CardContent>
                <Button
                  variant={active ? "default" : "outline"}
                  className={active ? "bg-indigo-600 hover:bg-indigo-700" : ""}
                  onClick={() => toggleInterest(feature.id, feature.title)}
                >
                  {active ? "Marked interested" : "Mark interested"}
                </Button>
              </CardContent>
            </Card>
          );
        })}
      </div>
    </div>
  );
}
