"use client";

import { useState } from "react";
import { Check } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { useAnalytics } from "@/context/event-log-context";

const plans = [
  {
    id: "starter",
    name: "Starter",
    price: "Free",
    description: "For personal projects and small demos",
    features: ["10K events/month", "Basic event panel", "7-day data retention"],
  },
  {
    id: "pro",
    name: "Pro",
    price: "$39/mo",
    description: "For growing teams",
    features: [
      "1M events/month",
      "Funnel and retention analysis",
      "90-day data retention",
      "Email alerts",
    ],
    popular: true,
  },
  {
    id: "enterprise",
    name: "Enterprise",
    price: "Contact sales",
    description: "For large-scale production",
    features: [
      "Unlimited events",
      "Dedicated support",
      "SLA guarantee",
      "Private deployment",
    ],
  },
];

export default function PricingPage() {
  const { trackEvent } = useAnalytics();
  const [selectedPlan, setSelectedPlan] = useState<string | null>(null);

  function selectPlan(planId: string, planName: string) {
    setSelectedPlan(planId);
    trackEvent("plan_selected", { plan_id: planId, plan_name: planName });
  }

  return (
    <div className="space-y-8">
      <div className="text-center">
        <Badge className="mb-3 bg-indigo-600">Pricing plans</Badge>
        <h1 className="text-3xl font-bold tracking-tight">Choose the right plan</h1>
        <p className="mt-2 text-muted-foreground">
          Click to select a plan and fire a <code>plan_selected</code> conversion event
        </p>
      </div>

      <div className="grid gap-6 md:grid-cols-3">
        {plans.map((plan) => (
          <Card
            key={plan.id}
            className={
              selectedPlan === plan.id
                ? "border-indigo-400 ring-2 ring-indigo-200 dark:ring-indigo-900"
                : plan.popular
                  ? "border-indigo-200 shadow-md"
                  : ""
            }
          >
            <CardHeader>
              {plan.popular && (
                <Badge className="mb-2 w-fit bg-indigo-600">Most popular</Badge>
              )}
              <CardTitle>{plan.name}</CardTitle>
              <CardDescription>{plan.description}</CardDescription>
              <p className="pt-2 text-3xl font-bold">{plan.price}</p>
            </CardHeader>
            <CardContent>
              <ul className="space-y-2">
                {plan.features.map((feature) => (
                  <li key={feature} className="flex items-center gap-2 text-sm">
                    <Check className="size-4 shrink-0 text-indigo-600" />
                    {feature}
                  </li>
                ))}
              </ul>
            </CardContent>
            <CardFooter>
              <Button
                className="w-full bg-indigo-600 hover:bg-indigo-700"
                variant={selectedPlan === plan.id ? "secondary" : "default"}
                onClick={() => selectPlan(plan.id, plan.name)}
              >
                {selectedPlan === plan.id ? "Selected" : "Select plan"}
              </Button>
            </CardFooter>
          </Card>
        ))}
      </div>
    </div>
  );
}
