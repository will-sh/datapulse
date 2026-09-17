"use client";

import { useState } from "react";
import { Send, UserPlus } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useAnalytics } from "@/context/event-log-context";

export default function PlaygroundPage() {
  const { trackEvent, identifyUser } = useAnalytics();
  const [email, setEmail] = useState("");
  const [userId, setUserId] = useState("");
  const [submitted, setSubmitted] = useState(false);

  function handleFormSubmit(e: React.FormEvent) {
    e.preventDefault();
    trackEvent("demo_form_submitted", { email, has_email: Boolean(email) });
    setSubmitted(true);
    setTimeout(() => setSubmitted(false), 3000);
  }

  function handleIdentify() {
    const id = userId || `demo-user-${Date.now()}`;
    identifyUser(id, { email: email || undefined, plan: "demo" });
    setUserId(id);
  }

  return (
    <div className="space-y-8">
      <div>
        <Badge className="mb-3 bg-indigo-600">Behavior lab</Badge>
        <h1 className="text-3xl font-bold tracking-tight">Interactive event capture</h1>
        <p className="mt-2 max-w-2xl text-muted-foreground">
          Try the actions below and watch the event panel update in real time.
        </p>
      </div>

      <Tabs
        defaultValue="events"
        onValueChange={(value) =>
          trackEvent("playground_tab_changed", { tab: value })
        }
      >
        <TabsList>
          <TabsTrigger value="events">Custom events</TabsTrigger>
          <TabsTrigger value="form">Form submit</TabsTrigger>
          <TabsTrigger value="identity">User identify</TabsTrigger>
        </TabsList>

        <TabsContent value="events" className="mt-4">
          <Card>
            <CardHeader>
              <CardTitle>Trigger events</CardTitle>
              <CardDescription>
                Simulate common product user actions
              </CardDescription>
            </CardHeader>
            <CardContent className="flex flex-wrap gap-3">
              <Button
                onClick={() =>
                  trackEvent("button_clicked", { button: "primary_action" })
                }
              >
                Primary action
              </Button>
              <Button
                variant="outline"
                onClick={() =>
                  trackEvent("button_clicked", { button: "secondary_action" })
                }
              >
                Secondary action
              </Button>
              <Button
                variant="secondary"
                onClick={() =>
                  trackEvent("feature_used", {
                    feature: "export",
                    format: "csv",
                  })
                }
              >
                Export data
              </Button>
              <Button
                variant="destructive"
                onClick={() =>
                  trackEvent("account_action", { action: "delete_requested" })
                }
              >
                Delete account (simulated)
              </Button>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="form" className="mt-4">
          <Card>
            <CardHeader>
              <CardTitle>Subscription form</CardTitle>
              <CardDescription>
                Submitting fires a <code>demo_form_submitted</code> event
              </CardDescription>
            </CardHeader>
            <CardContent>
              <form onSubmit={handleFormSubmit} className="flex max-w-md gap-2">
                <Input
                  type="email"
                  placeholder="your@email.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  required
                />
                <Button type="submit" className="bg-indigo-600 hover:bg-indigo-700">
                  <Send className="size-4" />
                  Submit
                </Button>
              </form>
              {submitted && (
                <p className="mt-3 text-sm text-green-600 dark:text-green-400">
                  Form submitted — event captured!
                </p>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="identity" className="mt-4">
          <Card>
            <CardHeader>
              <CardTitle>User identify</CardTitle>
              <CardDescription>
                Call PostHog identify to link an anonymous visitor to a user ID
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex max-w-md gap-2">
                <Input
                  placeholder="User ID (auto-generated if empty)"
                  value={userId}
                  onChange={(e) => setUserId(e.target.value)}
                />
                <Button onClick={handleIdentify}>
                  <UserPlus className="size-4" />
                  Identify user
                </Button>
              </div>
              {userId && (
                <p className="text-sm text-muted-foreground">
                  Current user ID: <code>{userId}</code>
                </p>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
