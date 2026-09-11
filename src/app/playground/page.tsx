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
import { identifyUser, trackEvent } from "@/lib/analytics";

export default function PlaygroundPage() {
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
        <Badge className="mb-3 bg-indigo-600">行为实验室</Badge>
        <h1 className="text-3xl font-bold tracking-tight">交互式事件采集</h1>
        <p className="mt-2 max-w-2xl text-muted-foreground">
          在下方尝试不同操作，观察右侧事件面板的实时反馈。
        </p>
      </div>

      <Tabs
        defaultValue="events"
        onValueChange={(value) =>
          trackEvent("playground_tab_changed", { tab: value })
        }
      >
        <TabsList>
          <TabsTrigger value="events">自定义事件</TabsTrigger>
          <TabsTrigger value="form">表单提交</TabsTrigger>
          <TabsTrigger value="identity">用户识别</TabsTrigger>
        </TabsList>

        <TabsContent value="events" className="mt-4">
          <Card>
            <CardHeader>
              <CardTitle>一键触发事件</CardTitle>
              <CardDescription>
                模拟产品中的常见用户行为
              </CardDescription>
            </CardHeader>
            <CardContent className="flex flex-wrap gap-3">
              <Button
                onClick={() =>
                  trackEvent("button_clicked", { button: "primary_action" })
                }
              >
                主要操作
              </Button>
              <Button
                variant="outline"
                onClick={() =>
                  trackEvent("button_clicked", { button: "secondary_action" })
                }
              >
                次要操作
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
                导出数据
              </Button>
              <Button
                variant="destructive"
                onClick={() =>
                  trackEvent("account_action", { action: "delete_requested" })
                }
              >
                删除账户（模拟）
              </Button>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="form" className="mt-4">
          <Card>
            <CardHeader>
              <CardTitle>订阅表单</CardTitle>
              <CardDescription>
                提交后触发 <code>demo_form_submitted</code> 事件
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
                  提交
                </Button>
              </form>
              {submitted && (
                <p className="mt-3 text-sm text-green-600 dark:text-green-400">
                  表单已提交，事件已采集！
                </p>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="identity" className="mt-4">
          <Card>
            <CardHeader>
              <CardTitle>用户识别</CardTitle>
              <CardDescription>
                调用 PostHog identify，将匿名用户关联到具体 ID
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex max-w-md gap-2">
                <Input
                  placeholder="用户 ID（留空自动生成）"
                  value={userId}
                  onChange={(e) => setUserId(e.target.value)}
                />
                <Button onClick={handleIdentify}>
                  <UserPlus className="size-4" />
                  识别用户
                </Button>
              </div>
              {userId && (
                <p className="text-sm text-muted-foreground">
                  当前用户 ID：<code>{userId}</code>
                </p>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
