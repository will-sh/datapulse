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
    title: "实时事件流",
    description: "毫秒级采集用户点击、浏览与转化行为，右侧面板即时展示。",
  },
  {
    id: "funnels",
    icon: Filter,
    title: "漏斗分析",
    description: "追踪用户从访问到转化的完整路径，发现流失节点。",
  },
  {
    id: "cohorts",
    icon: Users,
    title: "用户分群",
    description: "按行为特征划分用户群体，精准定位高价值用户。",
  },
  {
    id: "alerts",
    icon: Bell,
    title: "异常告警",
    description: "关键指标波动时自动通知，快速响应业务变化。",
  },
  {
    id: "privacy",
    icon: Shield,
    title: "隐私合规",
    description: "支持数据脱敏与 GDPR 合规配置，保护用户隐私。",
  },
  {
    id: "export",
    icon: Share2,
    title: "数据导出",
    description: "一键导出事件数据，对接 BI 工具与数据仓库。",
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
        <Badge className="mb-3 bg-indigo-600">功能展示</Badge>
        <h1 className="text-3xl font-bold tracking-tight">DataPulse 核心能力</h1>
        <p className="mt-2 max-w-2xl text-muted-foreground">
          点击功能卡片标记感兴趣项，每次操作都会触发{" "}
          <code className="text-sm">feature_interest_toggled</code> 事件。
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
                  {active ? "已标记感兴趣" : "标记感兴趣"}
                </Button>
              </CardContent>
            </Card>
          );
        })}
      </div>
    </div>
  );
}
