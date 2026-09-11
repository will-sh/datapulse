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
import { trackEvent } from "@/lib/analytics";

const plans = [
  {
    id: "starter",
    name: "入门版",
    price: "免费",
    description: "适合个人项目与小型 Demo",
    features: ["每月 1 万事件", "基础事件面板", "7 天数据保留"],
  },
  {
    id: "pro",
    name: "专业版",
    price: "¥299/月",
    description: "适合成长型团队",
    features: ["每月 100 万事件", "漏斗与留存分析", "90 天数据保留", "邮件告警"],
    popular: true,
  },
  {
    id: "enterprise",
    name: "企业版",
    price: "联系销售",
    description: "适合大规模生产环境",
    features: ["无限事件", "专属技术支持", "SLA 保障", "私有化部署"],
  },
];

export default function PricingPage() {
  const [selectedPlan, setSelectedPlan] = useState<string | null>(null);

  function selectPlan(planId: string, planName: string) {
    setSelectedPlan(planId);
    trackEvent("plan_selected", { plan_id: planId, plan_name: planName });
  }

  return (
    <div className="space-y-8">
      <div className="text-center">
        <Badge className="mb-3 bg-indigo-600">定价方案</Badge>
        <h1 className="text-3xl font-bold tracking-tight">选择适合你的方案</h1>
        <p className="mt-2 text-muted-foreground">
          点击选择方案，触发 <code>plan_selected</code> 转化事件
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
                <Badge className="mb-2 w-fit bg-indigo-600">最受欢迎</Badge>
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
                {selectedPlan === plan.id ? "已选择" : "选择方案"}
              </Button>
            </CardFooter>
          </Card>
        ))}
      </div>
    </div>
  );
}
