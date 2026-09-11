# DataPulse

基于 PostHog 的用户行为分析 Demo 应用。在网站上进行点击、浏览、表单提交等操作，事件会实时显示在右下角的事件采集面板中，并可同步到 PostHog 云端。

## 功能

- **页面浏览追踪** — 自动采集路由切换
- **自定义事件** — 按钮点击、功能使用、方案选择等
- **表单提交** — 模拟订阅表单转化事件
- **用户识别** — PostHog `identify` 演示
- **本地事件面板** — 未配置 PostHog 时也可完整演示

## 快速开始

```bash
npm install
npm run dev
```

访问 [http://localhost:4317](http://localhost:4317)

## 配置 PostHog（可选）

1. 在 [PostHog](https://posthog.com) 注册并创建项目
2. 复制 Project API Key
3. 创建 `.env.local`：

```env
NEXT_PUBLIC_POSTHOG_KEY=phc_your_project_api_key_here
NEXT_PUBLIC_POSTHOG_HOST=https://us.i.posthog.com
```

4. 重启开发服务器

配置后，事件面板会显示「PostHog 已连接」，数据同步到 PostHog 后台的 Live Events。

## 项目结构

```
src/
  app/              # 页面路由
  components/       # UI 组件
  context/          # 事件日志 Context
  lib/analytics.ts  # 统一事件采集 API
  providers/        # PostHog Provider
instrumentation-client.ts  # PostHog 客户端初始化
```

## 技术栈

- Next.js 16 (App Router)
- TypeScript
- Tailwind CSS + shadcn/ui
- PostHog (`posthog-js` + `@posthog/react`)
