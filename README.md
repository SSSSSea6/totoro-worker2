# Totoro Worker

Supabase-backed worker that consumes tasks from the `Tasks` table and runs the long-running Totoro automation in the background.

## Quick start

1) `cp .env.example .env` 并填入 `SUPABASE_URL` 与 `SUPABASE_SERVICE_KEY`。  
2) `npm install`（已在仓库初始化过，可按需再次执行）。  
3) `npm start` 本地验证轮询逻辑。

## 任务负载

`worker.js` 会将 `Tasks.user_data` 作为唯一参数传递给 `executeRunTask`。当前 `runner.js` 已内置代跑全流程（加密、getRunBegin、sunRunExercises、sunRunExercisesDetail、随机路线生成），默认直连 `https://app.xtotoro.com/app/*`。运行需要任务数据包含：`runPoint`（含 pointList）、`session`（token/schoolId/campusId/stuNumber/phoneNumber）、`mileage`、`minTime`、`maxTime`。
