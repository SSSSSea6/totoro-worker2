require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const { executeRunTask } = require('./runner');

const { SUPABASE_URL, SUPABASE_SERVICE_KEY } = process.env;
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('[Worker] 未检测到 SUPABASE_URL 或 SUPABASE_SERVICE_KEY 环境变量，无法启动');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const RATE_LIMIT_DELAY = Number(process.env.WORKER_RATE_LIMIT_DELAY ?? 5000);
const POLLING_DELAY = Number(process.env.WORKER_POLLING_DELAY ?? 15000);
const WORKER_ID = process.env.WORKER_ID || `worker-${Math.random().toString(36).slice(2, 8)}`;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function acquireJob() {
  const { data: pendingJobs, error: fetchError } = await supabase
    .from('Tasks')
    .select('id, user_data')
    .eq('status', 'PENDING')
    .order('id', { ascending: true })
    .limit(1);

  if (fetchError) {
    console.error(`[Worker ${WORKER_ID}] 获取待处理任务失败:`, fetchError.message);
    return null;
  }

  const job = pendingJobs?.[0];
  if (!job) return null;

  const { data: updatedJob, error: lockError } = await supabase
    .from('Tasks')
    .update({ status: 'PROCESSING', result_log: null })
    .eq('id', job.id)
    .eq('status', 'PENDING')
    .select('id, user_data')
    .single();

  if (lockError || !updatedJob) {
    console.warn(`[Worker ${WORKER_ID}] 任务 ${job.id} 被其他实例锁定，跳过`);
    return null;
  }

  return updatedJob;
}

async function processJob(job) {
  console.log(`[Worker ${WORKER_ID}] 开始处理任务 ${job.id}`);
  try {
    const resultLog = await executeRunTask(job.user_data);
    await supabase
      .from('Tasks')
      .update({ status: 'SUCCESS', result_log: resultLog })
      .eq('id', job.id);
    console.log(`[Worker ${WORKER_ID}] 任务 ${job.id} 成功完成`);
  } catch (taskError) {
    await supabase
      .from('Tasks')
      .update({ status: 'FAILED', result_log: taskError.message })
      .eq('id', job.id);
    console.error(`[Worker ${WORKER_ID}] 任务 ${job.id} 失败: ${taskError.message}`);
  }
}

async function mainLoop() {
  console.log(`[Worker ${WORKER_ID}] 启动，开始轮询 Supabase`);
  while (true) {
    try {
      const job = await acquireJob();
      if (job) {
        await processJob(job);
        await sleep(RATE_LIMIT_DELAY);
      } else {
        await sleep(POLLING_DELAY);
      }
    } catch (error) {
      console.error(`[Worker ${WORKER_ID}] 主循环异常:`, error);
      await sleep(POLLING_DELAY);
    }
  }
}

mainLoop();
