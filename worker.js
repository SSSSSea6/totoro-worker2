require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const { executeRunTask } = require('./runner');

const { SUPABASE_URL, SUPABASE_SERVICE_KEY } = process.env;
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('[Worker] \u672a\u68c0\u6d4b\u5230 SUPABASE_URL \u548c SUPABASE_SERVICE_KEY \u73af\u5883\u53d8\u91cf\uff0c\u65e0\u6cd5\u542f\u52a8');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const RATE_LIMIT_DELAY = Math.max(0, Number(process.env.WORKER_RATE_LIMIT_DELAY ?? 0));
const POLLING_DELAY = Number(process.env.WORKER_POLLING_DELAY ?? 15000);
const MAX_ATTEMPTS = Math.max(1, Number(process.env.WORKER_MAX_ATTEMPTS ?? 3));
const WORKER_ID = process.env.WORKER_ID || `worker-${Math.random().toString(36).slice(2, 8)}`;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function markReservedCredit(job) {
  if (job.user_data?.reservedCredit) return;
  const newUserData = { ...job.user_data, reservedCredit: true };
  job.user_data = newUserData;
  const { error } = await supabase.from('Tasks').update({ user_data: newUserData }).eq('id', job.id);
  if (error) {
    console.warn(`[Worker ${WORKER_ID}] \u6807\u8bb0 reservedCredit \u5931\u8d25: ${error.message}`);
  }
}

async function consumeBackfillCredit(userId) {
  const { data, error } = await supabase
    .from('backfill_run_credits')
    .select('credits')
    .eq('user_id', userId)
    .maybeSingle();
  if (error && error.code !== 'PGRST116') {
    throw new Error(`查询补跑次数失败: ${error.message}`);
  }
  const currentCredits = data?.credits ?? 0;
  if (!data) {
    const { error: initError } = await supabase
      .from('backfill_run_credits')
      .insert({ user_id: userId, credits: currentCredits })
      .select('credits')
      .maybeSingle();
    if (initError) throw new Error(`初始化补跑次数失败: ${initError.message}`);
  }
  if (currentCredits < 1) {
    throw new Error('补跑次数不足');
  }
  const { error: updateError } = await supabase
    .from('backfill_run_credits')
    .update({ credits: currentCredits - 1, updated_at: new Date().toISOString() })
    .eq('user_id', userId);
  if (updateError) {
    throw new Error(`扣减补跑次数失败: ${updateError.message}`);
  }
}

async function refundBackfillCredit(userId) {
  const { data, error } = await supabase
    .from('backfill_run_credits')
    .select('credits')
    .eq('user_id', userId)
    .maybeSingle();
  if (error && error.code !== 'PGRST116') {
    throw new Error(`查询补跑次数失败: ${error.message}`);
  }
  const currentCredits = data?.credits ?? 0;
  const nextCredits = currentCredits + 1;
  const { error: updateError } = await supabase
    .from('backfill_run_credits')
    .upsert({ user_id: userId, credits: nextCredits, updated_at: new Date().toISOString() });
  if (updateError) {
    throw new Error(`返还补跑次数失败: ${updateError.message}`);
  }
}

async function acquireJob() {
  const { data: pendingJobs, error: fetchError } = await supabase
    .from('Tasks')
    .select('id, user_data')
    .eq('status', 'PENDING')
    .order('id', { ascending: true })
    .limit(1);

  if (fetchError) {
    console.error(`[Worker ${WORKER_ID}] \u83b7\u53d6\u5f85\u5904\u7406\u4efb\u52a1\u5931\u8d25:`, fetchError.message);
    return null;
  }

  const job = pendingJobs?.[0];
  if (!job) return null;

  const tryLock = async (clearLog = true) =>
    supabase
      .from('Tasks')
      .update(clearLog ? { status: 'PROCESSING', result_log: null } : { status: 'PROCESSING' })
      .eq('id', job.id)
      .eq('status', 'PENDING')
      .select('id, user_data')
      .maybeSingle();

  let { data: updatedJob, error: lockError } = await tryLock(true);
  if (lockError && /result_log/i.test(lockError.message || '')) {
    console.warn(
      `[Worker ${WORKER_ID}] Tasks.result_log \u5217\u4e0d\u53ef\u7528\uff0c\u8df3\u8fc7\u6e05\u7a7a\u65e5\u5fd7\u91cd\u8bd5\uff1a${lockError.message}`,
    );
    ({ data: updatedJob, error: lockError } = await tryLock(false));
  }

  if (lockError || !updatedJob) {
    const { data: statusRow, error: statusError } = await supabase
      .from('Tasks')
      .select('status')
      .eq('id', job.id)
      .maybeSingle();
    const currentStatus = statusRow?.status ?? '\u672a\u77e5';
    console.warn(
      `[Worker ${WORKER_ID}] \u4efb\u52a1 ${job.id} \u65e0\u6cd5\u9501\u5b9a\uff08\u5f53\u524d\u72b6\u6001\uff1a${currentStatus}\uff09`,
      lockError || statusError || '',
    );
    return null;
  }

  return updatedJob;
}

async function processJob(job) {
  const start = Date.now();
  const currentAttempt = Number(job.user_data?.retryCount ?? 0) + 1;
  console.log(`[Worker ${WORKER_ID}] 开始处理任务 ${job.id}（第 ${currentAttempt} 次尝试）`);
  const isBackfill = Boolean(job.user_data?.customDate || job.user_data?.customPeriod);
  const alreadyReserved = Boolean(job.user_data?.reservedCredit);
  const stuNumber = job.user_data?.session?.stuNumber;
  try {
    if (isBackfill && stuNumber && !alreadyReserved) {
      await consumeBackfillCredit(stuNumber);
      await markReservedCredit(job);
    }

    const resultLog = await executeRunTask(job.user_data);
    const userDataWithRetry = { ...job.user_data, retryCount: currentAttempt };
    await supabase
      .from('Tasks')
      .update({ status: 'SUCCESS', result_log: resultLog, user_data: userDataWithRetry })
      .eq('id', job.id);
    console.log(`[Worker ${WORKER_ID}] 任务 ${job.id} 成功，耗时 ${Date.now() - start} ms`);
  } catch (taskError) {
    const reachedMaxAttempts = currentAttempt >= MAX_ATTEMPTS;
    if (reachedMaxAttempts && stuNumber) {
      try {
        await refundBackfillCredit(stuNumber);
      } catch (refundError) {
        console.error(`[Worker ${WORKER_ID}] 返还补跑次数失败: ${refundError.message}`);
      }
    }

    const nextStatus = reachedMaxAttempts ? 'FAILED' : 'PENDING';
    const userDataWithRetry = { ...job.user_data, retryCount: currentAttempt };
    const { error: updateError } = await supabase
      .from('Tasks')
      .update({
        status: nextStatus,
        result_log: `第${currentAttempt}次失败: ${taskError.message}`,
        user_data: userDataWithRetry,
      })
      .eq('id', job.id);

    if (updateError) {
      console.error(`[Worker ${WORKER_ID}] 更新任务 ${job.id} 状态为 ${nextStatus} 失败: ${updateError.message}`);
    } else if (nextStatus === 'PENDING') {
      console.warn(`[Worker ${WORKER_ID}] 任务 ${job.id} 第 ${currentAttempt} 次失败，重新入队: ${taskError.message}`);
    } else {
      console.error(`[Worker ${WORKER_ID}] 任务 ${job.id} 失败，耗时 ${Date.now() - start} ms: ${taskError.message}`);
    }
  }
}

async function mainLoop() {
  console.log(`[Worker ${WORKER_ID}] \u542f\u52a8\uff0c\u5f00\u59cb\u8f6e\u8be2 Supabase`);
  while (true) {
    try {
      const job = await acquireJob();
      if (job) {
        await processJob(job);
        if (RATE_LIMIT_DELAY > 0) {
          await sleep(RATE_LIMIT_DELAY);
        }
      } else {
        await sleep(POLLING_DELAY);
      }
    } catch (error) {
      console.error(`[Worker ${WORKER_ID}] \u4e3b\u5faa\u73af\u5f02\u5e38:`, error);
      await sleep(POLLING_DELAY);
    }
  }
}

mainLoop();
