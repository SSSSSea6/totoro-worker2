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
const POLLING_DELAY = Number(process.env.WORKER_POLLING_DELAY ?? 3000);
const MAX_ATTEMPTS = Math.max(1, Number(process.env.WORKER_MAX_ATTEMPTS ?? 3));
const CREDIT_UPDATE_MAX_RETRY = 6;
const REFUND_REPAIR_INTERVAL = Math.max(30000, Number(process.env.WORKER_REFUND_REPAIR_INTERVAL ?? 60000));
const REFUND_REPAIR_BATCH_SIZE = Math.max(1, Number(process.env.WORKER_REFUND_REPAIR_BATCH_SIZE ?? 20));
const WORKER_ID = process.env.WORKER_ID || `worker-${Math.random().toString(36).slice(2, 8)}`;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
let lastRefundRepairAt = 0;

function getReservedCreditCount(userData) {
  const raw = Number(userData?.reservedCreditCount ?? 1);
  if (!Number.isFinite(raw) || raw <= 0) return 1;
  return Math.floor(raw);
}

function resolveTaskCreditInfo(userData) {
  const runMode = String(userData?.runMode || '').trim().toLowerCase();
  const explicitTable = String(userData?.creditTable || '').trim();
  const reservedCredit = Boolean(userData?.reservedCredit);
  let creditTable = '';

  if (explicitTable === 'backfill_run_credits' || explicitTable === 'sunrun_credits') {
    creditTable = explicitTable;
  } else if (runMode === 'backfill') {
    creditTable = 'backfill_run_credits';
  } else if (runMode === 'normal') {
    creditTable = 'sunrun_credits';
  } else if (userData?.customDate || userData?.customPeriod) {
    creditTable = 'backfill_run_credits';
  } else {
    creditTable = 'sunrun_credits';
  }

  return {
    userId: userData?.session?.stuNumber,
    reservedCredit,
    creditCount: getReservedCreditCount(userData),
    creditTable,
    creditLabel: creditTable === 'backfill_run_credits' ? '补跑次数' : '阳光跑次数',
    creditRefunded: Boolean(userData?.creditRefunded),
    creditRefundPending: Boolean(userData?.creditRefundPending),
  };
}

function appendTaskLog(baseMessage, extraMessage) {
  const base = String(baseMessage || '').trim();
  const extra = String(extraMessage || '').trim();
  if (!extra) return base;
  if (!base) return extra;
  if (base.includes(extra)) return base;
  return `${base}\n${extra}`;
}

async function updateTaskRow(taskId, patch, filters = {}) {
  let query = supabase.from('Tasks').update(patch).eq('id', taskId);
  for (const [key, value] of Object.entries(filters)) {
    query = query.eq(key, value);
  }
  const { data, error } = await query.select('id').maybeSingle();
  if (error) {
    throw new Error(`更新任务 ${taskId} 失败: ${error.message}`);
  }
  if (!data) {
    throw new Error(`更新任务 ${taskId} 失败: 未匹配到记录`);
  }
}

async function getOrInitCredits(table, userId, initialCredits = 1) {
  const { data, error } = await supabase
    .from(table)
    .select('credits')
    .eq('user_id', userId)
    .maybeSingle();

  if (error && error.code !== 'PGRST116') {
    throw new Error(`查询次数失败: ${error.message}`);
  }

  if (!error && data) {
    return Number(data.credits ?? 0);
  }

  const { data: inserted, error: insertError } = await supabase
    .from(table)
    .upsert({
      user_id: userId,
      credits: initialCredits,
      updated_at: new Date().toISOString(),
    })
    .select('credits')
    .maybeSingle();

  if (insertError) {
    throw new Error(`初始化次数失败: ${insertError.message}`);
  }

  return Number(inserted?.credits ?? initialCredits);
}

async function adjustCreditsWithRetry({ table, userId, delta, initialCredits = 1, minCredits = 0 }) {
  for (let i = 0; i < CREDIT_UPDATE_MAX_RETRY; i += 1) {
    const current = await getOrInitCredits(table, userId, initialCredits);
    const next = Number((current + delta).toFixed(4));

    if (!Number.isFinite(next)) {
      return { success: false, message: '次数计算异常' };
    }
    if (next < minCredits) {
      return { success: false, insufficient: true, credits: current, message: '次数不足' };
    }

    const { data: updated, error } = await supabase
      .from(table)
      .update({ credits: next, updated_at: new Date().toISOString() })
      .eq('user_id', userId)
      .eq('credits', current)
      .select('credits')
      .maybeSingle();

    if (error && error.code !== 'PGRST116') {
      return { success: false, message: error.message };
    }
    if (updated) {
      return { success: true, credits: Number(updated.credits ?? next) };
    }
  }

  return { success: false, message: '并发冲突，请重试' };
}

async function repairFailedRefunds() {
  const { data: failedJobs, error } = await supabase
    .from('Tasks')
    .select('id, user_data, result_log')
    .eq('status', 'FAILED')
    .order('id', { ascending: true })
    .limit(REFUND_REPAIR_BATCH_SIZE);

  if (error) {
    console.error(`[Worker ${WORKER_ID}] 查询待补偿返还任务失败: ${error.message}`);
    return;
  }

  for (const job of failedJobs || []) {
    const creditInfo = resolveTaskCreditInfo(job.user_data || {});
    if (
      !creditInfo.userId ||
      !creditInfo.reservedCredit ||
      creditInfo.creditRefunded ||
      !creditInfo.creditRefundPending
    ) {
      continue;
    }

    const refund = await adjustCreditsWithRetry({
      table: creditInfo.creditTable,
      userId: creditInfo.userId,
      delta: creditInfo.creditCount,
      initialCredits: 1,
      minCredits: 0,
    });

    if (!refund.success) {
      console.error(
        `[Worker ${WORKER_ID}] 自动补偿返还任务 ${job.id} 失败: ${refund.message}`,
      );
      continue;
    }

    await updateTaskRow(
      job.id,
      {
        user_data: {
          ...job.user_data,
          creditTable: creditInfo.creditTable,
          reservedCredit: true,
          reservedCreditCount: creditInfo.creditCount,
          creditRefunded: true,
          creditRefundPending: false,
          creditRefundError: null,
          creditRefundedAt: new Date().toISOString(),
        },
        result_log: appendTaskLog(
          job.result_log,
          `${creditInfo.creditLabel}已自动返还 ${creditInfo.creditCount} 次`,
        ),
      },
      { status: 'FAILED' },
    );
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
  const creditInfo = resolveTaskCreditInfo(job.user_data || {});
  const alreadyReserved = creditInfo.reservedCredit;
  const stuNumber = creditInfo.userId;
  try {
    if (stuNumber && !alreadyReserved) {
      const consume = await adjustCreditsWithRetry({
        table: creditInfo.creditTable,
        userId: stuNumber,
        delta: -creditInfo.creditCount,
        initialCredits: 1,
        minCredits: 0,
      });

      if (!consume.success) {
        const message = consume.insufficient
          ? `${creditInfo.creditLabel}不足（剩余 ${consume.credits ?? 0}）`
          : consume.message;
        throw new Error(message);
      }

      const reservedUserData = {
        ...job.user_data,
        runMode: creditInfo.creditTable === 'backfill_run_credits' ? 'backfill' : 'normal',
        creditTable: creditInfo.creditTable,
        reservedCredit: true,
        reservedCreditCount: creditInfo.creditCount,
        creditRefunded: false,
        creditRefundPending: false,
        creditRefundError: null,
      };
      job.user_data = reservedUserData;
      await updateTaskRow(job.id, { user_data: reservedUserData });
    }

    const resultLog = await executeRunTask(job.user_data);
    const userDataWithRetry = {
      ...job.user_data,
      runMode: creditInfo.creditTable === 'backfill_run_credits' ? 'backfill' : 'normal',
      creditTable: creditInfo.creditTable,
      reservedCredit: true,
      reservedCreditCount: creditInfo.creditCount,
      retryCount: currentAttempt,
      creditRefundPending: false,
      creditRefundError: null,
    };
    await updateTaskRow(job.id, {
      status: 'SUCCESS',
      result_log: resultLog,
      user_data: userDataWithRetry,
    });
    console.log(`[Worker ${WORKER_ID}] 任务 ${job.id} 成功，耗时 ${Date.now() - start} ms`);
  } catch (taskError) {
    const reachedMaxAttempts = currentAttempt >= MAX_ATTEMPTS;
    const nextStatus = reachedMaxAttempts ? 'FAILED' : 'PENDING';
    const baseLog = `第${currentAttempt}次失败: ${taskError.message}`;
    const userDataWithRetry = {
      ...job.user_data,
      runMode: creditInfo.creditTable === 'backfill_run_credits' ? 'backfill' : 'normal',
      creditTable: creditInfo.creditTable,
      reservedCredit: Boolean(job.user_data?.reservedCredit),
      reservedCreditCount: creditInfo.creditCount,
      retryCount: currentAttempt,
      creditRefunded: false,
      creditRefundPending: false,
      creditRefundError: null,
    };

    try {
      await updateTaskRow(job.id, {
        status: nextStatus,
        result_log: baseLog,
        user_data: userDataWithRetry,
      });
    } catch (updateError) {
      console.error(
        `[Worker ${WORKER_ID}] 更新任务 ${job.id} 状态为 ${nextStatus} 失败: ${updateError.message}`,
      );
      return;
    }

    if (!reachedMaxAttempts) {
      console.warn(`[Worker ${WORKER_ID}] 任务 ${job.id} 第 ${currentAttempt} 次失败，重新入队: ${taskError.message}`);
      return;
    }

    if (stuNumber && userDataWithRetry.reservedCredit) {
      const refund = await adjustCreditsWithRetry({
        table: creditInfo.creditTable,
        userId: stuNumber,
        delta: creditInfo.creditCount,
        initialCredits: 1,
        minCredits: 0,
      });

      if (refund.success) {
        await updateTaskRow(
          job.id,
          {
            result_log: appendTaskLog(
              baseLog,
              `${creditInfo.creditLabel}已返还 ${creditInfo.creditCount} 次`,
            ),
            user_data: {
              ...userDataWithRetry,
              creditRefunded: true,
              creditRefundPending: false,
              creditRefundError: null,
              creditRefundedAt: new Date().toISOString(),
            },
          },
          { status: 'FAILED' },
        );
      } else {
        await updateTaskRow(
          job.id,
          {
            result_log: appendTaskLog(
              baseLog,
              `${creditInfo.creditLabel}返还失败，等待自动补偿`,
            ),
            user_data: {
              ...userDataWithRetry,
              creditRefunded: false,
              creditRefundPending: true,
              creditRefundError: refund.message,
            },
          },
          { status: 'FAILED' },
        );
        console.error(`[Worker ${WORKER_ID}] 返还${creditInfo.creditLabel}失败: ${refund.message}`);
      }
    }

    console.error(`[Worker ${WORKER_ID}] 任务 ${job.id} 失败，耗时 ${Date.now() - start} ms: ${taskError.message}`);
  }
}

async function mainLoop() {
  console.log(`[Worker ${WORKER_ID}] \u542f\u52a8\uff0c\u5f00\u59cb\u8f6e\u8be2 Supabase`);
  while (true) {
    try {
      if (Date.now() - lastRefundRepairAt >= REFUND_REPAIR_INTERVAL) {
        lastRefundRepairAt = Date.now();
        await repairFailedRefunds();
      }
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
