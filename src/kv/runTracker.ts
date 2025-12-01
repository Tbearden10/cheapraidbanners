// ============================================================================
// Simple KV-based run tracking
// ============================================================================

interface RunInfo {
  runId: string;
  type: 'members' | 'stats';
  startedAt: number;
  completedAt?: number;
  status: 'queued' | 'processing' | 'completed' | 'failed';
  queued: number;
  processed: number;
  errors: number;
}

// Store run info in KV with 24-hour TTL
export async function trackRunStart(
  kv: KVNamespace,
  runId: string,
  type: 'members' | 'stats',
  queued: number
): Promise<void> {
  const info: RunInfo = {
    runId,
    type,
    startedAt: Date.now(),
    status: 'processing',
    queued,
    processed: 0,
    errors: 0,
  };
  
  // Store with 24-hour expiration
  await kv.put(`run:${runId}`, JSON.stringify(info), { expirationTtl: 86400 });
  
  // Also track as "latest" for quick access
  await kv.put(`latest:${type}`, runId, { expirationTtl: 86400 });
}

export async function trackRunProgress(
  kv: KVNamespace,
  runId: string,
  increment: { processed?: number; errors?: number }
): Promise<void> {
  const existing = await kv.get(`run:${runId}`, 'json') as RunInfo | null;
  if (!existing) return;
  
  existing.processed += increment.processed || 0;
  existing.errors += increment.errors || 0;
  
  await kv.put(`run:${runId}`, JSON.stringify(existing), { expirationTtl: 86400 });
}

export async function trackRunComplete(
  kv: KVNamespace,
  runId: string,
  status: 'completed' | 'failed'
): Promise<void> {
  const existing = await kv.get(`run:${runId}`, 'json') as RunInfo | null;
  if (!existing) return;
  
  existing.status = status;
  existing.completedAt = Date.now();
  
  // Keep completed runs for 24 hours
  await kv.put(`run:${runId}`, JSON.stringify(existing), { expirationTtl: 86400 });
}

export async function getRunInfo(
  kv: KVNamespace,
  runId: string
): Promise<RunInfo | null> {
  return await kv.get(`run:${runId}`, 'json') as RunInfo | null;
}

export async function getLatestRuns(
  kv: KVNamespace
): Promise<{ members?: RunInfo; stats?: RunInfo }> {
  const latestMembersId = await kv.get('latest:members');
  const latestStatsId = await kv.get('latest:stats');
  
  const [members, stats] = await Promise.all([
    latestMembersId ? getRunInfo(kv, latestMembersId) : null,
    latestStatsId ? getRunInfo(kv, latestStatsId) : null,
  ]);
  
  return {
    members: members || undefined,
    stats: stats || undefined,
  };
}