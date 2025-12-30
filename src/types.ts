// Consolidated TypeScript types for the Clan Stats app.
// Export the D1Database type (and other types) so all modules can import them.

export interface D1PreparedStatement {
  bind(...args: any[]): D1PreparedStatement;
  run(): Promise<any>;
  all(): Promise<{ results?: any[] }>;
  first(): Promise<any>;
}

export interface D1Database {
  prepare(sql: string): D1PreparedStatement;
  batch?(queries: Array<D1PreparedStatement | { sql: string; bindings?: any[] }>): Promise<any>;
}

/** Queue / Durable Object minimal types used by the app */
export interface QueueMessage<T = any> {
  body: T;
  ack(): void;
  retry(): void;
}

export interface QueueBatch<T = any> {
  messages: Array<QueueMessage<T>>;
}

export interface QueueBinding<T = any> {
  send(msg: T): Promise<void>;
}

/**
 * Durable Object storage API (minimal)
 */
export interface DurableObjectStorage {
  get(key: string): Promise<any>;
  put(key: string, value: any): Promise<void>;
  delete(key: string): Promise<void>;
  list?(options?: any): AsyncIterable<{ key: string; value?: any }>;
  setAlarm(when: number): Promise<void>;
}

export interface DurableObjectState {
  storage: DurableObjectStorage;
  id: DurableObjectId;
  blockConcurrencyWhile?: <T>(callback: () => Promise<T>) => Promise<T>;
}

export interface DurableObjectId {
  toString(): string;
}

export interface DurableObjectInstance {
  fetch(input: string | Request, init?: RequestInit): Promise<Response>;
}

export interface DurableObjectNamespace {
  idFromName(name: string): DurableObjectId;
  get(id: DurableObjectId): DurableObjectInstance;
}

export interface Env {
  // API Keys
  BUNGIE_API_KEY: string;
  API_TOKEN?: string;
  
  // Config
  ENVIRONMENT: string;
  BUNGIE_CLAN_ID: string;
  
  // Database
  DB: D1Database;
  
  // Queues
  MEMBER_STATS_QUEUE: QueueBinding<MemberJob>;
  STATS_QUEUE: QueueBinding<StatsQueueJob>;
  
  // Durable Objects
  BATCH_COORDINATOR: DurableObjectNamespace;
  RUN_TRACKER: DurableObjectNamespace;
}

/** Shapes matching DB rows (nullable where appropriate) */
export interface ClanMemberRow {
  id?: number;
  clan_id: string;
  membership_id: string;
  membership_type: number;
  display_name: string;
  is_online: number;
  last_online_status_change?: number | null;
  last_online_status_change_prev?: number | null;               // added
  last_online_status_change_resolved?: number | null;           // added
  last_online_status_change_resolved_prev?: number | null;      // added
  join_date?: string | null;
  emblem_path?: string | null;
  emblem_background_path?: string | null;
  is_active: number;
  created_at?: number;
  updated_at?: number;
}

export interface MemberDungeonStatsRow {
  id?: number;
  clan_id: string;
  membership_id: string;
  membership_type: number;
  dungeon_hash: string;
  total_clears?: number;                // added (all completions)
  total_full_clears: number;
  total_playtime_seconds: number;
  last_processed_date?: string | null;
  created_at?: number;
  updated_at?: number;
}

export interface ClanAggregateStatsRow {
  id?: number;
  clan_id: string;
  dungeon_hash: string;
  total_clears?: number;                // added (all completions)
  total_full_clears: number;
  total_playtime_seconds: number;
  active_member_count: number;
  last_updated: number;
}

export interface MemberJob {
  clanId: string;
  membershipId: string;
  membershipType: number;
  displayName: string;
  lastProcessedDate?: string | null;
  runId?: string;
}

export interface StatsQueueJob {
  clanId: string;
  membershipId: string;
  membershipType: number;
  dungeonHash: string;
  activities: Array<{
    instanceId: string;
    seconds?: number;
    date?: string;
    characterId?: string;
  }>;
  jobId: string;
  batchIndex?: number;
  totalBatches?: number;
}