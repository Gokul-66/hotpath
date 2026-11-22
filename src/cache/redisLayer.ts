import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";

export interface Entry<V> {
  value: V | null;
  softExpiresAt: number;
  hardExpiresAt: number;
}

const INVALIDATION_CHANNEL = "hotpath:invalidate";
const FLUSH_ALL = "*";
// Must outlive any origin load that captured an older version.
const VERSION_TTL_MS = 3_600_000;

const BREAKER_THRESHOLD = 5;
const BREAKER_OPEN_MS = 1_000;

// Versions are unique tokens, created on first read. If one is evicted, expires or is lost in a
// restart, the next comparison fails and nothing is stored. A counter would restart at a value seen before.
const READ_VERSION = `
local current = redis.call('GET', KEYS[1])
if current then return current end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return ARGV[1]`;

// Stores the value only if no invalidation happened since the version was read.
const SET_IF_VERSION = `
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1`;

// Replacing the version before deleting blocks any in-progress fill holding an older value.
const INVALIDATE = `
redis.call('SET', KEYS[2], ARGV[4], 'PX', ARGV[2])
redis.call('DEL', KEYS[1])
redis.call('PUBLISH', ARGV[1], ARGV[3])
return 1`;

// Releases a lock only if this caller still owns it.
const UNLOCK = `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0`;

export class RedisUnavailable extends Error {}

/** Shared cache layer. Every failure surfaces as RedisUnavailable, so callers can degrade instead of failing. */
export class RedisLayer {
  readonly stats = { errors: 0, breakerOpens: 0 };
  private readonly client: Redis;
  private readonly subscriber: Redis;
  private consecutiveFailures = 0;
  private openUntil = 0;

  constructor(
    url: string,
    private readonly handlers: { onInvalidate(key: string): void; onFlush(): void; onReconnect(): void },
    commandTimeoutMs = 100
  ) {
    const options = {
      commandTimeout: commandTimeoutMs,
      connectTimeout: 1_000,
      enableOfflineQueue: false,
      maxRetriesPerRequest: 0,
      retryStrategy: (attempt: number) => Math.min(50 * attempt, 500),
    };
    this.client = new Redis(url, options);
    // SUBSCRIBE is sent on connect, so the subscriber needs the offline queue and no command timeout.
    this.subscriber = new Redis(url, { ...options, enableOfflineQueue: true, commandTimeout: undefined, maxRetriesPerRequest: null });
    this.client.on("error", () => {});
    this.subscriber.on("error", () => {});
    this.client.on("ready", () => this.handlers.onReconnect());

    let subscribedBefore = false;
    this.subscriber.on("ready", () => {
      // Messages published while disconnected are lost, so anything cached locally is suspect.
      if (subscribedBefore) this.handlers.onFlush();
      subscribedBefore = true;
    });
    this.subscriber.on("message", (_channel: string, key: string) => {
      if (key === FLUSH_ALL) this.handlers.onFlush();
      else this.handlers.onInvalidate(key);
    });
    this.subscriber.subscribe(INVALIDATION_CHANNEL).catch(() => {});
  }

  async ready(timeoutMs = 5_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (this.client.status !== "ready" || this.subscriber.status !== "ready") {
      if (Date.now() > deadline) throw new RedisUnavailable("not connected");
      await new Promise((r) => setTimeout(r, 10));
    }
    await this.subscriber.subscribe(INVALIDATION_CHANNEL);
  }

  /** After repeated failures, fail immediately for a while instead of making every request wait out a timeout. */
  private async guard<T>(fn: () => Promise<T>): Promise<T> {
    if (Date.now() < this.openUntil) throw new RedisUnavailable("circuit open");
    try {
      const result = await fn();
      this.consecutiveFailures = 0;
      return result;
    } catch (err) {
      this.stats.errors++;
      if (++this.consecutiveFailures >= BREAKER_THRESHOLD) {
        this.consecutiveFailures = 0;
        this.openUntil = Date.now() + BREAKER_OPEN_MS;
        this.stats.breakerOpens++;
      }
      throw new RedisUnavailable((err as Error).message);
    }
  }

  get<V>(key: string): Promise<Entry<V> | null> {
    return this.guard(async () => {
      const raw = await this.client.get(`val:${key}`);
      return raw === null ? null : (JSON.parse(raw) as Entry<V>);
    });
  }

  version(key: string): Promise<string> {
    return this.guard(async () => String(await this.client.eval(READ_VERSION, 1, `ver:${key}`, randomUUID(), VERSION_TTL_MS)));
  }

  setIfVersion<V>(key: string, entry: Entry<V>, version: string, ttlMs: number): Promise<boolean> {
    return this.guard(async () => {
      const stored = await this.client.eval(
        SET_IF_VERSION, 2, `val:${key}`, `ver:${key}`, version, JSON.stringify(entry), Math.max(1, Math.round(ttlMs))
      );
      return stored === 1;
    });
  }

  /** Unconditional write. Exists so tests can show what the version guard prevents. */
  setUnguarded<V>(key: string, entry: Entry<V>, ttlMs: number): Promise<boolean> {
    return this.guard(async () => {
      await this.client.set(`val:${key}`, JSON.stringify(entry), "PX", Math.max(1, Math.round(ttlMs)));
      return true;
    });
  }

  invalidate(key: string): Promise<void> {
    return this.guard(async () => {
      await this.client.eval(INVALIDATE, 2, `val:${key}`, `ver:${key}`, INVALIDATION_CHANNEL, VERSION_TTL_MS, key, randomUUID());
    });
  }

  tryLock(key: string, token: string, ttlMs: number): Promise<boolean> {
    return this.guard(async () => (await this.client.set(`lock:${key}`, token, "PX", ttlMs, "NX")) === "OK");
  }

  unlock(key: string, token: string): Promise<void> {
    return this.guard(async () => {
      await this.client.eval(UNLOCK, 1, `lock:${key}`, token);
    });
  }

  async close(): Promise<void> {
    this.client.disconnect();
    this.subscriber.disconnect();
  }
}
