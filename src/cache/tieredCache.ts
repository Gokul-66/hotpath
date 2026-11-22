import { randomUUID } from "node:crypto";
import { LruCache } from "./lru.js";
import { type Entry, RedisLayer } from "./redisLayer.js";
import { SingleFlight } from "./singleflight.js";
import { TinyLfu } from "./tinylfu.js";

export type Source = "l1" | "l2" | "origin" | "stale" | "coalesced";

export interface Result<V> {
  value: V | null;
  source: Source;
}

export interface CacheOptions {
  /** Entries held in process. 0 turns the layer off. */
  l1Capacity: number;
  /** Upper bound on how long one process may serve an entry without rechecking Redis. */
  l1TtlMs: number;
  tinyLfu: boolean;
  /** Unset turns the shared layer off. */
  redisUrl?: string;
  redisTimeoutMs: number;
  /** Fresh until softTtl. Between soft and hard it may be served stale while one caller refreshes it. */
  softTtlMs: number;
  hardTtlMs: number;
  /** "Not found" is cached briefly, so requests for missing keys cannot hammer the origin. */
  negativeTtlMs: number;
  /** Spreads expiry times, so entries cached together do not all expire together. */
  jitter: number;
  coalesce: boolean;
  distributedLock: boolean;
  lockTtlMs: number;
  lockWaitMs: number;
  staleWhileRevalidate: boolean;
  /** Off only to demonstrate the bug it prevents. */
  versionGuard: boolean;
}

export const DEFAULTS: CacheOptions = {
  l1Capacity: 500,
  l1TtlMs: 5_000,
  tinyLfu: true,
  redisTimeoutMs: 100,
  softTtlMs: 30_000,
  hardTtlMs: 60_000,
  negativeTtlMs: 2_000,
  jitter: 0.1,
  coalesce: true,
  distributedLock: true,
  lockTtlMs: 5_000,
  lockWaitMs: 2_000,
  staleWhileRevalidate: true,
  versionGuard: true,
};

interface Fill {
  invalidated: boolean;
}

interface L1Entry<V> extends Entry<V> {
  l1ExpiresAt: number;
}

const LOCK_POLL_MS = 15;
const MAX_PENDING_INVALIDATIONS = 10_000;

/** Read-through cache: in-process LRU, then Redis, then the origin. */
export class TieredCache<V> {
  readonly stats = {
    l1: 0,
    l2: 0,
    stale: 0,
    coalesced: 0,
    originLoads: 0,
    lockWaits: 0,
    lockTimeouts: 0,
    guardRejections: 0,
    invalidationRetries: 0,
  };
  readonly options: CacheOptions;
  private readonly l1?: LruCache<L1Entry<V>>;
  private readonly redis?: RedisLayer;
  private readonly flights = new SingleFlight<Result<V>>();
  /** Fills in progress per key, so an invalidation can mark each one that started before it. */
  private readonly filling = new Map<string, Set<Fill>>();
  private readonly pendingInvalidations = new Set<string>();

  constructor(
    private readonly load: (key: string) => Promise<V | null>,
    options: Partial<CacheOptions> = {}
  ) {
    this.options = { ...DEFAULTS, ...options };
    const o = this.options;
    if (o.l1Capacity > 0) this.l1 = new LruCache(o.l1Capacity, o.tinyLfu ? new TinyLfu(o.l1Capacity) : undefined);
    if (o.redisUrl) {
      this.redis = new RedisLayer(
        o.redisUrl,
        {
          onInvalidate: (key) => this.dropLocal(key),
          onFlush: () => this.flushLocal(),
          onReconnect: () => void this.retryInvalidations(),
        },
        o.redisTimeoutMs
      );
    }
  }

  async ready(): Promise<void> {
    await this.redis?.ready();
  }

  async close(): Promise<void> {
    await this.redis?.close();
  }

  redisStats() {
    return this.redis?.stats ?? null;
  }

  async get(key: string): Promise<Result<V>> {
    const now = Date.now();
    const local = this.l1?.get(key);
    if (local && now < local.l1ExpiresAt) {
      if (now < local.softExpiresAt) {
        this.stats.l1++;
        return { value: local.value, source: "l1" };
      }
      if (this.options.staleWhileRevalidate && now < local.hardExpiresAt) return this.serveStale(key, local);
    }

    if (!this.options.coalesce) return this.fetch(key);
    const { promise, shared } = this.flights.do(key, () => this.fetch(key));
    if (!shared) return promise;
    this.stats.coalesced++;
    return { value: (await promise).value, source: "coalesced" };
  }

  /** Call after the write has committed. The local copy is dropped even if Redis is unreachable. */
  async invalidate(key: string): Promise<void> {
    this.dropLocal(key);
    if (!this.redis) return;
    try {
      await this.redis.invalidate(key);
      this.pendingInvalidations.delete(key);
    } catch {
      // Other processes may serve the old value until its TTL. Retried on reconnect.
      if (this.pendingInvalidations.size < MAX_PENDING_INVALIDATIONS) this.pendingInvalidations.add(key);
    }
  }

  private async fetch(key: string): Promise<Result<V>> {
    const fill = this.beginFill(key);
    try {
      const cached = await this.fromRedis(key);
      if (cached) {
        const now = Date.now();
        if (now < cached.softExpiresAt) {
          this.stats.l2++;
          this.storeLocal(key, cached, fill);
          return { value: cached.value, source: "l2" };
        }
        if (this.options.staleWhileRevalidate && now < cached.hardExpiresAt) return this.serveStale(key, cached);
      }
      return await this.fill(key, fill);
    } finally {
      this.endFill(key, fill);
    }
  }

  /** Loads from the origin, letting one process at a time do so when the distributed lock is on. */
  private async fill(key: string, fill: Fill): Promise<Result<V>> {
    const token = randomUUID();
    let locked = false;
    try {
      if (this.redis && this.options.distributedLock) {
        try {
          locked = await this.redis.tryLock(key, token, this.options.lockTtlMs);
          if (!locked) locked = await this.waitForLockOrEntry(key, token);
        } catch {
          // No Redis, no shared lock. In-process coalescing still applies.
        }
        // Whether the lock was won or waited for, another process may have filled the key meanwhile.
        const entry = await this.freshFromRedis(key);
        if (entry) {
          this.stats.l2++;
          this.storeLocal(key, entry, fill);
          return { value: entry.value, source: "l2" };
        }
      }
      return await this.loadAndStore(key, fill);
    } finally {
      if (locked) await this.redis!.unlock(key, token).catch(() => {});
    }
  }

  private async loadAndStore(key: string, fill: Fill): Promise<Result<V>> {
    // Read before loading: a write that lands during the load changes it and blocks the store.
    const version = await this.redis?.version(key).catch(() => null);
    this.stats.originLoads++;
    const value = await this.load(key);
    const entry = this.makeEntry(value);

    let mayStoreLocally = true;
    if (this.redis) {
      const ttl = entry.hardExpiresAt - Date.now();
      try {
        if (!this.options.versionGuard) {
          await this.redis.setUnguarded(key, entry, ttl);
        } else if (version === null || version === undefined) {
          // Redis failed before the load, so nothing proves this value is current.
        } else if (!(await this.redis.setIfVersion(key, entry, version, ttl))) {
          this.stats.guardRejections++;
          mayStoreLocally = false;
        }
      } catch {
        // Redis is down. Serve from the process, bounded by the L1 TTL.
      }
    }
    if (mayStoreLocally) this.storeLocal(key, entry, fill);
    return { value, source: "origin" };
  }

  private serveStale(key: string, entry: Entry<V>): Result<V> {
    this.stats.stale++;
    // Keyed apart from get(), because this may run inside a flight for the same key.
    this.flights
      .do(`refresh:${key}`, async () => {
        const fill = this.beginFill(key);
        try {
          return await this.refresh(key, fill);
        } finally {
          this.endFill(key, fill);
        }
      })
      .promise.catch(() => {});
    return { value: entry.value, source: "stale" };
  }

  /** Background refresh. If another process holds the lock, it is already refreshing. */
  private async refresh(key: string, fill: Fill): Promise<Result<V>> {
    const token = randomUUID();
    let locked = false;
    if (this.redis && this.options.distributedLock) {
      try {
        locked = await this.redis.tryLock(key, token, this.options.lockTtlMs);
        if (!locked) return { value: null, source: "stale" };
      } catch {
        // Refresh without the lock.
      }
    }
    try {
      const entry = await this.freshFromRedis(key);
      if (entry) {
        this.storeLocal(key, entry, fill);
        return { value: entry.value, source: "l2" };
      }
      return await this.loadAndStore(key, fill);
    } finally {
      if (locked) await this.redis!.unlock(key, token).catch(() => {});
    }
  }

  /**
   * Returns once the holder has filled the key (false) or the lock becomes free (true). The lock can
   * be released with nothing stored, when a write made the holder's value stale.
   */
  private async waitForLockOrEntry(key: string, token: string): Promise<boolean> {
    this.stats.lockWaits++;
    const deadline = Date.now() + this.options.lockWaitMs;
    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, LOCK_POLL_MS));
      const entry = await this.redis!.get<V>(key);
      if (entry && Date.now() < entry.softExpiresAt) return false;
      if (await this.redis!.tryLock(key, token, this.options.lockTtlMs)) return true;
    }
    // The holder is slow or dead. Availability wins over protection.
    this.stats.lockTimeouts++;
    return false;
  }

  private async freshFromRedis(key: string): Promise<Entry<V> | null> {
    const entry = await this.fromRedis(key);
    return entry && Date.now() < entry.softExpiresAt ? entry : null;
  }

  private async fromRedis(key: string): Promise<Entry<V> | null> {
    if (!this.redis) return null;
    try {
      return await this.redis.get<V>(key);
    } catch {
      return null;
    }
  }

  private makeEntry(value: V | null): Entry<V> {
    const now = Date.now();
    if (value === null) {
      const expires = now + this.options.negativeTtlMs;
      return { value, softExpiresAt: expires, hardExpiresAt: expires };
    }
    const spread = 1 + (Math.random() * 2 - 1) * this.options.jitter;
    const soft = now + this.options.softTtlMs * spread;
    return { value, softExpiresAt: soft, hardExpiresAt: Math.max(soft, now + this.options.hardTtlMs * spread) };
  }

  private storeLocal(key: string, entry: Entry<V>, fill: Fill): void {
    // An invalidation that arrived during this fill means the value may predate the write.
    if (!this.l1 || (this.options.versionGuard && fill.invalidated)) return;
    this.l1.set(key, { ...entry, l1ExpiresAt: Date.now() + this.options.l1TtlMs });
  }

  private beginFill(key: string): Fill {
    const fill: Fill = { invalidated: false };
    let fills = this.filling.get(key);
    if (!fills) {
      fills = new Set();
      this.filling.set(key, fills);
    }
    fills.add(fill);
    return fill;
  }

  private endFill(key: string, fill: Fill): void {
    const fills = this.filling.get(key);
    fills?.delete(fill);
    if (fills?.size === 0) this.filling.delete(key);
  }

  private dropLocal(key: string): void {
    this.l1?.delete(key);
    for (const fill of this.filling.get(key) ?? []) fill.invalidated = true;
    if (this.options.versionGuard) {
      this.flights.forget(key);
      this.flights.forget(`refresh:${key}`);
    }
  }

  private flushLocal(): void {
    this.l1?.clear();
    for (const fills of this.filling.values()) for (const fill of fills) fill.invalidated = true;
    if (this.options.versionGuard) this.flights.forgetAll();
  }

  private async retryInvalidations(): Promise<void> {
    for (const key of [...this.pendingInvalidations]) {
      this.stats.invalidationRetries++;
      await this.invalidate(key);
    }
  }
}
