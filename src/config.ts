import { z } from "zod";
import { type CacheOptions, DEFAULTS } from "./cache/tieredCache.js";

const flag = (fallback: boolean) => z.stringbool().default(fallback);
const int = (fallback: number) => z.coerce.number().int().min(0).default(fallback);

const schema = z.object({
  DATABASE_URL: z.string().min(1),
  REDIS_URL: z.string().optional(),
  PORT: int(3000),
  POOL_SIZE: int(10),
  L1_CAPACITY: int(DEFAULTS.l1Capacity),
  L1_TTL_MS: int(DEFAULTS.l1TtlMs),
  SOFT_TTL_MS: int(DEFAULTS.softTtlMs),
  HARD_TTL_MS: int(DEFAULTS.hardTtlMs),
  TINY_LFU: flag(DEFAULTS.tinyLfu),
  COALESCE: flag(DEFAULTS.coalesce),
  DISTRIBUTED_LOCK: flag(DEFAULTS.distributedLock),
  STALE_WHILE_REVALIDATE: flag(DEFAULTS.staleWhileRevalidate),
  // Reintroduces a known bug so the benchmark can show it. Never set outside it.
  BENCH_DISABLE_VERSION_GUARD: flag(false),
});

export interface Config {
  databaseUrl: string;
  port: number;
  poolSize: number;
  cache: Partial<CacheOptions>;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const e = schema.parse(env);
  return {
    databaseUrl: e.DATABASE_URL,
    port: e.PORT,
    poolSize: e.POOL_SIZE,
    cache: {
      redisUrl: e.REDIS_URL || undefined,
      l1Capacity: e.L1_CAPACITY,
      l1TtlMs: e.L1_TTL_MS,
      softTtlMs: e.SOFT_TTL_MS,
      hardTtlMs: e.HARD_TTL_MS,
      tinyLfu: e.TINY_LFU,
      coalesce: e.COALESCE,
      distributedLock: e.DISTRIBUTED_LOCK,
      staleWhileRevalidate: e.STALE_WHILE_REVALIDATE,
      versionGuard: !e.BENCH_DISABLE_VERSION_GUARD,
    },
  };
}
