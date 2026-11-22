import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import EmbeddedPostgres from "embedded-postgres";
import { Redis } from "ioredis";
import type pg from "pg";
import { createPool, migrate } from "../src/db.js";

export { RedisProxy } from "../src/redisProxy.js";

export const REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:6399";

export async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, () => {
      const { port } = server.address() as net.AddressInfo;
      server.close(() => resolve(port));
    });
  });
}

export interface TestDb {
  url: string;
  pool: pg.Pool;
  stop(): Promise<void>;
}

/** Clean shutdown. The library force-kills on Windows, which can leave a child holding the test runner's pipes. */
function stopPostgres(server: EmbeddedPostgres, databaseDir: string): void {
  const platform = process.platform === "win32" ? "windows" : process.platform;
  const pgCtl = path.join("node_modules", "@embedded-postgres", `${platform}-${process.arch}`, "native", "bin", "pg_ctl");
  const result = spawnSync(pgCtl, ["stop", "-D", databaseDir, "-m", "fast", "-w"], { stdio: "ignore" });
  if (result.status !== 0) throw new Error(`pg_ctl stop failed with status ${result.status}`);
  // Already stopped, so the library's exit hook must not wait for it.
  (server as unknown as { process?: unknown }).process = undefined;
}

export async function startDb(poolSize = 20): Promise<TestDb> {
  const databaseDir = mkdtempSync(path.join(os.tmpdir(), "hotpath-pg-"));
  const port = await freePort();
  const server = new EmbeddedPostgres({
    databaseDir,
    user: "hot",
    password: "hot",
    port,
    persistent: true,
    postgresFlags: ["-c", "max_connections=200"],
    onLog: () => {},
    onError: () => {},
  });
  await server.initialise();
  await server.start();
  await server.createDatabase("hot");
  const url = `postgres://hot:hot@localhost:${port}/hot`;
  const pool = createPool(url, poolSize);
  await migrate(pool);
  return {
    url,
    pool,
    async stop() {
      await pool.end();
      stopPostgres(server, databaseDir);
      rmSync(databaseDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 });
    },
  };
}

/** Fails loudly when Redis is missing, so tests never pass by skipping the shared layer. */
export async function connectRedis(): Promise<Redis> {
  const redis = new Redis(REDIS_URL, { lazyConnect: true, maxRetriesPerRequest: 1, retryStrategy: () => null });
  redis.on("error", () => {});
  try {
    await redis.connect();
  } catch {
    throw new Error(`Redis is required at ${REDIS_URL}. Start one with: docker compose up -d redis`);
  }
  return redis;
}

export function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  return sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)];
}

/** Seeded, so every run draws the same sequence. */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
  };
}

export function zipfCdf(n: number, exponent: number): Float64Array {
  const cdf = new Float64Array(n);
  let total = 0;
  for (let i = 0; i < n; i++) {
    total += 1 / (i + 1) ** exponent;
    cdf[i] = total;
  }
  return cdf;
}

/** Draws 0..n-1 with Zipfian popularity. Building is O(n) and each draw is a binary search, O(log n). */
export function zipf(n: number, exponent: number, random: () => number): () => number {
  const cdf = zipfCdf(n, exponent);
  const total = cdf[n - 1];
  return () => {
    const target = random() * total;
    let lo = 0;
    let hi = n - 1;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (cdf[mid] < target) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  };
}
