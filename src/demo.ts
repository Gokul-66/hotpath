import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Fastify, { type FastifyInstance, type FastifyRequest } from "fastify";
import type pg from "pg";
import { type CacheOptions, TieredCache } from "./cache/tieredCache.js";
import { loadProduct } from "./origin.js";
import { RedisProxy } from "./redisProxy.js";

// The demo serves only its page, its experiments and health checks. The product and price
// routes are not registered, because they have no authentication.
const PAGE_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "public", "index.html");
const REQUESTS_PER_MINUTE = 30;
const MAX_RUNNING = 3;
const STAMPEDE_REQUESTS = 60;
const OUTAGE_READS = 20;
const OLD_PRICE = 1_000;
const NEW_PRICE = 2_500;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const round = (ms: number) => Math.round(ms * 100) / 100;

class Busy extends Error {
  constructor(message: string) {
    super(message);
  }
}

/** Render's edge sets cf-connecting-ip. Without it, every visitor would share the proxy's address. */
function visitor(req: FastifyRequest): string {
  const ip = req.headers["cf-connecting-ip"];
  return typeof ip === "string" ? ip : req.ip;
}

/** Fixed window per visitor, plus a cap on experiments running at once. In memory, for one instance. */
function createLimiter() {
  const windows = new Map<string, { start: number; count: number }>();
  let running = 0;
  return async <T>(req: FastifyRequest, fn: () => Promise<T>): Promise<T> => {
    const now = Date.now();
    const key = visitor(req);
    const w = windows.get(key);
    if (!w || now - w.start > 60_000) {
      if (windows.size > 10_000) windows.clear();
      windows.set(key, { start: now, count: 1 });
    } else if (++w.count > REQUESTS_PER_MINUTE) {
      throw new Busy("Too many experiments. Try again in a minute.");
    }
    if (running >= MAX_RUNNING) throw new Busy("Other visitors are running experiments. Try again in a few seconds.");
    running++;
    try {
      return await fn();
    } finally {
      running--;
    }
  };
}

export interface Demo {
  app: FastifyInstance;
  close(): Promise<void>;
}

export async function createDemo(pool: pg.Pool, redisUrl: string): Promise<Demo> {
  const loads = new Map<string, number>();
  const gates = new Map<string, { entered: () => void; wait: Promise<void> }>();

  /** Keys are `product:<id>:<run>` or `price:<uuid>`. The run suffix makes every experiment start cold. */
  const load = async (key: string): Promise<unknown> => {
    loads.set(key, (loads.get(key) ?? 0) + 1);
    const [kind, id] = key.split(":");
    if (kind === "product") return loadProduct(pool, Number(id));
    const { rows } = await pool.query("SELECT price_cents FROM demo_prices WHERE id = $1", [id]);
    const gate = gates.get(key);
    if (gate) {
      gates.delete(key);
      gate.entered();
      await gate.wait;
    }
    return rows[0] ? { priceCents: rows[0].price_cents } : null;
  };
  const takeLoads = (key: string) => {
    const n = loads.get(key) ?? 0;
    loads.delete(key);
    return n;
  };

  // Each instance has its own memory and Redis connections, so three behave like three servers.
  const servers = (n: number, options: Partial<CacheOptions>) =>
    Array.from({ length: n }, () => new TieredCache<unknown>(load, { tinyLfu: false, redisTimeoutMs: 500, ...options }));
  const guarded = servers(3, { redisUrl });
  const unguarded = servers(2, { redisUrl, versionGuard: false });
  const unprotected = servers(3, { coalesce: false, distributedLock: false });
  await Promise.all([...guarded, ...unguarded].map((c) => c.ready()));

  const productCount = Number((await pool.query("SELECT count(*) FROM products")).rows[0].count);
  const coldProduct = () => `product:${1 + Math.floor(Math.random() * productCount)}:${randomUUID()}`;

  async function layers() {
    const key = coldProduct();
    const steps = [];
    for (const [server, cache] of [["A", guarded[0]], ["B", guarded[1]], ["B", guarded[1]]] as const) {
      const t = performance.now();
      const { source } = await cache.get(key);
      steps.push({ server, source, ms: round(performance.now() - t) });
    }
    return { steps, databaseLoads: takeLoads(key) };
  }

  async function stampede() {
    const burst = async (caches: Array<TieredCache<unknown>>) => {
      const key = coldProduct();
      const t = performance.now();
      await Promise.all(Array.from({ length: STAMPEDE_REQUESTS }, (_, i) => caches[i % caches.length].get(key)));
      const ms = Math.round(performance.now() - t);
      return { databaseLoads: takeLoads(key), ms };
    };
    return { requests: STAMPEDE_REQUESTS, unprotected: await burst(unprotected), protected: await burst(guarded) };
  }

  /** A reader is paused between reading the old price and storing it, while a writer changes the price. */
  async function race() {
    const run = async ([a, b]: Array<TieredCache<unknown>>) => {
      const id = randomUUID();
      const key = `price:${id}`;
      await pool.query("INSERT INTO demo_prices (id, price_cents) VALUES ($1, $2)", [id, OLD_PRICE]);
      let release!: () => void;
      try {
        const entered = new Promise<void>((resolve) => {
          gates.set(key, { entered: resolve, wait: new Promise((r) => (release = r)) });
        });
        const slowRead = a.get(key);
        // If the load fails before reaching the gate, the read rejects and this does not wait forever.
        await Promise.race([entered, slowRead]);
        await pool.query("UPDATE demo_prices SET price_cents = $2 WHERE id = $1", [id, NEW_PRICE]);
        await b.invalidate(key);
        release();
        const price = (r: { value: unknown }) => (r.value as { priceCents: number }).priceCents;
        const slowReader = price(await slowRead);
        await sleep(50);
        return { slowReader, laterOnA: price(await a.get(key)), laterOnB: price(await b.get(key)), database: NEW_PRICE };
      } finally {
        release?.();
        gates.delete(key);
        loads.delete(key);
        await pool.query("DELETE FROM demo_prices WHERE id = $1", [id]);
      }
    };
    return { oldPrice: OLD_PRICE, newPrice: NEW_PRICE, unguarded: await run(unguarded), guarded: await run(guarded) };
  }

  /** Redis itself stays up. A private proxy in front of it is cut, so other visitors are unaffected. */
  async function outage() {
    const proxy = new RedisProxy(redisUrl);
    const cache = new TieredCache<unknown>(load, { redisUrl: await proxy.start(), tinyLfu: false, redisTimeoutMs: 500 });
    const phase = async () => {
      const key = coldProduct();
      const t = performance.now();
      const results = await Promise.allSettled(Array.from({ length: OUTAGE_READS }, () => cache.get(key)));
      const ms = Math.round(performance.now() - t);
      return { served: results.filter((r) => r.status === "fulfilled").length, databaseLoads: takeLoads(key), ms };
    };
    try {
      await cache.ready();
      const healthy = await phase();
      proxy.cut();
      await sleep(50);
      const down = await phase();
      proxy.restore();
      const t = performance.now();
      let recoveredMs: number | null = null;
      while (performance.now() - t < 5_000) {
        const key = coldProduct();
        await cache.get(key);
        const shared = (await guarded[2].get(key)).source === "l2";
        loads.delete(key);
        if (shared) {
          recoveredMs = Math.round(performance.now() - t);
          break;
        }
        await sleep(100);
      }
      return { reads: OUTAGE_READS, healthy, down, recoveredMs };
    } finally {
      await cache.close();
      await proxy.stop();
    }
  }

  const app = Fastify({ bodyLimit: 1024 });
  const limited = createLimiter();

  app.setErrorHandler((err: any, _req, reply) => {
    if (err instanceof Busy) return reply.status(429).send({ message: err.message });
    if (err.statusCode && err.statusCode < 500) return reply.status(err.statusCode).send({ message: "Bad request" });
    console.error(JSON.stringify({ msg: "demo.failed", error: err.message }));
    return reply.status(503).send({ message: "The experiment failed. Try again in a moment." });
  });

  app.get("/", async (_req, reply) => {
    reply.header("cache-control", "no-cache");
    return reply.type("text/html; charset=utf-8").send(await readFile(PAGE_PATH, "utf8"));
  });
  app.get("/healthz", async () => ({ status: "ok" }));
  app.post("/demo/layers", (req) => limited(req, layers));
  app.post("/demo/stampede", (req) => limited(req, stampede));
  app.post("/demo/race", (req) => limited(req, race));
  app.post("/demo/outage", (req) => limited(req, outage));

  return {
    app,
    async close() {
      await app.close();
      await Promise.all([...guarded, ...unguarded, ...unprotected].map((c) => c.close()));
    },
  };
}
