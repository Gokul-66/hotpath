// Real service processes, Postgres and Redis. Needs Redis at REDIS_URL.
import { type ChildProcess, spawn } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import os from "node:os";
import { Worker } from "node:worker_threads";
import { Pool as HttpPool } from "undici";
import { DEFAULT_SEED, seed } from "../src/db.js";
import { REDIS_URL, connectRedis, freePort, percentile, startDb, zipfCdf } from "./lib.js";

const SECONDS = Number(process.env.BENCH_SECONDS ?? 10);
const LOAD_THREADS = 8;
const CONNECTIONS_PER_THREAD = 8;
const CONCURRENCY = LOAD_THREADS * CONNECTIONS_PER_THREAD;
const REPEATS = 3;
const median = (values: number[]) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const ms = (n: number) => (n >= 100 ? n.toFixed(0) : n.toFixed(1));

const redis = await connectRedis();
const db = await startDb();
await seed(db.pool);
console.log("seeded");

/** Removes only this project's keys, in case Redis is shared. */
async function clearRedis(): Promise<void> {
  for (const prefix of ["val:product:*", "ver:product:*", "lock:product:*"]) {
    let cursor = "0";
    do {
      const [next, keys] = await redis.scan(cursor, "MATCH", prefix, "COUNT", 1_000);
      if (keys.length) await redis.del(...keys);
      cursor = next;
    } while (cursor !== "0");
  }
}

interface Api {
  port: number;
  proc: ChildProcess;
  http: HttpPool;
  stop(): Promise<void>;
}

async function startApi(env: Record<string, string>): Promise<Api> {
  const port = await freePort();
  const proc = spawn(process.execPath, ["--import", "tsx", "src/server.ts"], {
    stdio: "ignore",
    env: { ...process.env, REDIS_URL: "", DATABASE_URL: db.url, PORT: String(port), POOL_SIZE: "10", ...env },
  });
  const http = new HttpPool(`http://127.0.0.1:${port}`, { connections: 200 });
  for (let i = 0; ; i++) {
    try {
      const res = await http.request({ path: "/readyz", method: "GET" });
      await res.body.dump();
      if (res.statusCode === 200) break;
    } catch {}
    if (i > 200) throw new Error("API did not start");
    await sleep(50);
  }
  return {
    port,
    proc,
    http,
    async stop() {
      await http.destroy();
      proc.kill();
      await sleep(100);
    },
  };
}

async function getProduct(api: Api, id: number): Promise<{ status: number; source: string; price: number }> {
  const res = await api.http.request({ path: `/products/${id}`, method: "GET" });
  const body = (await res.body.json()) as { priceCents: number };
  return { status: res.statusCode, source: String(res.headers["x-cache"]), price: body.priceCents };
}

async function putPrice(api: Api, id: number, priceCents: number): Promise<void> {
  const res = await api.http.request({
    path: `/products/${id}/price`,
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ priceCents }),
  });
  await res.body.dump();
  if (res.statusCode !== 200) throw new Error(`PUT failed: ${res.statusCode}`);
}

async function metrics(api: Api): Promise<any> {
  return (await api.http.request({ path: "/metrics", method: "GET" })).body.json();
}

async function resetMetrics(api: Api): Promise<void> {
  await (await api.http.request({ path: "/metrics/reset", method: "POST" })).body.dump();
}

interface LayerRun {
  rps: number;
  p50: number;
  p95: number;
  p99: number;
  l1: number;
  redisPerSecond: number;
  originPerSecond: number;
  busy: number;
  failed: number;
}

// 1. Layers on and off, under a skewed read workload.
async function layers(): Promise<string[]> {
  const variants: Array<[string, Record<string, string>, string?]> = [
    ["HTTP ceiling (an empty endpoint)", { L1_CAPACITY: "0" }, "/healthz"],
    ["No cache", { L1_CAPACITY: "0", COALESCE: "false" }],
    ["Redis only", { L1_CAPACITY: "0", REDIS_URL }],
    ["In-process only (LRU)", { TINY_LFU: "false" }],
    ["In-process only (TinyLFU)", { TINY_LFU: "true" }],
    ["Both layers", { REDIS_URL }],
  ];
  const out: string[] = [];
  for (const [name, env, fixedPath] of variants) {
    await clearRedis();
    // Long TTLs, so the comparison shows steady state and not expiry.
    const api = await startApi({ SOFT_TTL_MS: "600000", HARD_TTL_MS: "1200000", L1_TTL_MS: "600000", ...env });
    const cdf = zipfCdf(DEFAULT_SEED.products, 1);
    const threads = Array.from(
      { length: LOAD_THREADS },
      (_, i) =>
        new Worker(new URL("./loadWorker.mjs", import.meta.url), {
          workerData: { port: api.port, connections: CONNECTIONS_PER_THREAD, cdf, seed: 100 + i, fixedPath },
          execArgv: [],
        })
    );
    const drive = async (seconds: number) => {
      const started = performance.now();
      const parts = await Promise.all(
        threads.map(
          (t) =>
            new Promise<{ latencies: Float64Array; failed: number }>((resolve, reject) => {
              t.once("message", resolve);
              t.once("error", reject);
              t.postMessage(seconds);
            })
        )
      );
      const latencies = parts.flatMap((p) => Array.from(p.latencies)).sort((x, y) => x - y);
      return { latencies, failed: parts.reduce((n, p) => n + p.failed, 0), elapsed: (performance.now() - started) / 1_000 };
    };
    // Touch every product once, then run the real workload, so nothing measured is a cold miss.
    let nextId = 1;
    await Promise.all(
      Array.from({ length: CONCURRENCY }, async () => {
        while (nextId <= DEFAULT_SEED.products) await getProduct(api, nextId++);
      })
    );
    await drive(Math.max(3, SECONDS / 2));
    const runs: LayerRun[] = [];
    for (let run = 0; run < REPEATS; run++) {
      await resetMetrics(api);
      const { latencies, failed, elapsed } = await drive(SECONDS);
      const m = await metrics(api);
      const n = latencies.length;
      runs.push({
        rps: n / elapsed,
        p50: percentile(latencies, 50),
        p95: percentile(latencies, 95),
        p99: percentile(latencies, 99),
        l1: m.cache.l1 / n,
        redisPerSecond: m.cache.l2 / elapsed,
        originPerSecond: m.cache.originLoads / elapsed,
        busy: m.eventLoopUtilization,
        failed,
      });
    }
    const mid = (k: keyof LayerRun) => median(runs.map((r) => r[k]));
    const range = runs.map((r) => Math.round(r.rps)).sort((x, y) => x - y);
    out.push(
      `| ${name} | ${Math.round(mid("rps"))} (${range[0]}–${range.at(-1)}) | ${ms(mid("p50"))} | ${ms(mid("p95"))} | ${ms(mid("p99"))} | ${(mid("l1") * 100).toFixed(1)}% | ${Math.round(mid("redisPerSecond"))} | ${Math.round(mid("originPerSecond"))} | ${(mid("busy") * 100).toFixed(0)}% | ${runs.reduce((t, r) => t + r.failed, 0)} |`
    );
    console.log(out.at(-1));
    await Promise.all(threads.map((t) => t.terminate()));
    await api.stop();
  }
  return out;
}

// 2. One hot key goes cold in every process at once.
async function stampede(): Promise<string[]> {
  const ROUNDS = 5;
  const BURST = 300;
  const id = 1;
  const base = { REDIS_URL, STALE_WHILE_REVALIDATE: "false" };
  const variants: Array<{ name: string; env: Record<string, string>; soft?: boolean; warm?: boolean }> = [
    { name: "Baseline: key already warm, no stampede", env: base, warm: true },
    { name: "No protection", env: { ...base, COALESCE: "false", DISTRIBUTED_LOCK: "false" } },
    { name: "Coalescing in each process", env: { ...base, DISTRIBUTED_LOCK: "false" } },
    { name: "Coalescing and a Redis lock", env: base },
    { name: "Also serving stale while refreshing", env: { REDIS_URL, SOFT_TTL_MS: "400", HARD_TTL_MS: "60000" }, soft: true },
  ];
  const out: string[] = [];
  for (const v of variants) {
    await clearRedis();
    const apis = await Promise.all([1, 2, 3].map(() => startApi(v.env)));
    // Open connections first, so the burst measures the cache and not TCP setup.
    await Promise.all(apis.flatMap((a) => Array.from({ length: 100 }, (_, i) => getProduct(a, 1_000 + i))));
    const latencies: number[] = [];
    const loads: number[] = [];
    // One discarded round warms up the JIT, so the first real round is not an outlier.
    await Promise.all(Array.from({ length: BURST }, (_, i) => getProduct(apis[i % 3], 2_000)));
    for (let round = 0; round < ROUNDS; round++) {
      if (v.warm) {
        await Promise.all(apis.map((a) => getProduct(a, id)));
      } else if (v.soft) {
        await Promise.all(apis.map((a) => getProduct(a, id)));
        await sleep(600); // past the soft TTL, well inside the hard one
      } else {
        await redis.del(`val:product:${id}`);
        await redis.publish("hotpath:invalidate", "*");
        await sleep(100);
      }
      await Promise.all(apis.map(resetMetrics));
      await Promise.all(
        Array.from({ length: BURST }, async (_, i) => {
          const t = performance.now();
          const r = await getProduct(apis[i % 3], id);
          if (r.status !== 200) throw new Error(`status ${r.status}`);
          latencies.push(performance.now() - t);
        })
      );
      await sleep(300);
      const all = await Promise.all(apis.map(metrics));
      loads.push(all.reduce((sum, m) => sum + m.cache.originLoads, 0));
    }
    latencies.sort((a, b) => a - b);
    out.push(`| ${v.name} | ${loads.join(", ")} | ${ms(percentile(latencies, 50))} | ${ms(percentile(latencies, 99))} |`);
    console.log(out.at(-1));
    for (const a of apis) await a.stop();
  }
  return out;
}

// 3. After a write is acknowledged, how long do other processes serve the old value?
async function staleness(): Promise<string[]> {
  const WRITES = 100;
  const L1_TTL = 1_000;
  const out: string[] = [];
  for (const [name, env] of [
    ["Redis pub/sub invalidation", { REDIS_URL, L1_TTL_MS: "60000" }],
    [`No shared layer, ${L1_TTL} ms TTL only`, { L1_TTL_MS: String(L1_TTL) }],
  ] as Array<[string, Record<string, string>]>) {
    await clearRedis();
    const apis = await Promise.all([1, 2, 3].map(() => startApi(env)));
    const windows: number[] = [];
    let immediate = 0;
    for (let i = 0; i < WRITES; i++) {
      const id = 100 + (i % 20);
      const price = 1_000_000 + i;
      await Promise.all(apis.map((a) => getProduct(a, id)));
      await putPrice(apis[0], id, price);
      const acked = performance.now();
      const perProcess = await Promise.all(
        apis.slice(1).map(async (a) => {
          let lastStale = 0;
          for (;;) {
            const sent = performance.now();
            if ((await getProduct(a, id)).price === price) return lastStale;
            lastStale = sent - acked; // a read sent this long after the write was still stale
            if (lastStale > 5_000) throw new Error("never became fresh");
          }
        })
      );
      for (const w of perProcess) {
        windows.push(w);
        if (w === 0) immediate++;
      }
    }
    windows.sort((a, b) => a - b);
    out.push(
      `| ${name} | ${windows.length} | ${immediate} | ${ms(percentile(windows, 50))} | ${ms(percentile(windows, 99))} | ${ms(windows.at(-1)!)} |`
    );
    console.log(out.at(-1));
    for (const a of apis) await a.stop();
  }
  return out;
}

// 4. Heavy reads and writes on a few keys, then silence. Any old value left behind is a bug.
async function writeStorm(): Promise<string[]> {
  const ROUNDS = 20;
  const KEYS = 20;
  const out: string[] = [];
  for (const [name, env] of [
    ["Version guard on", {}],
    ["Version guard off (sabotage)", { BENCH_DISABLE_VERSION_GUARD: "true" }],
  ] as Array<[string, Record<string, string>]>) {
    await clearRedis();
    const apis = await Promise.all([1, 2, 3].map(() => startApi({ REDIS_URL, ...env })));
    let price = 2_000_000;
    let reads = 0;
    let writes = 0;
    let staleKeys = 0;
    let checked = 0;
    for (let round = 0; round < ROUNDS; round++) {
      const stopAt = Date.now() + 500;
      const acknowledged = new Map<number, number>();
      await Promise.all([
        ...Array.from({ length: 30 }, async (_, i) => {
          while (Date.now() < stopAt) {
            await getProduct(apis[i % 3], 200 + Math.floor(Math.random() * KEYS));
            reads++;
          }
        }),
        // One writer per key, so the last acknowledged price is the true one.
        ...Array.from({ length: KEYS }, async (_, i) => {
          while (Date.now() < stopAt) {
            const next = ++price;
            await putPrice(apis[(i + round) % 3], 200 + i, next);
            acknowledged.set(200 + i, next);
            writes++;
            await sleep(Math.random() * 20);
          }
        }),
      ]);
      await sleep(300);
      for (const [id, expected] of acknowledged) {
        for (const api of apis) {
          checked++;
          if ((await getProduct(api, id)).price !== expected) staleKeys++;
        }
      }
    }
    const rejections = (await Promise.all(apis.map(metrics))).reduce((s, m) => s + m.cache.guardRejections, 0);
    out.push(`| ${name} | ${writes} | ${reads} | ${rejections} | ${staleKeys} of ${checked} |`);
    console.log(out.at(-1));
    for (const a of apis) await a.stop();
  }
  return out;
}

const layerRows = await layers();
const stampedeRows = await stampede();
const stalenessRows = await staleness();
const stormRows = await writeStorm();

const pgVersion = (await db.pool.query("SHOW server_version")).rows[0].server_version;
const redisVersion = /redis_version:(\S+)/.exec(await redis.info("server"))?.[1];
redis.disconnect();

const md = `# Benchmark

Generated by \`npm run bench\` on ${new Date().toISOString().slice(0, 10)}.

Local single-machine run: ${os.cpus().length} × ${os.cpus()[0].model.trim()}, ${Math.round(os.totalmem() / 2 ** 30)} GB RAM, Node ${process.version}, PostgreSQL ${pgVersion}, Redis ${redisVersion}, ${process.platform}. The load generator, the service, Postgres and Redis shared this machine, so use these numbers to compare designs. They are not capacity figures.

Data: ${DEFAULT_SEED.products.toLocaleString("en")} products, ${(DEFAULT_SEED.products * DEFAULT_SEED.reviewsPerProduct).toLocaleString("en")} reviews, ${(DEFAULT_SEED.products * DEFAULT_SEED.ordersPerProduct).toLocaleString("en")} order items. The slow read ranks a product in its category and has no artificial delay.

## Cache layers

One process, ${CONCURRENCY} concurrent clients, median of ${REPEATS} runs of ${SECONDS} s after every product was read once, long TTLs, Zipf 1.0 over all products. The in-process layer holds 500 entries, 10% of the products.

| Configuration | RPS, median (range) | p50 (ms) | p95 (ms) | p99 (ms) | Served from process memory | Reads served by Redis/s | Postgres loads/s | Service event loop busy | Failed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
${layerRows.join("\n")}

Load comes from ${LOAD_THREADS} separate threads, so the service is the bottleneck and not the generator.

## Stampede

Three processes. One hot key goes cold everywhere, then 300 requests arrive at once. Five rounds.

| Protection | Origin loads in each round | p50 (ms) | p99 (ms) |
| --- | --- | --- | --- |
${stampedeRows.join("\n")}

The baseline row is the cost of this test itself: 300 simultaneous requests generated and answered on one machine. Latency above it is the stampede's. In the last row the entry is past its soft TTL and not deleted, because serving stale needs an old value to serve.

## Staleness after a write

The write goes to one process. The two others are then read continuously until they return the new value. The window is how long after the acknowledgement a read was still sent and answered with the old value.

| Invalidation | Samples | Fresh on first read | p50 (ms) | p99 (ms) | Max (ms) |
| --- | --- | --- | --- | --- | --- |
${stalenessRows.join("\n")}

## Write storm, then silence

Three processes, 20 keys, 20 rounds. After each storm the system settles for 300 ms, then every key is read from every process and compared with the last acknowledged write.

| Design | Writes | Reads during the storm | Stale stores rejected | Wrong values left behind |
| --- | --- | --- | --- | --- |
${stormRows.join("\n")}
`;
mkdirSync("docs", { recursive: true });
writeFileSync("docs/bench-results.md", md);
await db.stop();
process.exit(0);
