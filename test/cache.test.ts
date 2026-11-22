import assert from "node:assert/strict";
import { after, afterEach, before, test } from "node:test";
import { randomUUID } from "node:crypto";
import type { Redis } from "ioredis";
import { type CacheOptions, TieredCache } from "../src/cache/tieredCache.js";
import { REDIS_URL, RedisProxy, connectRedis } from "../scripts/lib.js";

let redis: Redis;
const open: Array<{ close(): Promise<void> }> = [];
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

before(async () => {
  redis = await connectRedis();
});
after(async () => {
  redis.disconnect();
});
afterEach(async () => {
  for (const c of open.splice(0)) await c.close();
});

/** A stand-in origin with a value that can change and a load that can be held open. */
class Origin {
  value: string | null = "v1";
  loads = 0;
  delayMs = 0;
  private gate: Promise<void> | null = null;
  private openGate: () => void = () => {};

  hold(): void {
    this.gate = new Promise((r) => (this.openGate = r));
  }
  release(): void {
    this.gate = null;
    this.openGate();
  }
  load = async (): Promise<string | null> => {
    this.loads++;
    const seen = this.value; // what the database returned, before any later write
    if (this.gate) await this.gate;
    if (this.delayMs) await sleep(this.delayMs);
    return seen;
  };
}

async function make(origin: Origin, options: Partial<CacheOptions> = {}): Promise<TieredCache<string>> {
  const cache = new TieredCache<string>(origin.load, { redisUrl: REDIS_URL, jitter: 0, tinyLfu: false, ...options });
  open.push(cache);
  if (options.redisUrl !== undefined || !("redisUrl" in options)) await cache.ready();
  return cache;
}

const key = () => `test:${randomUUID()}`;
const until = async (check: () => boolean, ms = 3_000) => {
  const deadline = Date.now() + ms;
  while (!check()) {
    if (Date.now() > deadline) throw new Error("condition not reached");
    await sleep(5);
  }
};

test("reads fall through origin, then Redis, then memory", async () => {
  const origin = new Origin();
  const k = key();
  const a = await make(origin);
  const b = await make(origin);
  assert.equal((await a.get(k)).source, "origin");
  assert.equal((await a.get(k)).source, "l1");
  assert.equal((await b.get(k)).source, "l2");
  assert.equal(origin.loads, 1);
});

test("200 concurrent reads in one process load the origin once", async () => {
  const origin = new Origin();
  origin.delayMs = 30;
  const c = await make(origin, { redisUrl: undefined });
  const k = key();
  await Promise.all(Array.from({ length: 200 }, () => c.get(k)));
  assert.equal(origin.loads, 1);
});

test("without coalescing the same burst hits the origin 200 times", async () => {
  const origin = new Origin();
  origin.delayMs = 30;
  const c = await make(origin, { redisUrl: undefined, coalesce: false });
  const k = key();
  await Promise.all(Array.from({ length: 200 }, () => c.get(k)));
  assert.equal(origin.loads, 200);
});

test("the distributed lock keeps three processes to one load, and without it each loads", async () => {
  for (const [distributedLock, expected] of [[true, 1], [false, 3]] as const) {
    const origin = new Origin();
    origin.delayMs = 60;
    const k = key();
    const caches = await Promise.all([1, 2, 3].map(() => make(origin, { distributedLock })));
    const results = await Promise.all(caches.flatMap((c) => Array.from({ length: 50 }, () => c.get(k))));
    assert.ok(results.every((r) => r.value === "v1"));
    assert.equal(origin.loads, expected);
  }
});

test("a holder that dies does not block readers past the wait limit", async () => {
  const origin = new Origin();
  const k = key();
  const c = await make(origin, { lockWaitMs: 150 });
  await redis.set(`lock:${k}`, "dead-process", "PX", 10_000);
  const started = performance.now();
  assert.equal((await c.get(k)).value, "v1");
  assert.ok(performance.now() - started < 600);
  assert.equal(c.stats.lockTimeouts, 1);
  assert.equal(await redis.get(`lock:${k}`), "dead-process"); // another owner's lock is never released
});

test("a write during a load cannot leave the old value cached", async () => {
  const origin = new Origin();
  const k = key();
  const a = await make(origin);
  const b = await make(origin);

  origin.hold();
  const slow = a.get(k);
  await until(() => origin.loads === 1);
  origin.value = "v2";
  await b.invalidate(k);
  origin.release();
  assert.equal((await slow).value, "v1"); // it began before the write, so this is allowed

  assert.equal((await a.get(k)).value, "v2");
  assert.equal((await b.get(k)).value, "v2");
  assert.equal(a.stats.guardRejections, 1);
});

test("losing the version during a load, as eviction can, rejects the store instead of allowing it", async () => {
  for (const write of [false, true]) {
    const origin = new Origin();
    const k = key();
    const c = await make(origin);
    const other = await make(origin);
    origin.hold();
    const slow = c.get(k);
    await until(() => origin.loads === 1);
    if (write) {
      origin.value = "v2";
      await other.invalidate(k);
    }
    await redis.del(`ver:${k}`);
    origin.release();
    await slow;
    assert.equal(c.stats.guardRejections, 1);
    assert.equal((await other.get(k)).value, write ? "v2" : "v1");
  }
});

test("without the version guard the same write leaves the old value cached", async () => {
  const origin = new Origin();
  const k = key();
  const a = await make(origin, { versionGuard: false });
  const b = await make(origin, { versionGuard: false });

  origin.hold();
  const slow = a.get(k);
  await until(() => origin.loads === 1);
  origin.value = "v2";
  await b.invalidate(k);
  origin.release();
  await slow;

  assert.equal((await b.get(k)).value, "v1"); // the bug
});

test("a read that starts after a write never joins a load that started before it", async () => {
  const origin = new Origin();
  const k = key();
  const c = await make(origin, { distributedLock: false });
  origin.hold();
  const before = c.get(k);
  await until(() => origin.loads === 1);
  origin.value = "v2";
  await c.invalidate(k);
  const afterWrite = c.get(k);
  await until(() => origin.loads === 2);
  origin.release();
  assert.equal((await before).value, "v1");
  assert.equal((await afterWrite).value, "v2");
  assert.equal((await c.get(k)).value, "v2");
});

test("a waiter takes over promptly when the lock holder's value is rejected", async () => {
  const origin = new Origin();
  const k = key();
  const a = await make(origin);
  const b = await make(origin);
  origin.hold();
  const slow = a.get(k);
  await until(() => origin.loads === 1);
  origin.value = "v2";
  await b.invalidate(k);
  const waiting = b.get(k);
  await until(() => b.stats.lockWaits === 1);
  const started = performance.now();
  origin.release();
  await slow;
  assert.equal((await waiting).value, "v2");
  assert.ok(performance.now() - started < 1_000); // the full wait would be 2 s
});

test("an invalidation reaches other processes' memory", async () => {
  const origin = new Origin();
  const k = key();
  const a = await make(origin, { l1TtlMs: 60_000 });
  const b = await make(origin, { l1TtlMs: 60_000 });
  await a.get(k);
  await b.get(k);
  origin.value = "v2";
  await a.invalidate(k);
  assert.equal((await a.get(k)).value, "v2");
  const deadline = Date.now() + 1_000;
  while ((await b.get(k)).value !== "v2") {
    assert.ok(Date.now() < deadline, "b kept serving the old value");
    await sleep(2);
  }
});

test("past the soft TTL, readers get the old value at once and one refresh runs", async () => {
  const origin = new Origin();
  const k = key();
  const c = await make(origin, { softTtlMs: 40, hardTtlMs: 5_000, l1TtlMs: 5_000 });
  await c.get(k);
  await sleep(60);
  origin.value = "v2";
  origin.delayMs = 50;
  const results = await Promise.all(Array.from({ length: 50 }, () => c.get(k)));
  assert.ok(results.every((r) => r.value === "v1" && r.source === "stale"));
  await until(() => origin.loads === 2);
  await sleep(80);
  assert.equal(origin.loads, 2);
  assert.equal((await c.get(k)).value, "v2");
});

test("past the hard TTL nothing stale is served", async () => {
  const origin = new Origin();
  const k = key();
  const c = await make(origin, { softTtlMs: 20, hardTtlMs: 50, l1TtlMs: 5_000 });
  await c.get(k);
  await sleep(80);
  origin.value = "v2";
  assert.equal((await c.get(k)).value, "v2");
});

test("missing keys are cached briefly, then looked up again", async () => {
  const origin = new Origin();
  origin.value = null;
  const k = key();
  const c = await make(origin, { negativeTtlMs: 60 });
  assert.equal((await c.get(k)).value, null);
  assert.equal((await c.get(k)).value, null);
  assert.equal(origin.loads, 1);
  await sleep(90);
  origin.value = "now exists";
  assert.equal((await c.get(k)).value, "now exists");
});

test("origin errors reach every waiter and are not cached", async () => {
  let fail = true;
  const c = new TieredCache<string>(
    async () => {
      await sleep(10);
      if (fail) throw new Error("db down");
      return "ok";
    },
    { redisUrl: REDIS_URL }
  );
  open.push(c);
  await c.ready();
  const k = key();
  const results = await Promise.allSettled(Array.from({ length: 20 }, () => c.get(k)));
  assert.ok(results.every((r) => r.status === "rejected"));
  fail = false;
  assert.equal((await c.get(k)).value, "ok");
  assert.equal(await redis.exists(`lock:${k}`), 0);
});

test("reads keep working when Redis is cut off, and recover afterwards", async () => {
  const proxy = new RedisProxy(REDIS_URL);
  const url = await proxy.start();
  const origin = new Origin();
  const c = await make(origin, { redisUrl: url });
  try {
    await c.get(key());
    proxy.cut();
    await sleep(20);
    const k = key();
    const results = await Promise.all(Array.from({ length: 50 }, () => c.get(k)));
    assert.ok(results.every((r) => r.value === "v1"));
    assert.equal(origin.loads, 2); // still coalesced in process

    proxy.restore();
    await sleep(2_000);
    const k2 = key();
    await c.get(k2);
    assert.equal(await redis.exists(`val:${k2}`), 1);
  } finally {
    await c.close();
    await proxy.stop();
  }
});

test("a hung Redis costs a bounded delay, then the breaker makes reads fast", async () => {
  const proxy = new RedisProxy(REDIS_URL);
  const url = await proxy.start();
  const origin = new Origin();
  const c = await make(origin, { redisUrl: url, redisTimeoutMs: 50 });
  try {
    proxy.freeze();
    const started = performance.now();
    while (c.redisStats()!.breakerOpens === 0) {
      assert.equal((await c.get(key())).value, "v1");
      assert.ok(performance.now() - started < 3_000);
    }
    const t = performance.now();
    assert.equal((await c.get(key())).value, "v1");
    assert.ok(performance.now() - t < 40, "breaker open, so no timeout is paid");
  } finally {
    await c.close();
    await proxy.stop();
  }
});

test("a write while Redis is down is invalidated everywhere once it returns", async () => {
  const proxy = new RedisProxy(REDIS_URL);
  const url = await proxy.start();
  const origin = new Origin();
  const k = key();
  const writer = await make(origin, { redisUrl: url });
  const reader = await make(origin, { l1TtlMs: 50 });
  try {
    await reader.get(k);
    proxy.cut();
    await sleep(20);
    origin.value = "v2";
    await writer.invalidate(k);
    assert.equal((await reader.get(k)).value, "v1"); // the documented window: stale, bounded by TTL
    proxy.restore();
    const deadline = Date.now() + 5_000;
    while ((await reader.get(k)).value !== "v2") {
      assert.ok(Date.now() < deadline, "invalidation was lost");
      await sleep(20);
    }
  } finally {
    await writer.close();
    await proxy.stop();
  }
});

test("missed invalidations during a disconnect cannot leave stale memory behind", async () => {
  const proxy = new RedisProxy(REDIS_URL);
  const url = await proxy.start();
  const origin = new Origin();
  const k = key();
  const isolated = await make(origin, { redisUrl: url, l1TtlMs: 60_000, softTtlMs: 120_000, hardTtlMs: 240_000 });
  const writer = await make(origin);
  try {
    await isolated.get(k);
    proxy.cut();
    await sleep(20);
    origin.value = "v2";
    await writer.invalidate(k);
    assert.equal((await isolated.get(k)).value, "v1");
    proxy.restore();
    const deadline = Date.now() + 5_000;
    while ((await isolated.get(k)).value !== "v2") {
      assert.ok(Date.now() < deadline, "memory was not flushed on reconnect");
      await sleep(20);
    }
  } finally {
    await isolated.close();
    await proxy.stop();
  }
});
