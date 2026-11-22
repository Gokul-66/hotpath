import assert from "node:assert/strict";
import { test } from "node:test";
import { LruCache } from "../src/cache/lru.js";
import { SingleFlight } from "../src/cache/singleflight.js";
import { CountMinSketch, TinyLfu } from "../src/cache/tinylfu.js";
import { mulberry32, zipf } from "../scripts/lib.js";

test("LRU evicts the least recently used key", () => {
  const c = new LruCache<number>(2);
  c.set("a", 1);
  c.set("b", 2);
  c.get("a");
  c.set("c", 3);
  assert.deepEqual(c.keys(), ["c", "a"]);
  assert.equal(c.get("b"), undefined);
});

test("LRU matches a simple reference model over 50,000 random operations", () => {
  const random = mulberry32(7);
  const capacity = 20;
  const cache = new LruCache<number>(capacity);
  let model: Array<[string, number]> = []; // most recent first
  for (let i = 0; i < 50_000; i++) {
    const key = `k${Math.floor(random() * 50)}`;
    const op = random();
    const found = model.find(([k]) => k === key);
    if (op < 0.5) {
      assert.equal(cache.get(key), found?.[1]);
      if (found) model = [found, ...model.filter(([k]) => k !== key)];
    } else if (op < 0.9) {
      cache.set(key, i);
      model = ([[key, i], ...model.filter(([k]) => k !== key)] as Array<[string, number]>).slice(0, capacity);
    } else {
      assert.equal(cache.delete(key), found !== undefined);
      model = model.filter(([k]) => k !== key);
    }
    assert.equal(cache.size, model.length);
  }
  assert.deepEqual(cache.keys(), model.map(([k]) => k));
});

test("LRU rejects an invalid capacity and can be cleared and reused", () => {
  assert.throws(() => new LruCache(0), RangeError);
  const c = new LruCache<number>(1);
  c.set("a", 1);
  c.clear();
  c.set("b", 2);
  assert.deepEqual(c.keys(), ["b"]);
});

test("count-min sketch never underestimates before aging", () => {
  const random = mulberry32(3);
  const sketch = new CountMinSketch(256);
  const truth = new Map<string, number>();
  for (let i = 0; i < 2_000; i++) {
    const key = `k${Math.floor(random() * 400)}`;
    sketch.increment(key);
    truth.set(key, (truth.get(key) ?? 0) + 1);
  }
  for (const [key, n] of truth) assert.ok(sketch.estimate(key) >= Math.min(n, 15), key);
});

test("TinyLFU keeps popular keys through a one-off scan, and plain LRU does not", () => {
  const run = (cache: LruCache<number>) => {
    const read = (key: string) => {
      if (cache.get(key) === undefined) cache.set(key, 1);
    };
    for (let round = 0; round < 8; round++) for (let i = 0; i < 100; i++) read(`hot${i}`);
    for (let i = 0; i < 1_000; i++) read(`scan${i}`);
    return cache.keys().filter((k) => k.startsWith("hot")).length;
  };
  assert.equal(run(new LruCache<number>(100)), 0);
  assert.ok(run(new LruCache<number>(100, new TinyLfu(100))) >= 95);
});

test("TinyLFU lets a new key in once it becomes more popular than the victim", () => {
  const cache = new LruCache<number>(10, new TinyLfu(10));
  for (let i = 0; i < 10; i++) {
    cache.get(`old${i}`);
    cache.set(`old${i}`, 1);
  }
  let reads = 0;
  while (!cache.keys().includes("new")) {
    cache.get("new");
    cache.set("new", 1);
    assert.ok(++reads < 20);
  }
  assert.ok(reads > 1);
});

test("single flight shares one execution and does not cache failures", async () => {
  const flight = new SingleFlight<number>();
  let calls = 0;
  const slow = async () => {
    calls++;
    await new Promise((r) => setTimeout(r, 20));
    return calls;
  };
  const results = await Promise.all(Array.from({ length: 100 }, () => flight.do("k", slow).promise));
  assert.equal(calls, 1);
  assert.ok(results.every((r) => r === 1));
  assert.equal(flight.size, 0);

  await assert.rejects(flight.do("k", async () => Promise.reject(new Error("boom"))).promise, /boom/);
  assert.equal(await flight.do("k", slow).promise, 2);
});

test("forgetting a flight lets a new one start, and the old one finishing does not remove it", async () => {
  const flight = new SingleFlight<string>();
  let finishOld!: (v: string) => void;
  let finishNew!: (v: string) => void;
  const old = flight.do("k", () => new Promise((r) => (finishOld = r))).promise;
  flight.forget("k");
  const fresh = flight.do("k", () => new Promise((r) => (finishNew = r))).promise;
  finishOld("old");
  await old;
  assert.equal(flight.do("k", async () => "third").shared, true);
  finishNew("new");
  assert.equal(await fresh, "new");
});

test("zipf draws the most popular item most often", () => {
  const draw = zipf(1_000, 1, mulberry32(1));
  const counts = new Array(1_000).fill(0);
  for (let i = 0; i < 100_000; i++) counts[draw()]++;
  // With exponent 1 over 1,000 items, rank 1 gets 1/H(1000), about 13.4%.
  assert.ok(Math.abs(counts[0] / 100_000 - 0.134) < 0.01);
  assert.ok(counts[0] > counts[1] && counts[1] > counts[9]);
});
