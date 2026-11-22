import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { seed } from "../src/db.js";
import { type Demo, createDemo } from "../src/demo.js";
import { REDIS_URL, type TestDb, connectRedis, startDb } from "../scripts/lib.js";

let db: TestDb;
let demo: Demo;

before(async () => {
  (await connectRedis()).disconnect();
  db = await startDb();
  await seed(db.pool, { products: 50, categories: 5, reviewsPerProduct: 10, ordersPerProduct: 10 });
  demo = await createDemo(db.pool, REDIS_URL);
});
after(async () => {
  await demo.close();
  await db.stop();
});

let nextIp = 1;
const post = (url: string, ip = `10.0.0.${nextIp++}`) =>
  demo.app.inject({ method: "POST", url, headers: { "cf-connecting-ip": ip } });

test("the page is served, and the unauthenticated product routes are not", async () => {
  const page = await demo.app.inject({ url: "/" });
  assert.equal(page.statusCode, 200);
  assert.match(page.body, /Hotpath/);
  assert.equal((await demo.app.inject({ url: "/products/1" })).statusCode, 404);
  const put = await demo.app.inject({ method: "PUT", url: "/products/1/price", payload: { priceCents: 1 } });
  assert.equal(put.statusCode, 404);
  assert.equal((await demo.app.inject({ method: "POST", url: "/metrics/reset" })).statusCode, 404);
});

test("layers: the same read moves from the database to Redis to memory", async () => {
  const body = (await post("/demo/layers")).json();
  assert.deepEqual(body.steps.map((s: { source: string }) => s.source), ["origin", "l2", "l1"]);
  assert.equal(body.databaseLoads, 1);
});

test("stampede: protection reduces the database loads to one", async () => {
  const body = (await post("/demo/stampede")).json();
  assert.equal(body.protected.databaseLoads, 1);
  assert.ok(body.unprotected.databaseLoads > 10);
});

test("race: the guard serves the new price, and without it the old one stays", async () => {
  const body = (await post("/demo/race")).json();
  for (const run of [body.guarded, body.unguarded]) {
    assert.equal(run.slowReader, body.oldPrice);
    assert.equal(run.database, body.newPrice);
  }
  assert.equal(body.guarded.laterOnA, body.newPrice);
  assert.equal(body.guarded.laterOnB, body.newPrice);
  assert.equal(body.unguarded.laterOnA, body.oldPrice);
  assert.equal(body.unguarded.laterOnB, body.oldPrice);
  assert.equal((await db.pool.query("SELECT 1 FROM demo_prices")).rowCount, 0);
});

test("outage: every read is served with Redis cut, and the shared layer recovers", async () => {
  const body = (await post("/demo/outage")).json();
  assert.equal(body.healthy.served, body.reads);
  assert.equal(body.down.served, body.reads);
  assert.equal(body.down.databaseLoads, 1);
  assert.ok(body.recoveredMs !== null);
});

test("a fourth experiment at once is turned away, and the slots are released", async () => {
  const codes = (await Promise.all(Array.from({ length: 4 }, () => post("/demo/stampede")))).map((r) => r.statusCode);
  assert.deepEqual(codes.sort(), [200, 200, 200, 429]);
  assert.equal((await post("/demo/layers")).statusCode, 200);
});

test("one visitor is rate limited without affecting another", async () => {
  let last = 200;
  for (let i = 0; i < 31; i++) last = (await post("/demo/layers", "192.0.2.1")).statusCode;
  assert.equal(last, 429);
  assert.equal((await post("/demo/layers", "192.0.2.2")).statusCode, 200);
});
