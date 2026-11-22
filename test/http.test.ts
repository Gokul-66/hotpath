import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import type { FastifyInstance } from "fastify";
import { createApp, productKey } from "../src/app.js";
import { TieredCache } from "../src/cache/tieredCache.js";
import { seed } from "../src/db.js";
import { type ProductDetail, loadProduct } from "../src/origin.js";
import { REDIS_URL, type TestDb, connectRedis, startDb } from "../scripts/lib.js";

let db: TestDb;
let cache: TieredCache<ProductDetail>;
let app: FastifyInstance;

before(async () => {
  db = await startDb();
  await seed(db.pool, { products: 50, categories: 5, reviewsPerProduct: 10, ordersPerProduct: 10 });
  const redis = await connectRedis();
  for (let id = 1; id <= 50; id++) await redis.del(`val:${productKey(id)}`);
  redis.disconnect();
  cache = new TieredCache((key) => loadProduct(db.pool, Number(key.split(":")[1])), { redisUrl: REDIS_URL });
  await cache.ready();
  app = createApp(db.pool, cache);
});
after(async () => {
  await app.close();
  await cache.close();
  await db.stop();
});

test("a product is served, then cached, and a price change is visible at once", async () => {
  const first = await app.inject({ url: "/products/7" });
  assert.equal(first.statusCode, 200);
  assert.equal(first.headers["x-cache"], "origin");
  const body = first.json();
  assert.equal(Object.values<number>(body.rating.histogram).reduce((a, b) => a + b, 0), 10);

  const second = await app.inject({ url: "/products/7" });
  assert.equal(second.headers["x-cache"], "l1");
  assert.deepEqual(second.json(), body);

  const put = await app.inject({ method: "PUT", url: "/products/7/price", payload: { priceCents: 123 } });
  assert.equal(put.statusCode, 200);
  assert.equal((await app.inject({ url: "/products/7" })).json().priceCents, 123);
});

test("unknown products and bad input are rejected", async () => {
  assert.equal((await app.inject({ url: "/products/99999" })).statusCode, 404);
  assert.equal((await app.inject({ url: "/products/abc" })).statusCode, 400);
  assert.equal((await app.inject({ method: "PUT", url: "/products/7/price", payload: { priceCents: -1 } })).statusCode, 400);
  assert.equal((await app.inject({ method: "PUT", url: "/products/99999/price", payload: { priceCents: 1 } })).statusCode, 404);
});

test("category rank agrees with a direct calculation", async () => {
  const product = (await app.inject({ url: "/products/3" })).json() as ProductDetail;
  const { rows } = await db.pool.query(
    `SELECT o.product_id, sum(o.quantity)::int AS units FROM order_items o JOIN products p ON p.id = o.product_id
     WHERE p.category = 3 AND o.ordered_at > now() - interval '30 days' GROUP BY 1`
  );
  const mine = rows.find((r) => r.product_id === 3);
  assert.equal(product.unitsSold30d, mine?.units ?? 0);
  assert.equal(product.categoryRank, mine ? 1 + rows.filter((r) => r.units > mine.units).length : null);
});
