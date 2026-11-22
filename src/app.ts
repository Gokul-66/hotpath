import { performance } from "node:perf_hooks";
import Fastify, { type FastifyInstance } from "fastify";
import type pg from "pg";
import { z } from "zod";
import type { TieredCache } from "./cache/tieredCache.js";
import { type ProductDetail, updatePrice } from "./origin.js";

const idParams = z.object({ id: z.coerce.number().int().min(1).max(2_147_483_647) });
const priceBody = z.object({ priceCents: z.number().int().min(0).max(100_000_000) });

export const productKey = (id: number) => `product:${id}`;

export function createApp(pool: pg.Pool, cache: TieredCache<ProductDetail>): FastifyInstance {
  const app = Fastify({ bodyLimit: 4 * 1024 });
  let loopBaseline = performance.eventLoopUtilization();

  app.setErrorHandler((err: any, _req, reply) => {
    if (err instanceof z.ZodError) return reply.status(400).send({ error: "VALIDATION_ERROR" });
    if (err.statusCode && err.statusCode < 500) return reply.status(err.statusCode).send({ error: "BAD_REQUEST" });
    console.error(JSON.stringify({ msg: "request.failed", error: err.message }));
    return reply.status(503).send({ error: "UNAVAILABLE" });
  });

  app.get("/products/:id", async (req, reply) => {
    const { id } = idParams.parse(req.params);
    const { value, source } = await cache.get(productKey(id));
    reply.header("x-cache", source);
    if (!value) return reply.status(404).send({ error: "NOT_FOUND" });
    return value;
  });

  // No authentication: this is a demo of cache behaviour. See the design doc's limits.
  app.put("/products/:id/price", async (req, reply) => {
    const { id } = idParams.parse(req.params);
    const { priceCents } = priceBody.parse(req.body);
    if (!(await updatePrice(pool, id, priceCents))) return reply.status(404).send({ error: "NOT_FOUND" });
    await cache.invalidate(productKey(id));
    return { id, priceCents };
  });

  app.get("/metrics", async () => ({
    cache: cache.stats,
    redis: cache.redisStats(),
    // Share of time the event loop was busy since the last reset. 1 means the process is saturated.
    eventLoopUtilization: performance.eventLoopUtilization(loopBaseline).utilization,
  }));

  app.post("/metrics/reset", async () => {
    for (const k of Object.keys(cache.stats) as Array<keyof typeof cache.stats>) cache.stats[k] = 0;
    loopBaseline = performance.eventLoopUtilization();
    return { reset: true };
  });

  app.get("/healthz", async () => ({ status: "ok" }));

  // Redis is optional for serving, so readiness depends only on the database.
  app.get("/readyz", async (_req, reply) => {
    try {
      await pool.query("SELECT 1");
      return { status: "ready" };
    } catch {
      return reply.status(503).send({ status: "unavailable" });
    }
  });

  return app;
}
