import { createApp } from "./app.js";
import { TieredCache } from "./cache/tieredCache.js";
import { loadConfig } from "./config.js";
import { createPool, migrate, seed } from "./db.js";
import { type ProductDetail, loadProduct } from "./origin.js";

const config = loadConfig();
const pool = createPool(config.databaseUrl, config.poolSize);
await migrate(pool);
await seed(pool);

const cache = new TieredCache<ProductDetail>((key) => loadProduct(pool, Number(key.split(":")[1])), config.cache);
await cache.ready().catch(() => console.error(JSON.stringify({ msg: "redis.unavailable_at_start" })));

const app = createApp(pool, cache);
const address = await app.listen({ port: config.port, host: "0.0.0.0" });
console.log(JSON.stringify({ msg: "service.listening", address }));

let stopping = false;
async function shutdown(): Promise<void> {
  if (stopping) return;
  stopping = true;
  await app.close();
  await cache.close();
  await pool.end();
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
