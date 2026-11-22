import { createPool, migrate, seed } from "./db.js";
import { loadConfig } from "./config.js";
import { createDemo } from "./demo.js";

const config = loadConfig();
if (!config.cache.redisUrl) throw new Error("The demo needs REDIS_URL");
const pool = createPool(config.databaseUrl, config.poolSize);
await migrate(pool);
await seed(pool);

const demo = await createDemo(pool, config.cache.redisUrl);
const address = await demo.app.listen({ port: config.port, host: "0.0.0.0" });
console.log(JSON.stringify({ msg: "demo.listening", address }));

let stopping = false;
async function shutdown(): Promise<void> {
  if (stopping) return;
  stopping = true;
  await demo.close();
  await pool.end();
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
