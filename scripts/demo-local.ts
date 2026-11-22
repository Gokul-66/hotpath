// Runs the demo against a throwaway local Postgres. Needs Redis at REDIS_URL.
import { spawn } from "node:child_process";
import { REDIS_URL, connectRedis, freePort, startDb } from "./lib.js";

(await connectRedis()).disconnect();
const db = await startDb(5);
const port = Number(process.env.PORT) || (await freePort());
const service = spawn(process.execPath, ["--import", "tsx", "src/demoServer.ts"], {
  stdio: "inherit",
  env: { ...process.env, DATABASE_URL: db.url, REDIS_URL, PORT: String(port), POOL_SIZE: "5" },
});
console.log(`demo: http://localhost:${port}/`);

const stop = async () => {
  service.kill();
  await db.stop();
  process.exit(0);
};
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
