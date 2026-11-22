// Plain JavaScript on its own thread, so generating load does not limit what is measured.
import http from "node:http";
import { parentPort, workerData } from "node:worker_threads";

const { port, connections, cdf, seed, fixedPath } = workerData;
const agent = new http.Agent({ keepAlive: true, maxSockets: connections });

// The same seeded generator and Zipf draw as scripts/lib.ts.
let state = seed >>> 0;
function random() {
  state = (state + 0x6d2b79f5) >>> 0;
  let t = Math.imul(state ^ (state >>> 15), 1 | state);
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
  return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
}

function draw() {
  const target = random() * cdf[cdf.length - 1];
  let lo = 0;
  let hi = cdf.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (cdf[mid] < target) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

function get(path) {
  return new Promise((resolve, reject) => {
    const req = http.get({ host: "127.0.0.1", port, path, agent }, (res) => {
      res.resume();
      res.on("end", () => resolve(res.statusCode));
      res.on("error", reject);
    });
    req.on("error", reject);
  });
}

parentPort.on("message", async (seconds) => {
  const latencies = [];
  let failed = 0;
  const stopAt = performance.now() + seconds * 1_000;
  await Promise.all(
    Array.from({ length: connections }, async () => {
      while (performance.now() < stopAt) {
        const t = performance.now();
        const status = await get(fixedPath ?? `/products/${draw() + 1}`).catch(() => 0);
        latencies.push(performance.now() - t);
        if (status !== 200) failed++;
      }
    })
  );
  parentPort.postMessage({ latencies: Float64Array.from(latencies), failed });
});
