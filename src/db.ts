import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";

const MIGRATIONS_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "migrations");
const MIGRATION_LOCK = 727_002;

export function createPool(connectionString: string, max = 10): pg.Pool {
  const pool = new pg.Pool({ connectionString, max, connectionTimeoutMillis: 30_000 });
  // Without these listeners, a connection dropped by a database restart crashes the process.
  pool.on("error", () => {});
  pool.on("connect", (client) => client.on("error", () => {}));
  return pool;
}

export async function migrate(pool: pg.Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock($1)", [MIGRATION_LOCK]);
    await client.query("CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY)");
    const files = (await readdir(MIGRATIONS_DIR)).filter((f) => f.endsWith(".sql")).sort();
    for (const name of files) {
      const { rowCount } = await client.query("SELECT 1 FROM schema_migrations WHERE name = $1", [name]);
      if (rowCount) continue;
      await client.query(await readFile(path.join(MIGRATIONS_DIR, name), "utf8"));
      await client.query("INSERT INTO schema_migrations (name) VALUES ($1)", [name]);
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

export interface SeedSize {
  products: number;
  categories: number;
  reviewsPerProduct: number;
  ordersPerProduct: number;
}

export const DEFAULT_SEED: SeedSize = { products: 5_000, categories: 25, reviewsPerProduct: 40, ordersPerProduct: 60 };

export async function seed(pool: pg.Pool, size: SeedSize = DEFAULT_SEED): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock($1)", [MIGRATION_LOCK]);
    if ((await client.query("SELECT 1 FROM products LIMIT 1")).rowCount) {
      await client.query("COMMIT");
      return;
    }
    await client.query("SELECT setseed(0.42)");
    await client.query(
      `INSERT INTO products (id, name, category, price_cents)
       SELECT i, 'Product ' || i, i % $2::int, 500 + (random() * 20000)::int FROM generate_series(1, $1::int) i`,
      [size.products, size.categories]
    );
    await client.query(
      `INSERT INTO reviews (product_id, rating)
       SELECT p.id, 1 + floor(random() * 5)::int FROM products p, generate_series(1, $1::int)`,
      [size.reviewsPerProduct]
    );
    await client.query(
      `INSERT INTO order_items (product_id, quantity, ordered_at)
       SELECT p.id, 1 + floor(random() * 4)::int, now() - random() * interval '90 days'
       FROM products p, generate_series(1, $1::int)`,
      [size.ordersPerProduct]
    );
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
  await pool.query("VACUUM ANALYZE");
}
