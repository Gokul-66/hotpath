import type pg from "pg";

export interface ProductDetail {
  id: number;
  name: string;
  category: number;
  priceCents: number;
  rating: { average: number | null; histogram: Record<string, number> };
  unitsSold30d: number;
  categoryRank: number | null;
}

/**
 * The slow read. Ranking a product inside its category aggregates every recent order in that
 * category, so the cost is real work in Postgres and not an artificial delay.
 */
export async function loadProduct(pool: pg.Pool, id: number): Promise<ProductDetail | null> {
  const { rows } = await pool.query(
    `SELECT p.id, p.name, p.category, p.price_cents,
       (SELECT round(avg(rating), 2)::float8 FROM reviews WHERE product_id = p.id) AS average,
       (SELECT coalesce(json_object_agg(rating, n), '{}') FROM
          (SELECT rating, count(*) AS n FROM reviews WHERE product_id = p.id GROUP BY rating) h) AS histogram,
       r.units, r.position
     FROM products p
     LEFT JOIN LATERAL (
       SELECT units, position FROM (
         SELECT o.product_id, sum(o.quantity)::int AS units, rank() OVER (ORDER BY sum(o.quantity) DESC)::int AS position
         FROM products peer JOIN order_items o ON o.product_id = peer.id
         WHERE peer.category = p.category AND o.ordered_at > now() - interval '30 days'
         GROUP BY o.product_id
       ) ranked WHERE ranked.product_id = p.id
     ) r ON true
     WHERE p.id = $1`,
    [id]
  );
  const row = rows[0];
  if (!row) return null;
  return {
    id: row.id,
    name: row.name,
    category: row.category,
    priceCents: row.price_cents,
    rating: { average: row.average, histogram: row.histogram },
    unitsSold30d: row.units ?? 0,
    categoryRank: row.position ?? null,
  };
}

export async function updatePrice(pool: pg.Pool, id: number, priceCents: number): Promise<boolean> {
  const { rowCount } = await pool.query("UPDATE products SET price_cents = $2, updated_at = now() WHERE id = $1", [id, priceCents]);
  return rowCount === 1;
}
