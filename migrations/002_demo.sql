-- Sandbox rows for the demo page's write race. Each run deletes its own row.
CREATE TABLE demo_prices (
  id          uuid PRIMARY KEY,
  price_cents integer NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);
