CREATE TABLE products (
  id          integer PRIMARY KEY,
  name        text NOT NULL,
  category    integer NOT NULL,
  price_cents integer NOT NULL CHECK (price_cents >= 0),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX products_category_idx ON products (category);

CREATE TABLE reviews (
  product_id integer NOT NULL REFERENCES products (id),
  rating     smallint NOT NULL CHECK (rating BETWEEN 1 AND 5)
);
CREATE INDEX reviews_product_idx ON reviews (product_id) INCLUDE (rating);

CREATE TABLE order_items (
  product_id integer NOT NULL REFERENCES products (id),
  quantity   smallint NOT NULL,
  ordered_at timestamptz NOT NULL
);
CREATE INDEX order_items_product_idx ON order_items (product_id) INCLUDE (quantity, ordered_at);
