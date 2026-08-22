CREATE TABLE users (
    user_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    total NUMERIC(12,2) NOT NULL CHECK (total >= 0),
    -- pg keeps the generation expression in pg_attrdef beside real defaults;
    -- the capture must not report it as one (PG12+)
    total_with_tax NUMERIC(12,2) GENERATED ALWAYS AS (total * 1.21) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX orders_by_user ON orders(user_id);
