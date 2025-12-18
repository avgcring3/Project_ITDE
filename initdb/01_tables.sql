CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.raw_data (
  order_id BIGINT,
  user_id BIGINT,
  user_phone TEXT,
  address_text TEXT,
  created_at TIMESTAMPTZ,
  paid_at TIMESTAMPTZ,
  delivery_started_at TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ,
  canceled_at TIMESTAMPTZ,
  payment_type TEXT,

  item_id BIGINT,
  item_title TEXT,
  item_category TEXT,
  item_quantity INT,
  item_price NUMERIC(12,2),
  item_canceled_quantity INT,
  item_replaced_id BIGINT,

  order_discount NUMERIC(12,2),
  item_discount NUMERIC(12,2),
  order_cancellation_reason TEXT,

  driver_id BIGINT,
  driver_phone TEXT,
  delivery_cost NUMERIC(12,2),

  store_id BIGINT,
  store_address TEXT
);

CREATE TABLE IF NOT EXISTS dwh.users (
  user_id BIGINT PRIMARY KEY,
  user_phone TEXT
);

CREATE TABLE IF NOT EXISTS dwh.drivers (
  driver_id BIGINT PRIMARY KEY,
  driver_phone TEXT
);

CREATE TABLE IF NOT EXISTS dwh.stores (
  store_id BIGINT PRIMARY KEY,
  store_address TEXT
);

CREATE TABLE IF NOT EXISTS dwh.items (
  item_id BIGINT PRIMARY KEY,
  item_title TEXT,
  item_category TEXT
);

CREATE TABLE IF NOT EXISTS dwh.orders (
  order_id BIGINT PRIMARY KEY,
  user_id BIGINT,
  store_id BIGINT,
  driver_id BIGINT,
  address_text TEXT,
  created_at TIMESTAMPTZ,
  paid_at TIMESTAMPTZ,
  delivery_started_at TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ,
  canceled_at TIMESTAMPTZ,
  payment_type TEXT,
  delivery_cost NUMERIC(12,2),
  order_discount NUMERIC(12,2),
  order_cancellation_reason TEXT,
  CONSTRAINT fk_orders_user   FOREIGN KEY (user_id)  REFERENCES dwh.users(user_id),
  CONSTRAINT fk_orders_store  FOREIGN KEY (store_id) REFERENCES dwh.stores(store_id),
  CONSTRAINT fk_orders_driver FOREIGN KEY (driver_id) REFERENCES dwh.drivers(driver_id)
);

CREATE TABLE IF NOT EXISTS dwh.order_items (
  order_id BIGINT,
  item_id BIGINT,
  item_quantity INT,
  item_price NUMERIC(12,2),
  item_discount NUMERIC(12,2),
  item_canceled_quantity INT,
  item_replaced_id BIGINT,
  PRIMARY KEY (order_id, item_id),
  CONSTRAINT fk_order_items_order    FOREIGN KEY (order_id) REFERENCES dwh.orders(order_id),
  CONSTRAINT fk_order_items_item     FOREIGN KEY (item_id) REFERENCES dwh.items(item_id),
  CONSTRAINT fk_order_items_replaced FOREIGN KEY (item_replaced_id) REFERENCES dwh.items(item_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_created_at ON dwh.orders(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_user_id    ON dwh.orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_store_id   ON dwh.orders(store_id);
CREATE INDEX IF NOT EXISTS idx_orders_driver_id  ON dwh.orders(driver_id);
CREATE INDEX IF NOT EXISTS idx_order_items_item_id  ON dwh.order_items(item_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON dwh.order_items(order_id);
