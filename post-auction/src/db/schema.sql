DROP TABLE IF EXISTS event_store CASCADE;
DROP TABLE IF EXISTS captain_payments CASCADE;
DROP TABLE IF EXISTS fulfillment_sales CASCADE;
DROP TABLE IF EXISTS processed_events CASCADE;
DROP TABLE IF EXISTS members_projection CASCADE;
DROP TABLE IF EXISTS bids CASCADE;
DROP TABLE IF EXISTS rebid_queue CASCADE;
DROP TABLE IF EXISTS auction_baskets CASCADE;
DROP TABLE IF EXISTS auction_sessions CASCADE;
DROP TABLE IF EXISTS buyers_projection CASCADE;
DROP TABLE IF EXISTS catalog_baskets_projection CASCADE;
DROP TABLE IF EXISTS sale_records CASCADE;

CREATE TABLE buyers_projection (
  buyer_id VARCHAR(100) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255),
  phone VARCHAR(50),
  address TEXT,
  occurred_at TIMESTAMPTZ,
  raw_payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE catalog_baskets_projection (
  basket_id VARCHAR(100) PRIMARY KEY,
  species VARCHAR(255),
  quantity NUMERIC(12, 2),
  unit VARCHAR(50),
  quality VARCHAR(50),
  base_price NUMERIC(12, 2),
  boat_name VARCHAR(255),
  member_id VARCHAR(100),
  occurred_at TIMESTAMPTZ,
  raw_payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE members_projection (
  member_id VARCHAR(100) PRIMARY KEY,
  member_name VARCHAR(255) NOT NULL,
  boat_name VARCHAR(255) NOT NULL,
  email VARCHAR(255),
  phone VARCHAR(50),
  occurred_at TIMESTAMPTZ,
  raw_payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE processed_events (
  event_id UUID PRIMARY KEY,
  topic VARCHAR(255) NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE event_store (
  id BIGSERIAL PRIMARY KEY,
  event_id UUID UNIQUE NOT NULL,
  aggregate_type VARCHAR(100) NOT NULL,
  aggregate_id VARCHAR(100) NOT NULL,
  event_type VARCHAR(150) NOT NULL,
  payload JSONB NOT NULL,
  topic VARCHAR(255) NOT NULL,
  key VARCHAR(255),
  occurred_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ,
  publish_error TEXT
);

CREATE TABLE auction_sessions (
  session_id VARCHAR(100) PRIMARY KEY,
  title VARCHAR(255),
  status VARCHAR(50) NOT NULL DEFAULT 'DRAFT',
  start_time TIMESTAMPTZ,
  rebid_round INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE auction_baskets (
  basket_id VARCHAR(100) PRIMARY KEY,
  session_id VARCHAR(100) NOT NULL REFERENCES auction_sessions(session_id) ON DELETE CASCADE,
  basket_no INTEGER,
  description TEXT,
  base_price NUMERIC(12, 2),
  details_ready BOOLEAN NOT NULL DEFAULT FALSE,
  status VARCHAR(50) NOT NULL DEFAULT 'LISTED',
  highest_bid NUMERIC(12, 2),
  opened_at TIMESTAMPTZ,
  closed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (session_id, basket_id)
);

CREATE TABLE bids (
  id BIGSERIAL PRIMARY KEY,
  session_id VARCHAR(100) NOT NULL,
  basket_id VARCHAR(100) NOT NULL,
  bidder_id VARCHAR(100) NOT NULL REFERENCES buyers_projection(buyer_id) ON DELETE CASCADE,
  bidder_name VARCHAR(255) NOT NULL,
  amount NUMERIC(12, 2) NOT NULL,
  placed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  FOREIGN KEY (session_id, basket_id) REFERENCES auction_baskets (session_id, basket_id) ON DELETE CASCADE
);

CREATE TABLE rebid_queue (
  id BIGSERIAL PRIMARY KEY,
  session_id VARCHAR(100) NOT NULL,
  basket_id VARCHAR(100) NOT NULL,
  reason TEXT,
  status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
  queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (basket_id),
  FOREIGN KEY (session_id, basket_id) REFERENCES auction_baskets (session_id, basket_id) ON DELETE CASCADE
);

CREATE TABLE fulfillment_sales (
  sale_id UUID PRIMARY KEY,
  session_id VARCHAR(100) NOT NULL,
  basket_id VARCHAR(100) NOT NULL UNIQUE,
  buyer_id VARCHAR(100) NOT NULL,
  winning_bid_id VARCHAR(100) NOT NULL,
  sale_price NUMERIC(12, 2) NOT NULL,
  boat_name VARCHAR(255),
  member_id VARCHAR(100),
  sale_status VARCHAR(50) NOT NULL DEFAULT 'RECORDED',
  pickup_location TEXT,
  pickup_time_window TEXT,
  delivery_address TEXT,
  delivery_available BOOLEAN,
  delivery_reason VARCHAR(100),
  fulfillment_status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  pickup_scheduled_at TIMESTAMPTZ,
  delivery_checked_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  raw_payload JSONB
);

CREATE TABLE captain_payments (
  captain_payment_id UUID PRIMARY KEY,
  session_id VARCHAR(100) NOT NULL,
  member_id VARCHAR(100),
  captain_name VARCHAR(255),
  boat_name VARCHAR(255) NOT NULL,
  gross_amount NUMERIC(12, 2) NOT NULL,
  commission_amount NUMERIC(12, 2) NOT NULL,
  net_amount NUMERIC(12, 2) NOT NULL,
  basket_ids JSONB NOT NULL,
  status VARCHAR(50) NOT NULL DEFAULT 'CALCULATED',
  calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (session_id, boat_name)
);

CREATE INDEX idx_event_store_aggregate ON event_store (aggregate_type, aggregate_id);
CREATE INDEX idx_bids_basket_id ON bids (basket_id);
CREATE INDEX idx_rebid_queue_session_status ON rebid_queue (session_id, status);
CREATE INDEX idx_fulfillment_sales_session ON fulfillment_sales (session_id);
CREATE INDEX idx_fulfillment_sales_status ON fulfillment_sales (fulfillment_status);
CREATE INDEX idx_captain_payments_session ON captain_payments (session_id);
