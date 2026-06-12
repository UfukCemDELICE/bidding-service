CREATE TABLE IF NOT EXISTS event_store (
  id BIGSERIAL PRIMARY KEY,
  aggregate_type VARCHAR(100) NOT NULL,
  aggregate_id VARCHAR(100) NOT NULL,
  event_type VARCHAR(150) NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS auction_sessions (
  session_id VARCHAR(100) PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  status VARCHAR(50) NOT NULL DEFAULT 'SCHEDULED',
  source_payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS auction_baskets (
  basket_id VARCHAR(100) PRIMARY KEY,
  session_id VARCHAR(100) NOT NULL REFERENCES auction_sessions(session_id) ON DELETE CASCADE,
  basket_no INTEGER,
  description TEXT,
  starting_price NUMERIC(12, 2) NOT NULL DEFAULT 0,
  status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
  highest_bid NUMERIC(12, 2),
  opened_at TIMESTAMPTZ,
  closed_at TIMESTAMPTZ,
  source_payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bids (
  id BIGSERIAL PRIMARY KEY,
  session_id VARCHAR(100) NOT NULL REFERENCES auction_sessions(session_id) ON DELETE CASCADE,
  basket_id VARCHAR(100) NOT NULL REFERENCES auction_baskets(basket_id) ON DELETE CASCADE,
  bidder_id VARCHAR(100) NOT NULL,
  bidder_name VARCHAR(255) NOT NULL,
  amount NUMERIC(12, 2) NOT NULL,
  placed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rebid_queue (
  id BIGSERIAL PRIMARY KEY,
  session_id VARCHAR(100) NOT NULL REFERENCES auction_sessions(session_id) ON DELETE CASCADE,
  basket_id VARCHAR(100) NOT NULL REFERENCES auction_baskets(basket_id) ON DELETE CASCADE,
  reason TEXT,
  status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
  queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (basket_id)
);

CREATE TABLE IF NOT EXISTS sale_records (
  id BIGSERIAL PRIMARY KEY,
  session_id VARCHAR(100) NOT NULL REFERENCES auction_sessions(session_id) ON DELETE CASCADE,
  basket_id VARCHAR(100) NOT NULL REFERENCES auction_baskets(basket_id) ON DELETE CASCADE,
  winning_bid_id BIGINT REFERENCES bids(id),
  winner_id VARCHAR(100),
  winning_bid_amount NUMERIC(12, 2) NOT NULL,
  payment_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
  sold_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (basket_id)
);

CREATE INDEX IF NOT EXISTS idx_event_store_aggregate ON event_store (aggregate_type, aggregate_id);
CREATE INDEX IF NOT EXISTS idx_bids_basket_id ON bids (basket_id);
CREATE INDEX IF NOT EXISTS idx_rebid_queue_session_status ON rebid_queue (session_id, status);
CREATE INDEX IF NOT EXISTS idx_sale_records_session_id ON sale_records (session_id);
