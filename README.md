# Post-Auction & Fulfillment Service

This repository hosts the **Post-Auction & Fulfillment Service** — Service 4 in the Online Fish Auction System. It is an isolated, deployable Node.js/Express microservice with a private PostgreSQL database and Kafka-based inter-service communication.

The service lives entirely under `post-auction/`. It does not contain any Auction & Bidding Service code; that service is a separate microservice in a separate repository/deployment.

## 1. Overview

Post-Auction & Fulfillment is responsible for the post-sale phase of a fish auction:

1. **Recording sales** when a basket is sold (`bid.basket.sold`).
2. **Managing pickup information** for sold baskets.
3. **Checking whether delivery is possible** for nearby buyer addresses.
4. **Calculating the amount to be paid to each captain** for a closed auction session.
5. **Closing the auction** (post-auction finalization) and publishing the `fulfillment.auction.closed` event.

The service keeps its own private Postgres schema. It builds local projections from a small set of upstream Kafka events (`user.buyer.registered`, `user.member.registered`, `catalog.basket.created`, `catalog.published`, `bid.basket.sold`, `bid.all.baskets.finalized`) so it can answer fulfillment questions without calling other services over HTTP. All inter-service communication is **solely via Kafka**.

## 2. Repository layout

```text
.
├── README.md                # This file (single source of truth)
├── .gitignore
└── post-auction/
    ├── package.json
    ├── package-lock.json
    ├── Dockerfile
    ├── .dockerignore
    ├── .env.example
    ├── .gitignore
    ├── src/
    │   ├── app.js
    │   ├── controllers/fulfillment.controller.js
    │   ├── routes/fulfillment.routes.js
    │   ├── domain/fulfillment.service.js
    │   ├── db/{init,pool,schema.sql}
    │   ├── kafka/{config,consumer,producer,schema-registry,topics}.js
    │   ├── views/{fulfillment.ejs,index.ejs}
    │   ├── public/main.js
    │   └── socket.js
    ├── schema/              # JSON schemas for consumed + produced events
    │   ├── fulfillment.*.schema.json   (6 files)
    │   └── user.member.registered.schema.json
    └── test/
        ├── fulfillment.unit.test.js
        └── fulfillment.integration.test.js
```

## 3. Kafka contract

### Consume

| Topic | Purpose |
|---|---|
| `user.buyer.registered` | Buyer contact + address projection (used for delivery check) |
| `user.member.registered` | Captain/member projection (used for payout) |
| `catalog.basket.created` | Basket → boat/captain projection |
| `catalog.published` | Catalog publication marker |
| `bid.basket.sold` | Triggers sale recording |
| `bid.all.baskets.finalized` | Triggers captain payout + auction close |

### Publish

| Topic | Emitted when |
|---|---|
| `fulfillment.sale.recorded` | A `bid.basket.sold` event has been processed and a sale row created |
| `fulfillment.pickup.scheduled` | Pickup location + time window saved for a sale |
| `fulfillment.delivery.checked` | Delivery availability check completed for a sale |
| `fulfillment.basket.completed` | A basket was marked fulfilled |
| `fulfillment.captain.payment.calculated` | Captain payouts computed for a session |
| `fulfillment.auction.closed` | Post-auction closing completed for a session |

Kafka group: `post-auction-service-local` (override via `KAFKA_GROUP_ID`).
Kafka client: `post-auction-service` (override via `KAFKA_CLIENT_ID`).

## 4. HTTP API

| Method | Path | Purpose |
|---|---|---|
| GET | `/` | Dashboard index |
| GET | `/fulfillment` | Post-Auction dashboard UI |
| GET | `/api/fulfillment/snapshot` | Local projections + sales snapshot |
| GET | `/health` | Liveness check |
| POST | `/fulfillment/sales/:basketId/pickup` | Schedule pickup |
| POST | `/fulfillment/sales/:basketId/delivery/check` | Run delivery availability check |
| POST | `/fulfillment/sales/:basketId/complete` | Mark basket fulfilled |
| POST | `/fulfillment/sessions/:sessionId/captain-payments/calculate` | Calculate captain payouts |
| POST | `/fulfillment/sessions/:sessionId/close` | Close auction session |

The UI lives at `http://<host>:<port>/fulfillment`.

## 5. Prerequisites

- Node.js 22 LTS
- npm 9+
- PostgreSQL 14+ (local Docker container recommended)
- A Kafka broker / Confluent Cloud cluster reachable from the service host
- Docker (for local Postgres)

## 6. Environment variables

All env vars are read from `post-auction/.env`. A template lives at `post-auction/.env.example`.

| Variable | Description |
|---|---|
| `PORT` | HTTP port (default `3000`) |
| `PGHOST` | Postgres host |
| `PGPORT` | Postgres port (e.g. `5432` inside container, `5433` if published) |
| `PGDATABASE` | Database name (e.g. `post_auction_service`) |
| `PGUSER` | Postgres user |
| `PGPASSWORD` | Postgres password |
| `KAFKA_BROKERS` | Comma-separated bootstrap servers |
| `KAFKA_SASL_USERNAME` | Kafka SASL username (API key for Confluent Cloud) |
| `KAFKA_SASL_PASSWORD` | Kafka SASL password (API secret) |
| `KAFKA_GROUP_ID` | Consumer group (default `post-auction-service-local`) |
| `KAFKA_CLIENT_ID` | Producer/consumer client id (default `post-auction-service`) |

## 7. Local setup

```bash
# 1. Start a private Postgres container
docker run --name post-auction-postgres \
  -e POSTGRES_DB=post_auction_service \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 \
  -d postgres:16

# (later)
docker start post-auction-postgres

# 2. Install + configure
cd post-auction
npm install
cp .env.example .env
# edit .env: PGPASSWORD, KAFKA_BROKERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD

# 3. Initialize schema
npm run db:init

# 4. Run
npm run dev
```

App listens on `PORT` (default `3000`). UI: `http://localhost:3000/fulfillment`.

## 8. Required Kafka topics

The cluster must contain these topics for live publishing to succeed:

```text
fulfillment.sale.recorded
fulfillment.pickup.scheduled
fulfillment.delivery.checked
fulfillment.basket.completed
fulfillment.captain.payment.calculated
fulfillment.auction.closed
```

The consumed topics must also exist on the cluster:

```text
user.buyer.registered
user.member.registered
catalog.basket.created
catalog.published
bid.basket.sold
bid.all.baskets.finalized
```

If topic creation is restricted at the API key level (`TOPIC_AUTHORIZATION_FAILED`), ask the Kafka admin team to provision any missing topics.

## 9. Database model

`post-auction/src/db/schema.sql` creates these tables:

- `buyers_projection` — local copy of buyer contact/address
- `members_projection` — local copy of captain/member records
- `catalog_baskets_projection` — local copy of basket → captain mapping
- `fulfillment_sales` — recorded sales + pickup + delivery state
- `captain_payments` — calculated captain payouts per session
- `event_store` — transactional outbox for `fulfillment.*` events
- `processed_events` — idempotency log of consumed event IDs

## 10. Tests

```bash
cd post-auction
npm test
```

Coverage:

- Unit: delivery availability logic, captain payout math, event creation
- Integration: `bid.basket.sold` → sale recorded → pickup → delivery → complete → captain payment → auction close
- Outbox/event_store verification for all `fulfillment.*` topics
- Frontend render + API integration for `/fulfillment`

## 11. 10-second verification

With Docker/Postgres running and `.env` filled:

```bash
cd post-auction
npm test
curl -I http://localhost:3000/fulfillment
```

Expected:

- `npm test` reports all tests passing
- `curl` returns `HTTP/1.1 200 OK`

## 12. Notes

- This service is intentionally isolated. There is no HTTP-level coupling to the Auction service; all cross-service communication is through Kafka.
- Local key files such as `api-key-*.txt` are ignored by git.
- The service has been verified locally; live deployment (Railway or another platform) is not yet complete.
