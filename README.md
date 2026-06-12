# BID Service

Single-service BID microservice for the event-driven fish auction system.

## Run

1. Copy `.env.example` to `.env` and update PostgreSQL / Kafka values.
2. Run `npm install` if dependencies are not already present.
3. Create the database, then run `npm run db:init`.
4. Start the service with `npm run dev`.
5. Open `http://localhost:3000`.

## Managed PostgreSQL and Kafka

For a managed provider such as Aiven, copy the connection values from the provider dashboard into `.env`.

Example:

```env
PORT=3000
DATABASE_URL=postgres://USERNAME:PASSWORD@PG_HOST:PG_PORT/defaultdb?sslmode=require
PGHOST=PG_HOST
PGPORT=PG_PORT
PGDATABASE=defaultdb
PGUSER=USERNAME
PGPASSWORD=PASSWORD
PGSSL=true
PGSSLMODE=require
PGSSL_REJECT_UNAUTHORIZED_FALSE=false
PGSSL_CA=-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----

KAFKA_CLIENT_ID=bid-service
KAFKA_BROKERS=KAFKA_HOST:KAFKA_PORT
KAFKA_GROUP_ID=bid-service-group
KAFKA_SSL=true
KAFKA_SSL_REJECT_UNAUTHORIZED_FALSE=false
KAFKA_SSL_CA=-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----
KAFKA_SSL_CERT=-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----
KAFKA_SSL_KEY=-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----
KAFKA_SASL_MECHANISM=plain
KAFKA_SASL_USERNAME=KAFKA_USERNAME
KAFKA_SASL_PASSWORD=KAFKA_PASSWORD
```

If you previously committed real credentials into `.env.example`, rotate them in the provider dashboard and keep real secrets only in `.env`.

For Aiven PostgreSQL, prefer setting `PGSSL_CA` to the CA certificate from the dashboard with newlines escaped as `\n`.

For Aiven Kafka, use either:

- TLS client certificate auth with `KAFKA_SSL_CA`, `KAFKA_SSL_CERT`, and `KAFKA_SSL_KEY`
- SASL auth with `KAFKA_SASL_USERNAME` and `KAFKA_SASL_PASSWORD`

## Minimal local seed flow

The service is projection-driven, so the preferred seed path is Kafka:

- publish `cls.auction.listing.published` with a payload containing `sessionId`, `title`, and `baskets`
- publish `main.online.auction.live` with `sessionId`

Example listing payload shape:

```json
{
  "sessionId": "session-001",
  "title": "Morning Fish Auction",
  "baskets": [
    {
      "basketId": "basket-001",
      "basketNo": 1,
      "description": "Anchovy - 25kg",
      "startingPrice": 100
    },
    {
      "basketId": "basket-002",
      "basketNo": 2,
      "description": "Sea Bass - 12kg",
      "startingPrice": 180
    }
  ]
}
```

If Kafka is unavailable, you can still inspect the pages after manually inserting rows into `auction_sessions` and `auction_baskets`.
