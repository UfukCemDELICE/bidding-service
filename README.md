# Auction & Bidding Service — Online Fish Auction System

This repository implements **Service 3: Auction & Bidding Service** for the Online Fish Auction System. It is an event-driven, autonomous microservice built with Node.js and Express, utilizing a private PostgreSQL database for storing local data projections and transaction states, and communicating with other services using Kafka.

---

## 1. Overview

The Auction & Bidding Service is a core component of the Online Fish Auction System's event-driven microservice architecture. It manages the active phase of fish auctions by:
1. Building local data projections from upstream Kafka events (`user.buyer.registered`, `catalog.basket.created`, and `catalog.published`) to remain self-contained.
2. Managing session and basket state transitions in real time.
3. Conducting active bidding phases (accepting bids, closing baskets, handling rebid queues).
4. Publishing domain events to downstream services via Kafka (`auction.session.started`, `auction.basket.opened`, `bid.placed`, `bid.basket.sold`, `bid.basket.unsold`, `bid.rebid.round.opened`, `bid.all.baskets.finalized`).

By keeping its own private database schema, this service remains decoupled from the databases of other microservices. Inter-service communication is achieved **solely via Kafka**, while Socket.IO is utilized strictly for live UI updates on the operator dashboard.

---

## 2. Service Responsibility

To maintain clear boundary lines within the microservice cluster, the responsibilities of this service are strictly defined:

### In Scope
* **Live Auction Session Management**: Creating READY sessions from catalog publications and transitioning them to LIVE.
* **Opening Baskets for Bidding**: Making specific fish baskets available for buyers to place bids.
* **Bid Collection**: Receiving, validating, and recording bids placed by registered buyers.
* **Highest Bid Selection**: Dynamically tracking the highest bid amount for each active basket.
* **Basket Sold / Unsold Decision**: Determining if a basket was successfully auctioned (sold to the highest bidder) or remained unsold.
* **Rebid Queue**: Automatically queueing baskets that received no bids during their active window.
* **Rebid Round Opening**: Allowing operators to initiate a secondary bidding round for all queued unsold baskets.
* **Bidding Phase Finalization**: Transitioning the entire session to a completed state once all baskets are finalized.

### Out of Scope
* **Payment Confirmation**: Handling credit cards, wire transfers, or updating payment status (belongs to Service 4).
* **Final Sale Completion**: Recording completed sales or invoices after payment (belongs to Service 4).
* **Sales Records**: Generating official sales ledger records (belongs to Service 4).
* **Pickup / Delivery**: Coordinating logistics, schedules, or warehouses (belongs to Service 4).
* **Fulfillment**: Managing buyer shipments or post-auction handovers (belongs to Service 4).
* **Captain Payment Calculation**: Determining payouts for boat captains based on sale prices (belongs to Service 4).
* **Auction Closing**: Performing post-auction accounting and finalizing transaction ledgers (belongs to Service 4).

---

## 3. Architecture Summary

The Auction & Bidding Service utilizes a clean, lightweight Node.js stack with Express, EJS, and Vanilla CSS. It avoids heavy ORMs and client-side framework overhead.

```mermaid
graph TD
    subgraph Upstream Services
        S1[Service 1: User Service]
        S2[Service 2: Catalog Service]
    end

    subgraph Kafka Broker
        Topic1[user.buyer.registered]
        Topic2[catalog.basket.created]
        Topic3[catalog.published]
        TopicOut[auction/bid topics...]
    end

    subgraph Auction & Bidding Service
        App[src/app.js]
        Cons[Kafka Consumer]
        Proj[bidding.service.js]
        DB[(PostgreSQL)]
        Ctrl[auction.controller.js]
        EJS[EJS Views]
        Socket[Socket.IO Server]
        Outbox[Transactional Outbox Flusher]
    end

    subgraph Client Browser
        UI[Live Operator Dashboard]
    end

    subgraph Downstream Services
        S4[Service 4: Post-Auction & Fulfillment]
    end

    %% Inputs
    S1 -->|Publish| Topic1
    S2 -->|Publish| Topic2
    S2 -->|Publish| Topic3

    Topic1 -->|Consume| Cons
    Topic2 -->|Consume| Cons
    Topic3 -->|Consume| Cons

    Cons -->|Update Projections| Proj
    Proj -->|Write| DB

    %% App Structure
    App --> Ctrl
    Ctrl --> Proj
    Proj -->|Write Outbox & Business State| DB
    Outbox -->|Batch Publish| TopicOut
    TopicOut -->|Consume| S4

    %% UI flow
    Ctrl -->|Render| EJS
    EJS --> UI
    Proj -->|Trigger Socket Event| Socket
    Socket -.->|Websocket Broadcast| UI
```

### Core Architecture Components:
* **Express MVC App**: Simplifies routing and request-response handling.
* **EJS Server-Rendered Views**: Generates views on the server side (`index.ejs` and `auction.ejs`) and delivers them to the client.
* **PostgreSQL Private Database**: Accessed via `pg` connection pooling. All schema definitions, bids, projections, and outbox logs are housed here.
* **Kafka Consumer**: Uses KafkaJS to automatically listen for buyer registration, basket creation, and catalog publication events.
* **Kafka Producer**: Handles event publishing using Confluent-compatible SSL and SASL configuration.
* **Transactional Outbox**: Guarantees at-least-once message delivery by recording outbound events in the database `event_store` within the same database transaction as the business state updates.
* **Socket.IO for Live UI Updates Only**: Broadcasts events to the operator UI (e.g., `bidPlaced`, `basketOpened`, `basketClosed`) so the dashboard reloads dynamically. It is **never** used for inter-service communication.

---

## 4. Repository Structure

Below is the file tree structure of the repository:

```text
BiddingService/
├── .env.example             # Template for local and production environment variables
├── package.json             # Service scripts, dependencies, and metadata
├── package-lock.json        # Locked dependency tree
├── README.md                # Technical service documentation
└── src/
    ├── app.js               # Application entry point, server setup, and outbox cron
    ├── controllers/
    │   └── auction.controller.js  # Express handlers routing HTTP to domain logic
    ├── db/
    │   ├── init.js          # Database schema migration initialization runner
    │   ├── pool.js          # Connection pool supporting SSL & environment parses
    │   └── schema.sql       # DDL SQL file containing table and index definitions
    ├── domain/
    │   └── bidding.service.js    # Core business domain logic & event handlers
    ├── kafka/
    │   ├── consumer.js      # Kafka consumer client setting up topics subscription
    │   ├── producer.js      # Kafka producer client setting up payload delivery
    │   └── topics.js        # Centralized definitions of consumed and published topics
    ├── public/
    │   └── main.js          # Client-side Socket.IO and periodic polling script
    ├── sockets/
    │   └── socket.js        # Socket.IO server initialization wrapper
    └── views/
        ├── auction.ejs      # Live auction session view (baskets, bids, rebid UI)
        └── index.ejs        # Dashboard overview listing all auction sessions
```

---

## 5. Prerequisites

To run this service locally or in a staging environment, make sure you have the following installed:

* **Node.js** (v18.x or v20.x LTS recommended)
* **npm** (v9.x or later)
* **PostgreSQL** (v14 or later, running locally or managed via Aiven/similar cloud provider)
* **Kafka Broker / Confluent Cloud Cluster** (supporting SASL/SSL plain authentication)
* **Git**

---

## 6. Environment Variables

Create a file named `.env` in the root of the project by copying `.env.example`.

### Kafka Credentials Mapping
If you are integrating with Confluent Cloud or a corporate Kafka platform, map your team credentials as follows:

| Environment Variable Name | Note / Details |
| :--- | :--- |
| `KAFKA_BROKERS` | Comma-separated list of brokers |
| `KAFKA_SASL_USERNAME` | Kafka security username credential |
| `KAFKA_SASL_PASSWORD` | Kafka security password credential |


### Example `.env` Configuration File

```env
PORT=3000
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/bid_service

# PostgreSQL Connection Override (Optional, used if DATABASE_URL is not set)
# PGHOST=localhost
# PGPORT=5432
# PGDATABASE=bid_service
# PGUSER=postgres
# PGPASSWORD=postgres
# PGSSL=false

# Kafka Broker Configuration
KAFKA_CLIENT_ID=auction-bidding-service
KAFKA_GROUP_ID=auction-bidding-service
KAFKA_BROKERS=localhost:9092

# SASL/SSL Configuration (Set KAFKA_SSL=true and supply username/password for Confluent Cloud)
KAFKA_SSL=true
KAFKA_SSL_REJECT_UNAUTHORIZED_FALSE=false
KAFKA_SASL_USERNAME=<api-key>
KAFKA_SASL_PASSWORD=<api-secret>
```

---

## 7. Installation & Setup

Follow these steps to initialize and start the service:

### Step 1: Clone the Repository
```bash
git clone <repository-url>
cd BiddingService
```

### Step 2: Install Node Dependencies
```bash
npm install
```

### Step 3: Create the Environment File
Copy the example environment file and fill in your PostgreSQL and Kafka details:
```bash
cp .env.example .env
```

### Step 4: Initialize the Database Schema
Ensure your PostgreSQL instance is running and the database matches your configuration, then run:
```bash
npm run db:init
```
*This script runs `src/db/init.js`, executing the DDL schema inside `src/db/schema.sql`.*

### Step 5: Start the Development Server
```bash
npm run dev
```
*This starts the service using `nodemon`. The console will display server startup logs and Kafka consumer partition assignments.*

---

## 8. Running the Service

Once the service starts:
1. Open your browser and navigate to `http://localhost:3000`.
2. If the service database has just been initialized and no upstream events have been consumed, you will see the following message:
   > **"No local auction sessions yet. Waiting for real Kafka catalog events from other services."**
3. The service relies on real Kafka events to populate its dashboards. The database projections remain empty until:
   * Upstream services publish user registrations (`user.buyer.registered`).
   * Upstream services publish catalog baskets (`catalog.basket.created`).
   * Upstream services publish catalog sessions (`catalog.published`).
4. Once these real events are consumed, the home dashboard will automatically update via Socket.IO to render the active sessions.

---

## 9. Kafka Integration Contract

The service implements an event-driven flow, subscribing to catalog and user topics and publishing bidding-specific events.

### Payload Standards
* **Key**: All published events use `sessionId` as their message key to ensure ordered processing of a session's events on the Kafka partition.
* **Format**: Payloads are serialized as JSON.
* **Envelope Fields**: Every event includes `eventId` (UUID) and `occurredAt` (ISO timestamp) inside the payload.
* **Headers**: Every published message includes headers for `content-type` (`application/json`) and `eventType` (matching the topic name).

### A. Consumed Events

| Topic | Producer Service | Purpose | Main Fields | Local Table Updated |
| :--- | :--- | :--- | :--- | :--- |
| `user.buyer.registered` | Service 1: User | Registers buyer profiles locally to allow validation of bid placements. | `id`/`buyerId`, `name`, `email`, `phone`, `occurredAt` | `buyers_projection` |
| `catalog.basket.created` | Service 2: Catalog | Receives basket profiles, ensuring baseline weights and descriptions are stored locally. | `id`/`basketId`, `species`, `quantity`, `basePrice`, `occurredAt` | `catalog_baskets_projection` |
| `catalog.published` | Service 2: Catalog | Signals that a catalog session has been finalized and published. Creates the session locally. | `id`/`sessionId`, `title`, `startTime`, `basketIds` (array) | `auction_sessions`, `auction_baskets` |

### B. Published Events

| Topic | Consumer Service(s) | Trigger | Main Fields | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `auction.session.started` | Post-Auction, UI | Operator clicks "Start Auction Session" for a `READY` session. | `sessionId`, `startTime`, `totalBaskets` | Transitions session to `LIVE` status. |
| `auction.basket.opened` | Bidders, UI | Operator clicks "Open Basket" for a `LISTED` or `REBID_READY` basket. | `sessionId`, `basketId`, `basePrice` | Transitions basket status to `OPEN`. |
| `bid.placed` | Bidders, UI | Buyer places a valid bid exceeding current highest bid and base price. | `sessionId`, `basketId`, `buyerId`, `bidId`, `bidAmount` | Appends a record to `bids` and updates `highest_bid`. |
| `bid.basket.sold` | Service 4, UI | Operator clicks "Close Basket" when the basket has at least one valid bid. | `sessionId`, `basketId`, `buyerId`, `winningBidId`, `salePrice` | Indicates highest bid was selected. **Does not imply payment completion**. |
| `bid.basket.unsold` | Service 4, UI | Operator clicks "Close Basket" when the basket has received no bids. | `sessionId`, `basketId`, `reason` ("NO_BIDS") | Transitions basket to `REBID_QUEUED` (1st round) or `FINAL_UNSOLD` (2nd round). |
| `bid.rebid.round.opened` | Bidders, UI | Operator clicks "Open Rebid Round" for pending unsold baskets. | `sessionId`, `roundNumber`, `basketIds` (array) | Increments rebid round count. Baskets move to `REBID_READY`. |
| `bid.all.baskets.finalized` | Service 4, UI | All baskets in the session have been finalized as `SOLD` or `FINAL_UNSOLD`. | `sessionId`, `totalBaskets`, `soldBasketCount`, `unsoldBasketCount` | Transitions session to `BIDDING_COMPLETED`. |

---

## 10. Database Model

The private PostgreSQL database acts as the source of truth for the local application state and projections. It consists of the following tables:

```mermaid
erDiagram
    buyers_projection {
        varchar buyer_id PK
        varchar name
        varchar email
        varchar phone
        text address
        timestamptz occurred_at
        jsonb raw_payload
        timestamptz created_at
    }

    catalog_baskets_projection {
        varchar basket_id PK
        varchar species
        numeric quantity
        varchar unit
        varchar quality
        numeric base_price
        varchar boat_name
        timestamptz occurred_at
        jsonb raw_payload
        timestamptz created_at
    }

    auction_sessions {
        varchar session_id PK
        varchar title
        varchar status
        timestamptz start_time
        integer rebid_round
        timestamptz created_at
        timestamptz updated_at
    }

    auction_baskets {
        varchar basket_id PK
        varchar session_id FK
        integer basket_no
        text description
        numeric base_price
        boolean details_ready
        varchar status
        numeric highest_bid
        timestamptz opened_at
        timestamptz closed_at
        timestamptz created_at
        timestamptz updated_at
    }

    bids {
        bigserial id PK
        varchar session_id FK
        varchar basket_id FK
        varchar bidder_id FK
        varchar bidder_name
        numeric amount
        timestamptz placed_at
    }

    rebid_queue {
        bigserial id PK
        varchar session_id FK
        varchar basket_id FK
        text reason
        varchar status
        timestamptz queued_at
    }

    event_store {
        bigserial id PK
        uuid event_id
        varchar aggregate_type
        varchar aggregate_id
        varchar event_type
        jsonb payload
        varchar topic
        varchar key
        timestamptz occurred_at
        timestamptz created_at
        timestamptz published_at
        text publish_error
    }

    auction_sessions ||--o{ auction_baskets : "contains"
    auction_baskets ||--o{ bids : "receives"
    buyers_projection ||--o{ bids : "places"
    auction_baskets ||--o{ rebid_queue : "references"
```

### Table Schema Details
1. **`buyers_projection`**: Local cache of registered buyers to validate bidder existence and display names in real time without querying user databases.
2. **`catalog_baskets_projection`**: Local cache of basket metadata published by the Catalog service.
3. **`auction_sessions`**: Manages state of active sessions (`DRAFT`, `READY`, `LIVE`, `BIDDING_COMPLETED`).
4. **`auction_baskets`**: Manages states of individual baskets inside a session. Base prices and descriptions are populated from the projections.
5. **`bids`**: Holds a log of all valid bids placed on active baskets.
6. **`rebid_queue`**: Stores baskets that did not receive any bids during their initial bidding phase, marking them as `PENDING` until a rebid round is officially initiated.
7. **`event_store`**: Outbox pattern log. Stores outbox events that need to be published to Kafka.

---

## 11. State Machine Transitions

The service acts as a state machine governing both auction sessions and individual baskets.

### Session States
* `DRAFT`: Initial schema layout (unused in primary workflow).
* `READY`: Catalog publication event (`catalog.published`) is consumed. The session and its associated baskets are registered.
* `LIVE`: Started by the operator dashboard. Actively processes bidding.
* `BIDDING_COMPLETED`: Set automatically when all baskets are finalized (either `SOLD` or `FINAL_UNSOLD`).

### Basket States
* `LISTED`: Registered in the session but not yet opened for bidding.
* `OPEN`: Opened by the operator. Buyers can actively submit bids.
* `SOLD`: Closed by the operator with at least one bid. Highest bid selected as the winner.
* `REBID_QUEUED`: Closed without bids on the first round. Sent to the rebid queue.
* `REBID_READY`: Promoted from `REBID_QUEUED` when a new rebid round is opened by the operator.
* `FINAL_UNSOLD`: Closed without bids during a rebid round. Finalized as unsold.

### State Transition Workflow
```mermaid
stateDiagram-v2
    [*] --> READY : catalog.published event consumed
    READY --> LIVE : POST /auction/:sessionId/start (operator starts session)
    
    state LIVE {
        [*] --> LISTED : basket registered
        LISTED --> OPEN : POST /auction/:sessionId/baskets/:basketId/open (operator opens basket)
        OPEN --> SOLD : POST /auction/:sessionId/baskets/:basketId/close (closed with bids)
        OPEN --> REBID_QUEUED : POST /auction/:sessionId/baskets/:basketId/close (closed with NO bids)
        
        REBID_QUEUED --> REBID_READY : POST /auction/:sessionId/rebid/open (operator opens rebid round)
        REBID_READY --> OPEN : POST /auction/:sessionId/baskets/:basketId/open (operator re-opens basket)
        
        OPEN --> FINAL_UNSOLD : POST /auction/:sessionId/baskets/:basketId/close (closed with NO bids during rebid round)
    }

    LIVE --> BIDDING_COMPLETED : All baskets finalized as SOLD or FINAL_UNSOLD
    BIDDING_COMPLETED --> [*] : bid.all.baskets.finalized event published
```

---

## 12. HTTP Routes & API Endpoints

The Express server exposes the following controller-backed endpoints:

### Frontend / View Endpoints
* **`GET /`**
  * **Purpose**: Renders the home screen (`index.ejs`), showing all auction sessions.
  * **Transitions**: None.
  * **Events Published**: None.
* **`GET /auction/:sessionId`**
  * **Purpose**: Renders the operator dashboard screen (`auction.ejs`) showing details, current active basket, bids, all session baskets, and the rebid queue.
  * **Transitions**: None.
  * **Events Published**: None.

### REST / Polling Endpoints
* **`GET /api/home/snapshot`**
  * **Purpose**: Returns a JSON token summarizing the total session count and last update timestamp. Used for client polling.
  * **Transitions**: None.
  * **Events Published**: None.
* **`GET /api/auction/:sessionId/snapshot`**
  * **Purpose**: Returns a JSON token summarizing the status of a specific session and its baskets. Used for client polling.
  * **Transitions**: None.
  * **Events Published**: None.

### State Modification Endpoints (Operator POST Actions)
* **`POST /auction/:sessionId/start`**
  * **Purpose**: Changes session status to `LIVE`.
  * **Transitions**: Session: `READY` -> `LIVE`.
  * **Events Published**: `auction.session.started`
* **`POST /auction/:sessionId/baskets/:basketId/open`**
  * **Purpose**: Opens a basket for live bidding.
  * **Transitions**: Basket: `LISTED` or `REBID_READY` -> `OPEN`.
  * **Events Published**: `auction.basket.opened`
* **`POST /auction/:sessionId/baskets/:basketId/bid`**
  * **Purpose**: Submits a bid from a buyer. Validates that the amount exceeds the base price and the current highest bid, and that the bidder exists.
  * **Transitions**: None (registers bid and updates `highest_bid` on basket).
  * **Events Published**: `bid.placed`
* **`POST /auction/:sessionId/baskets/:basketId/close`**
  * **Purpose**: Closes bidding on the active basket.
  * **Transitions**:
    * If bids exist: Basket: `OPEN` -> `SOLD`.
    * If no bids (first round): Basket: `OPEN` -> `REBID_QUEUED` (adds to rebid queue with status `PENDING`).
    * If no bids (rebid round): Basket: `OPEN` -> `FINAL_UNSOLD` (marks queue entry as `FINAL_UNSOLD`).
    * If all session baskets are finalized: Session: `LIVE` -> `BIDDING_COMPLETED`.
  * **Events Published**: `bid.basket.sold` OR `bid.basket.unsold`, and potentially `bid.all.baskets.finalized`.
* **`POST /auction/:sessionId/rebid/open`**
  * **Purpose**: Moves all `PENDING` unsold baskets in the rebid queue to `REBID_READY`.
  * **Transitions**: Session: increments `rebid_round` count. Basket: `REBID_QUEUED` -> `REBID_READY`. Rebid Queue: `PENDING` -> `OPENED`.
  * **Events Published**: `bid.rebid.round.opened`

---

## 13. Real-Time UI Synchronization

Real-time updates are pushed to the user interface using a hybrid approach:
1. **Socket.IO Rooms**: On page load, the client dashboard joins a Socket.IO room named after the `sessionId`.
2. **WebSocket Broadcasts**: Whenever a state change occurs (e.g. `bidPlaced`, `basketOpened`, `basketClosed`), the server sends a socket event to the room.
3. **Graceful Reload**: Upon receiving a websocket event, the client displays a notification banner ("New bid placed. Refreshing view...") and automatically reloads the page after `800ms`.
4. **Best-Effort Polling**: A background JavaScript handler polls `/api/auction/:sessionId/snapshot` every 3 seconds. If the snapshot token changes (e.g. if the WebSocket connection was briefly interrupted), it triggers a reload.

> [!NOTE]
> Socket.IO is strictly an internal dashboard visual synchronization layer. It is not used for backend communication or microservice integration.

---

## 14. Transactional Outbox Pattern

To prevent issues like dual-write failures (where a database update succeeds but a network issue prevents publishing the corresponding Kafka event, or vice versa), the service implements the **Transactional Outbox** pattern.

```text
[HTTP POST / Kafka Event]
        │
        ▼
┌──────────────────────────────────────┐
│        withTx Database Transaction   │
│                                      │
│  1. Update Business Tables           │
│     (e.g., set basket to 'SOLD')     │
│  2. Insert Outbox Record             │
│     (write event to event_store)     │
└──────────────────┬───────────────────┘
                   │
         Commit Successful
                   │
                   ▼
┌──────────────────────────────────────┐
│         Immediate Kafka Publish      │
├──────────────────┬───────────────────┤
│     SUCCESS      │      FAILURE      │
│                  │                   │
│ Update outbox:   │ Update outbox:    │
│ Set published_at │ Set publish_error │
│                  │                   │
└──────────────────┴─────────┬─────────┘
                             │
                  Startup / Periodic Cron
                             │
                             ▼
               ┌───────────────────────────┐
               │    Outbox Retry Loop      │
               │   FOR UPDATE SKIP LOCKED  │
               └───────────────────────────┘
```

### How It Works:
1. **Atomic Transaction**: Every operation that updates state and emits an event (such as placing a bid or closing a basket) is wrapped in the `withTx` helper. It opens a database transaction, applies the state update, writes the Kafka event payload to `event_store`, and commits.
2. **Immediate Post-Commit Publish**: Once the transaction commits, the service immediately attempts to publish the event to Kafka.
   * If successful, the event's `published_at` timestamp is set to the current time.
   * If publishing fails, the error message is recorded in the `publish_error` column.
3. **Startup Flusher**: When the service boots, it automatically runs `flushUnpublishedOutbox` once to publish any outstanding events.
4. **Periodic Retry Loop**: A background interval runs every 10 seconds, calling `flushUnpublishedOutbox` to process and retry any events where `published_at IS NULL`.
5. **Safe Batching with PostgreSQL Locking**: The flusher retrieves events in batches of 100 using the following query:
   ```sql
   SELECT id, topic, key, payload FROM event_store
   WHERE published_at IS NULL
   ORDER BY created_at ASC
   LIMIT 100
   FOR UPDATE SKIP LOCKED;
   ```
   *Using `FOR UPDATE SKIP LOCKED` allows multiple instances of the service to run concurrently without locking issues or duplicate event publishing.*

---

## 15. Verification & Testing

The service has been verified using real Kafka integration and database assertions.

### Integration Assertions
* **Consumer Group Join**: The service successfully joins the group `auction-bidding-service` (or dynamically configured replicas) and maps partitions.
* **Upstream Consumption**: The service successfully processes upstream events (`user.buyer.registered`, `catalog.basket.created`, and `catalog.published`) and updates local projection tables.
* **Outbox Publishing**: Outbox events successfully publish to Kafka and update their `published_at` status in `event_store`.

### SQL Queries for Verification
Run these queries in your PostgreSQL terminal to monitor database state:

```sql
-- 1. Check all auction sessions and their status
SELECT session_id, title, status, rebid_round, start_time 
FROM auction_sessions;

-- 2. Check the status and details of baskets inside a session
SELECT session_id, basket_id, basket_no, status, base_price, highest_bid, details_ready
FROM auction_baskets
ORDER BY session_id, basket_no;

-- 3. Review the outbox log to verify successful Kafka deliveries
SELECT topic, key, payload->>'eventId' as event_id, published_at, publish_error
FROM event_store
ORDER BY created_at DESC
LIMIT 10;
```

### Verified Test Run Result
The end-to-end bidding flow was verified using the session `session-2026-06-17-c6464af7`.

The final status of the session and its baskets was verified as follows:
* **Session ID**: `session-2026-06-17-c6464af7`
* **Final Session Status**: `BIDDING_COMPLETED`
* **Basket 1**: `SOLD`
* **Basket 2**: `FINAL_UNSOLD` (closed without bids during the rebid round)
* **Basket 3**: `SOLD`
* **Final Event Emitted**: `bid.all.baskets.finalized`
* **Summary Counts**:
  * Total Baskets: `3`
  * Sold Basket Count: `2`
  * Unsold Basket Count: `1`

---

## 16. Important Notes & Limitations

* **Upstream Dependency**: This service does not create sessions or baskets manually. It builds projections from upstream services. If Kafka is down or upstream services have not published catalog events, the dashboards will remain empty.
* **No Mock Event Generator**: There is no mock data seeder built into the final runtime path. The service must consume real, valid Kafka events to populate its database projections.
* **Fulfillment Boundaries**: This service does not handle shipping, payment confirmation, captain payout calculations, or final invoicing. Any downstream action following the bidding phase is the responsibility of Service 4: Post-Auction & Fulfillment.

---

## 17. Team 

Team members:

* **Ufuk Cem Delice** 
* **Egehan Vardar**  
* **Mustafa Şengül** 

---

## 18. Report Support

This service demonstrates event-driven microservice collaboration by consuming upstream domain events to build private local projections and publishing bidding-domain events through a reliable transactional outbox. The design keeps the service autonomous, avoids shared databases, and separates bidding responsibilities from post-auction payment and fulfillment.
