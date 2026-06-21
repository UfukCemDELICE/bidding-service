const test = require("node:test");
const assert = require("node:assert/strict");
const { execFileSync } = require("node:child_process");

const pool = require("../src/db/pool");
const { createFulfillmentService } = require("../src/domain/fulfillment.service");

function ensureDockerPostgres() {
  execFileSync("docker", ["start", "bidding-postgres"], { stdio: "ignore" });
  for (let i = 0; i < 30; i += 1) {
    try {
      execFileSync("docker", ["exec", "bidding-postgres", "pg_isready", "-U", "postgres", "-d", "bidding_service"], { stdio: "ignore" });
      return;
    } catch (error) {
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 500);
    }
  }
  throw new Error("Postgres container is not ready");
}

async function resetDb() {
  ensureDockerPostgres();
  execFileSync("npm", ["run", "db:init"], { stdio: "ignore" });
}

test("Post-Auction fulfillment flow persists state and records outbound events", async () => {
  await resetDb();
  const published = [];
  const service = createFulfillmentService({
    pool,
    producer: { publish: async (topic, payload, key) => published.push({ topic, payload, key }) },
    getIo: () => ({ to: () => ({ emit: () => {} }), emit: () => {} })
  });

  await service.handleBuyerRegistered({
    eventId: "22222222-2222-4222-8222-222222222222",
    buyerId: "buyer-1",
    name: "Buyer One",
    address: "Urla Izmir",
    occurredAt: "2026-06-21T10:00:00.000Z"
  });
  await service.handleMemberRegistered({
    eventId: "33333333-3333-4333-8333-333333333333",
    memberId: "member-1",
    memberName: "Captain One",
    boatName: "Boat A",
    occurredAt: "2026-06-21T10:00:00.000Z"
  });
  await service.handleBasketCreated({
    eventId: "44444444-4444-4444-8444-444444444444",
    basketId: "basket-1",
    species: "Sea Bass",
    quantity: 10,
    unit: "kg",
    quality: "A",
    basePrice: 50,
    boatName: "Boat A",
    memberId: "member-1",
    occurredAt: "2026-06-21T10:00:00.000Z"
  });
  await service.handleBasketSold({
    eventId: "55555555-5555-4555-8555-555555555555",
    sessionId: "session-1",
    basketId: "basket-1",
    buyerId: "buyer-1",
    winningBidId: "bid-1",
    salePrice: 200,
    occurredAt: "2026-06-21T10:10:00.000Z"
  });
  await service.schedulePickup("basket-1", "Pier 1", "18:00-20:00");
  await service.checkDelivery("basket-1");
  await service.completeBasket("basket-1");
  await service.calculateCaptainPayments("session-1");
  await service.closeAuction("session-1");

  const sales = await pool.query("SELECT * FROM fulfillment_sales WHERE basket_id = 'basket-1'");
  assert.equal(sales.rows[0].fulfillment_status, "COMPLETED");
  assert.equal(sales.rows[0].delivery_available, true);

  const payments = await pool.query("SELECT * FROM captain_payments WHERE session_id = 'session-1'");
  assert.equal(payments.rowCount, 1);
  assert.equal(Number(payments.rows[0].gross_amount), 200);
  assert.equal(Number(payments.rows[0].commission_amount), 20);
  assert.equal(Number(payments.rows[0].net_amount), 180);

  const eventStore = await pool.query("SELECT event_type FROM event_store ORDER BY id");
  assert.deepEqual(eventStore.rows.map((row) => row.event_type), [
    "fulfillment.sale.recorded",
    "fulfillment.pickup.scheduled",
    "fulfillment.delivery.checked",
    "fulfillment.basket.completed",
    "fulfillment.captain.payment.calculated",
    "fulfillment.auction.closed"
  ]);
  assert.equal(published.length, 6);
});

test("Fulfillment page renders and shows persisted sale", async () => {
  const { once } = require("node:events");
  const http = require("node:http");
  const express = require("express");
  const path = require("node:path");
  const routes = require("../src/routes/fulfillment.routes");

  const app = express();
  app.set("view engine", "ejs");
  app.set("views", path.join(__dirname, "../src/views"));
  app.use(express.urlencoded({ extended: true }));
  app.use(express.json());
  app.use("/", routes);
  const server = http.createServer(app);
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();

  try {
    const response = await fetch(`http://127.0.0.1:${port}/fulfillment`);
    const html = await response.text();
    assert.equal(response.status, 200);
    assert.match(html, /Post-Auction & Fulfillment/);
    assert.match(html, /basket-1/);
    assert.match(html, /Captain payouts/);
  } finally {
    server.close();
  }
});

test.after(async () => {
  await pool.end();
});
