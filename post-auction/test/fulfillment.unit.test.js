const test = require("node:test");
const assert = require("node:assert/strict");

const { createFulfillmentService } = require("../src/domain/fulfillment.service");

function createMockPool() {
  const state = {
    queries: [],
    sale: {
      sale_id: "sale-1",
      session_id: "session-1",
      basket_id: "basket-1",
      buyer_id: "buyer-1",
      winning_bid_id: "bid-1",
      sale_price: 120,
      pickup_location: null,
      pickup_time_window: null,
      delivery_available: null,
      fulfillment_status: "PENDING"
    },
    events: []
  };

  const client = {
    query: async (sql, params = []) => {
      state.queries.push({ sql, params });
      if (sql === "BEGIN" || sql === "COMMIT" || sql === "ROLLBACK") return { rowCount: 0, rows: [] };
      if (sql.includes("INSERT INTO event_store")) {
        state.events.push(params[4]);
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("SELECT boat_name, member_id FROM catalog_baskets_projection")) {
        return { rowCount: 1, rows: [{ boat_name: "Boat A", member_id: "member-1" }] };
      }
      if (sql.includes("INSERT INTO processed_events")) return { rowCount: 1, rows: [] };
      if (sql.includes("INSERT INTO fulfillment_sales")) return { rowCount: 1, rows: [state.sale] };
      if (sql.includes("UPDATE fulfillment_sales") && sql.includes("pickup_location")) {
        state.sale = { ...state.sale, pickup_location: params[1], pickup_time_window: params[2], fulfillment_status: "PICKUP_SCHEDULED" };
        return { rowCount: 1, rows: [state.sale] };
      }
      throw new Error(`Unexpected query: ${sql}`);
    },
    release: () => {}
  };

  return {
    state,
    connect: async () => client,
    query: async () => ({ rowCount: 1, rows: [] })
  };
}

test("checkDeliveryAvailability marks nearby addresses as available", () => {
  const service = createFulfillmentService({
    pool: createMockPool(),
    producer: { publish: async () => {} },
    getIo: () => ({ to: () => ({ emit: () => {} }), emit: () => {} })
  });

  assert.deepEqual(service.checkDeliveryAvailability("Gulbahce Urla Izmir"), {
    available: true,
    reason: "NEARBY_ADDRESS"
  });
  assert.deepEqual(service.checkDeliveryAvailability("Ankara"), {
    available: false,
    reason: "OUT_OF_DELIVERY_AREA"
  });
  assert.deepEqual(service.checkDeliveryAvailability(""), {
    available: false,
    reason: "ADDRESS_MISSING"
  });
});

test("handleBasketSold records sale and queues fulfillment.sale.recorded event", async () => {
  const pool = createMockPool();
  const published = [];
  const service = createFulfillmentService({
    pool,
    producer: { publish: async (topic, payload, key) => published.push({ topic, payload, key }) },
    getIo: () => ({ to: () => ({ emit: () => {} }), emit: () => {} })
  });

  await service.handleBasketSold({
    eventId: "11111111-1111-4111-8111-111111111111",
    sessionId: "session-1",
    basketId: "basket-1",
    buyerId: "buyer-1",
    winningBidId: "bid-1",
    salePrice: 120,
    occurredAt: "2026-06-21T10:00:00.000Z"
  });

  assert.equal(pool.state.events.length, 1);
  assert.equal(pool.state.events[0].basketId, "basket-1");
  assert.equal(pool.state.events[0].salePrice, 120);
  assert.equal(published.length, 1);
  assert.equal(published[0].topic, "fulfillment.sale.recorded");
});

test("schedulePickup updates sale and publishes pickup event", async () => {
  const pool = createMockPool();
  const published = [];
  const service = createFulfillmentService({
    pool,
    producer: { publish: async (topic, payload, key) => published.push({ topic, payload, key }) },
    getIo: () => ({ to: () => ({ emit: () => {} }), emit: () => {} })
  });

  const sale = await service.schedulePickup("basket-1", "Pier 1", "18:00-20:00");

  assert.equal(sale.pickup_location, "Pier 1");
  assert.equal(pool.state.events[0].pickupLocation, "Pier 1");
  assert.equal(published[0].topic, "fulfillment.pickup.scheduled");
});
