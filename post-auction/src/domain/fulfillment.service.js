const crypto = require("crypto");

const pool = require("../db/pool");
const producer = require("../kafka/producer");
const topics = require("../kafka/topics");
const { getIo } = require("../socket");

const DEFAULT_COMMISSION_RATE = 0.1;
const DEFAULT_PICKUP_LOCATION = "Fish Auction Pickup Point";
const NEARBY_KEYWORDS = ["iyte", "gulbahce", "urla", "izmir"];

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function toIso(value) {
  if (!value) return new Date().toISOString();
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? new Date().toISOString() : date.toISOString();
}

function normalizeEventId(value) {
  return value || crypto.randomUUID();
}

function createFulfillmentService(deps = {}) {
  const db = deps.pool || pool;
  const kafkaProducer = deps.producer || producer;
  const socketProvider = deps.getIo || getIo;
  const commissionRate = deps.commissionRate ?? DEFAULT_COMMISSION_RATE;

  async function publishEvent(client, eventType, aggregateId, payload, topic, key) {
    const eventId = crypto.randomUUID();
    const occurredAt = new Date().toISOString();
    const eventPayload = {
      ...payload,
      eventId,
      occurredAt
    };

    await client.query(
      `
      INSERT INTO event_store (event_id, aggregate_type, aggregate_id, event_type, payload, topic, key, occurred_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `,
      [eventId, "fulfillment", aggregateId, eventType, eventPayload, topic, key || payload.sessionId, occurredAt]
    );

    return { topic, key: key || payload.sessionId, payload: eventPayload };
  }

  async function withTx(action) {
    const client = await db.connect();
    const events = [];
    let committed = false;

    try {
      await client.query("BEGIN");
      const result = await action(client, async (...args) => {
        const event = await publishEvent(client, ...args);
        events.push(event);
        return event;
      });
      await client.query("COMMIT");
      committed = true;

      for (const event of events) {
        try {
          await kafkaProducer.publish(event.topic, event.payload, event.key);
          await db.query("UPDATE event_store SET published_at = NOW(), publish_error = NULL WHERE event_id = $1", [event.payload.eventId]);
        } catch (error) {
          await db.query("UPDATE event_store SET publish_error = $2 WHERE event_id = $1", [event.payload.eventId, error.message]);
        }
      }

      return result;
    } catch (error) {
      if (!committed) {
        await client.query("ROLLBACK");
      }
      throw error;
    } finally {
      client.release();
    }
  }

  async function markProcessed(client, eventId, topic) {
    const normalized = normalizeEventId(eventId);
    const result = await client.query(
      "INSERT INTO processed_events (event_id, topic) VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING",
      [normalized, topic]
    );
    return result.rowCount === 1;
  }

  function emit(sessionId, eventName, payload) {
    try {
      socketProvider().to(sessionId).emit(eventName, payload);
      socketProvider().emit("fulfillmentUpdated", payload);
    } catch (error) {
      // Socket.IO is optional in tests and during early startup.
    }
  }

  async function handleBuyerRegistered(payload) {
    const buyerId = payload.buyerId || payload.id;
    if (!buyerId) return;

    await db.query(
      `
      INSERT INTO buyers_projection (buyer_id, name, email, phone, address, occurred_at, raw_payload)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (buyer_id) DO UPDATE SET
        name = EXCLUDED.name,
        email = EXCLUDED.email,
        phone = EXCLUDED.phone,
        address = EXCLUDED.address,
        occurred_at = EXCLUDED.occurred_at,
        raw_payload = EXCLUDED.raw_payload
      `,
      [
        buyerId,
        payload.name || payload.username || "Unknown Buyer",
        payload.email || null,
        payload.phone || null,
        payload.address || null,
        toIso(payload.occurredAt),
        payload
      ]
    );
  }

  async function handleMemberRegistered(payload) {
    const memberId = payload.memberId || payload.captainId || payload.id;
    if (!memberId) return;

    await db.query(
      `
      INSERT INTO members_projection (member_id, member_name, boat_name, email, phone, occurred_at, raw_payload)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (member_id) DO UPDATE SET
        member_name = EXCLUDED.member_name,
        boat_name = EXCLUDED.boat_name,
        email = EXCLUDED.email,
        phone = EXCLUDED.phone,
        occurred_at = EXCLUDED.occurred_at,
        raw_payload = EXCLUDED.raw_payload
      `,
      [
        memberId,
        payload.memberName || payload.name || "Unknown Captain",
        payload.boatName || "Unknown Boat",
        payload.email || null,
        payload.phone || null,
        toIso(payload.occurredAt),
        payload
      ]
    );
  }

  async function handleBasketCreated(payload) {
    const basketId = payload.basketId;
    if (!basketId) return;

    await db.query(
      `
      INSERT INTO catalog_baskets_projection (basket_id, species, quantity, unit, quality, base_price, boat_name, member_id, occurred_at, raw_payload)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      ON CONFLICT (basket_id) DO UPDATE SET
        species = EXCLUDED.species,
        quantity = EXCLUDED.quantity,
        unit = EXCLUDED.unit,
        quality = EXCLUDED.quality,
        base_price = EXCLUDED.base_price,
        boat_name = EXCLUDED.boat_name,
        member_id = EXCLUDED.member_id,
        occurred_at = EXCLUDED.occurred_at,
        raw_payload = EXCLUDED.raw_payload
      `,
      [
        basketId,
        payload.species || null,
        payload.quantity || null,
        payload.unit || null,
        payload.quality || null,
        payload.basePrice || null,
        payload.boatName || null,
        payload.memberId || null,
        toIso(payload.occurredAt),
        payload
      ]
    );
  }

  async function handleBasketSold(payload) {
    return withTx(async (client, registerEvent) => {
      const isNew = await markProcessed(client, payload.eventId, topics.consumedTopics.BID_BASKET_SOLD);
      if (!isNew) {
        return { duplicate: true };
      }

      const catalog = await client.query("SELECT boat_name, member_id FROM catalog_baskets_projection WHERE basket_id = $1", [payload.basketId]);
      const basketProjection = catalog.rows[0] || {};
      const saleId = crypto.randomUUID();
      const occurredAt = toIso(payload.occurredAt);

      const result = await client.query(
        `
        INSERT INTO fulfillment_sales (
          sale_id, session_id, basket_id, buyer_id, winning_bid_id, sale_price,
          boat_name, member_id, sale_status, fulfillment_status, recorded_at, updated_at, raw_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'RECORDED', 'PENDING', $9, NOW(), $10)
        ON CONFLICT (basket_id) DO UPDATE SET
          session_id = EXCLUDED.session_id,
          buyer_id = EXCLUDED.buyer_id,
          winning_bid_id = EXCLUDED.winning_bid_id,
          sale_price = EXCLUDED.sale_price,
          boat_name = COALESCE(EXCLUDED.boat_name, fulfillment_sales.boat_name),
          member_id = COALESCE(EXCLUDED.member_id, fulfillment_sales.member_id),
          raw_payload = EXCLUDED.raw_payload,
          updated_at = NOW()
        RETURNING *
        `,
        [
          saleId,
          payload.sessionId,
          payload.basketId,
          payload.buyerId,
          payload.winningBidId,
          payload.salePrice,
          basketProjection.boat_name || null,
          basketProjection.member_id || null,
          occurredAt,
          payload
        ]
      );

      await registerEvent(
        "fulfillment.sale.recorded",
        payload.basketId,
        {
          sessionId: payload.sessionId,
          basketId: payload.basketId,
          buyerId: payload.buyerId,
          winningBidId: payload.winningBidId,
          salePrice: toNumber(payload.salePrice)
        },
        topics.publishedTopics.FULFILLMENT_SALE_RECORDED,
        payload.sessionId
      );

      emit(payload.sessionId, "saleRecorded", { sessionId: payload.sessionId, basketId: payload.basketId });
      return result.rows[0];
    });
  }

  async function schedulePickup(basketId, pickupLocation, pickupTimeWindow) {
    return withTx(async (client, registerEvent) => {
      const result = await client.query(
        `
        UPDATE fulfillment_sales
        SET pickup_location = $2,
            pickup_time_window = $3,
            fulfillment_status = CASE WHEN fulfillment_status = 'PENDING' THEN 'PICKUP_SCHEDULED' ELSE fulfillment_status END,
            pickup_scheduled_at = NOW(),
            updated_at = NOW()
        WHERE basket_id = $1
        RETURNING *
        `,
        [basketId, pickupLocation || DEFAULT_PICKUP_LOCATION, pickupTimeWindow]
      );

      if (result.rowCount === 0) {
        throw new Error("Sale not found");
      }

      const sale = result.rows[0];
      await registerEvent(
        "fulfillment.pickup.scheduled",
        basketId,
        {
          sessionId: sale.session_id,
          basketId: sale.basket_id,
          buyerId: sale.buyer_id,
          pickupLocation: sale.pickup_location,
          pickupTimeWindow: sale.pickup_time_window
        },
        topics.publishedTopics.FULFILLMENT_PICKUP_SCHEDULED,
        sale.session_id
      );

      emit(sale.session_id, "pickupScheduled", { sessionId: sale.session_id, basketId });
      return sale;
    });
  }

  function checkDeliveryAvailability(address) {
    const normalized = String(address || "").toLowerCase();
    if (!normalized.trim()) {
      return { available: false, reason: "ADDRESS_MISSING" };
    }

    const available = NEARBY_KEYWORDS.some((keyword) => normalized.includes(keyword));
    return {
      available,
      reason: available ? "NEARBY_ADDRESS" : "OUT_OF_DELIVERY_AREA"
    };
  }

  async function checkDelivery(basketId, address) {
    return withTx(async (client, registerEvent) => {
      const saleResult = await client.query(
        `
        SELECT fs.*, bp.address AS buyer_address
        FROM fulfillment_sales fs
        LEFT JOIN buyers_projection bp ON bp.buyer_id = fs.buyer_id
        WHERE fs.basket_id = $1
        `,
        [basketId]
      );

      if (saleResult.rowCount === 0) {
        throw new Error("Sale not found");
      }

      const sale = saleResult.rows[0];
      const deliveryAddress = address || sale.buyer_address || "";
      const delivery = checkDeliveryAvailability(deliveryAddress);

      const updateResult = await client.query(
        `
        UPDATE fulfillment_sales
        SET delivery_address = $2,
            delivery_available = $3,
            delivery_reason = $4,
            delivery_checked_at = NOW(),
            updated_at = NOW()
        WHERE basket_id = $1
        RETURNING *
        `,
        [basketId, deliveryAddress, delivery.available, delivery.reason]
      );

      const updatedSale = updateResult.rows[0];
      await registerEvent(
        "fulfillment.delivery.checked",
        basketId,
        {
          sessionId: updatedSale.session_id,
          basketId: updatedSale.basket_id,
          buyerId: updatedSale.buyer_id,
          address: deliveryAddress,
          deliveryAvailable: delivery.available,
          reason: delivery.reason
        },
        topics.publishedTopics.FULFILLMENT_DELIVERY_CHECKED,
        updatedSale.session_id
      );

      emit(updatedSale.session_id, "deliveryChecked", { sessionId: updatedSale.session_id, basketId });
      return updatedSale;
    });
  }

  async function completeBasket(basketId) {
    return withTx(async (client, registerEvent) => {
      const result = await client.query(
        `
        UPDATE fulfillment_sales
        SET fulfillment_status = 'COMPLETED',
            sale_status = 'COMPLETED',
            completed_at = NOW(),
            updated_at = NOW()
        WHERE basket_id = $1
        RETURNING *
        `,
        [basketId]
      );

      if (result.rowCount === 0) {
        throw new Error("Sale not found");
      }

      const sale = result.rows[0];
      await registerEvent(
        "fulfillment.basket.completed",
        basketId,
        {
          sessionId: sale.session_id,
          basketId: sale.basket_id,
          buyerId: sale.buyer_id,
          fulfillmentStatus: "COMPLETED",
          deliveryAvailable: Boolean(sale.delivery_available)
        },
        topics.publishedTopics.FULFILLMENT_BASKET_COMPLETED,
        sale.session_id
      );

      emit(sale.session_id, "basketCompleted", { sessionId: sale.session_id, basketId });
      return sale;
    });
  }

  async function calculateCaptainPayments(sessionId) {
    return withTx(async (client, registerEvent) => {
      const salesResult = await client.query(
        `
        SELECT
          COALESCE(fs.boat_name, cbp.boat_name, 'Unknown Boat') AS boat_name,
          COALESCE(fs.member_id, cbp.member_id, mp.member_id) AS member_id,
          mp.member_name AS captain_name,
          SUM(fs.sale_price)::numeric AS gross_amount,
          jsonb_agg(fs.basket_id ORDER BY fs.basket_id) AS basket_ids
        FROM fulfillment_sales fs
        LEFT JOIN catalog_baskets_projection cbp ON cbp.basket_id = fs.basket_id
        LEFT JOIN members_projection mp ON mp.member_id = COALESCE(fs.member_id, cbp.member_id)
          OR (mp.boat_name = COALESCE(fs.boat_name, cbp.boat_name))
        WHERE fs.session_id = $1
        GROUP BY COALESCE(fs.boat_name, cbp.boat_name, 'Unknown Boat'), COALESCE(fs.member_id, cbp.member_id, mp.member_id), mp.member_name
        `,
        [sessionId]
      );

      const payments = [];
      for (const row of salesResult.rows) {
        const grossAmount = toNumber(row.gross_amount);
        const commissionAmount = Number((grossAmount * commissionRate).toFixed(2));
        const netAmount = Number((grossAmount - commissionAmount).toFixed(2));
        const paymentId = crypto.randomUUID();
        const basketIds = row.basket_ids || [];

        const upsert = await client.query(
          `
          INSERT INTO captain_payments (
            captain_payment_id, session_id, member_id, captain_name, boat_name,
            gross_amount, commission_amount, net_amount, basket_ids, status, calculated_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'CALCULATED', NOW())
          ON CONFLICT (session_id, boat_name) DO UPDATE SET
            member_id = EXCLUDED.member_id,
            captain_name = EXCLUDED.captain_name,
            gross_amount = EXCLUDED.gross_amount,
            commission_amount = EXCLUDED.commission_amount,
            net_amount = EXCLUDED.net_amount,
            basket_ids = EXCLUDED.basket_ids,
            calculated_at = NOW()
          RETURNING *
          `,
          [paymentId, sessionId, row.member_id || null, row.captain_name || null, row.boat_name, grossAmount, commissionAmount, netAmount, JSON.stringify(basketIds)]
        );

        const payment = upsert.rows[0];
        await registerEvent(
          "fulfillment.captain.payment.calculated",
          sessionId,
          {
            sessionId,
            memberId: payment.member_id || undefined,
            captainName: payment.captain_name || undefined,
            boatName: payment.boat_name,
            grossAmount: toNumber(payment.gross_amount),
            commissionAmount: toNumber(payment.commission_amount),
            netAmount: toNumber(payment.net_amount),
            basketIds
          },
          topics.publishedTopics.FULFILLMENT_CAPTAIN_PAYMENT_CALCULATED,
          sessionId
        );

        payments.push(payment);
      }

      emit(sessionId, "captainPaymentCalculated", { sessionId });
      return payments;
    });
  }

  async function closeAuction(sessionId) {
    return withTx(async (client, registerEvent) => {
      const summary = await client.query(
        `
        SELECT
          COUNT(*)::int AS total_sales,
          COALESCE(SUM(sale_price), 0)::numeric AS total_revenue
        FROM fulfillment_sales
        WHERE session_id = $1
        `,
        [sessionId]
      );
      const payments = await client.query(
        "SELECT COALESCE(SUM(net_amount), 0)::numeric AS total_captain_payments FROM captain_payments WHERE session_id = $1",
        [sessionId]
      );

      const totalSales = Number(summary.rows[0].total_sales || 0);
      const totalRevenue = toNumber(summary.rows[0].total_revenue);
      const totalCaptainPayments = toNumber(payments.rows[0].total_captain_payments);
      const closedAt = new Date().toISOString();

      await registerEvent(
        "fulfillment.auction.closed",
        sessionId,
        {
          sessionId,
          totalSales,
          totalRevenue,
          totalCaptainPayments,
          closedAt
        },
        topics.publishedTopics.FULFILLMENT_AUCTION_CLOSED,
        sessionId
      );

      emit(sessionId, "auctionClosed", { sessionId });
      return { sessionId, totalSales, totalRevenue, totalCaptainPayments, closedAt };
    });
  }

  async function handleAllBasketsFinalized(payload) {
    return withTx(async (client) => {
      await markProcessed(client, payload.eventId, topics.consumedTopics.BID_ALL_BASKETS_FINALIZED);
      return payload;
    });
  }

  async function getFulfillmentData() {
    const [sales, payments] = await Promise.all([
      db.query(
        `
        SELECT fs.*, bp.name AS buyer_name, bp.address AS buyer_address, cbp.species, cbp.quantity, cbp.unit, cbp.quality
        FROM fulfillment_sales fs
        LEFT JOIN buyers_projection bp ON bp.buyer_id = fs.buyer_id
        LEFT JOIN catalog_baskets_projection cbp ON cbp.basket_id = fs.basket_id
        ORDER BY fs.recorded_at DESC
        `
      ),
      db.query("SELECT * FROM captain_payments ORDER BY calculated_at DESC")
    ]);

    return {
      sales: sales.rows,
      captainPayments: payments.rows
    };
  }

  async function getFulfillmentSnapshot() {
    const result = await db.query(
      `
      SELECT COUNT(*)::int AS count, COALESCE(MAX(updated_at), '1970-01-01'::timestamptz) AS updated_at
      FROM fulfillment_sales
      `
    );
    const row = result.rows[0];
    return { token: `${row.count}:${new Date(row.updated_at).toISOString()}` };
  }

  return {
    handleBuyerRegistered,
    handleMemberRegistered,
    handleBasketCreated,
    handleBasketSold,
    handleAllBasketsFinalized,
    schedulePickup,
    checkDelivery,
    checkDeliveryAvailability,
    completeBasket,
    calculateCaptainPayments,
    closeAuction,
    getFulfillmentData,
    getFulfillmentSnapshot
  };
}

module.exports = createFulfillmentService();
module.exports.createFulfillmentService = createFulfillmentService;
