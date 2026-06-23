const crypto = require("crypto");
const pool = require("../db/pool");
const producer = require("../kafka/producer");
const topics = require("../kafka/topics");
const { getIo } = require("../sockets/socket");

async function withTx(action) {
  const client = await pool.connect();
  let committed = false;
  const eventsToPublish = [];
  try {
    await client.query("BEGIN");

    const registerEvent = async (eventType, aggregateType, aggregateId, payload, topic, key) => {
      const eventId = crypto.randomUUID();
      const occurredAt = new Date().toISOString();
      const eventPayload = {
        ...payload,
        eventId,
        occurredAt
      };

      const res = await client.query(
        `
          INSERT INTO event_store (event_id, aggregate_type, aggregate_id, event_type, payload, topic, key, occurred_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
          RETURNING id
        `,
        [eventId, aggregateType, aggregateId, eventType, eventPayload, topic, key, occurredAt]
      );

      eventsToPublish.push({
        id: res.rows[0].id,
        topic,
        key,
        payload: eventPayload
      });
    };

    const result = await action(client, registerEvent);

    await client.query("COMMIT");
    committed = true;

    // Publish to Kafka post-commit
    for (const ev of eventsToPublish) {
      try {
        await producer.publish(ev.topic, ev.payload, ev.key);
        await pool.query(
          `UPDATE event_store SET published_at = NOW(), publish_error = NULL WHERE id = $1`,
          [ev.id]
        );
      } catch (err) {
        console.error(`Failed to publish event ${ev.id} to topic ${ev.topic}:`, err.message);
        await pool.query(
          `UPDATE event_store SET publish_error = $1 WHERE id = $2`,
          [err.message, ev.id]
        );
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

async function flushUnpublishedOutbox() {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    // Batch of 100 with SKIP LOCKED for safe concurrent flushing
    const result = await client.query(
      `SELECT id, topic, key, payload FROM event_store
       WHERE published_at IS NULL
       ORDER BY created_at ASC
       LIMIT 100
       FOR UPDATE SKIP LOCKED`
    );

    if (result.rows.length === 0) {
      await client.query("COMMIT");
      return;
    }

    console.log(`Flushing ${result.rows.length} unpublished events from outbox...`);

    for (const ev of result.rows) {
      try {
        await producer.publish(ev.topic, ev.payload, ev.key);
        await client.query(
          `UPDATE event_store SET published_at = NOW(), publish_error = NULL WHERE id = $1`,
          [ev.id]
        );
      } catch (err) {
        console.error(`Failed to publish outbox event ${ev.id}:`, err.message);
        await client.query(
          `UPDATE event_store SET publish_error = $1 WHERE id = $2`,
          [err.message, ev.id]
        );
      }
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Error during flushUnpublishedOutbox:", err.message);
  } finally {
    client.release();
  }
}

async function getHomeData() {
  const result = await pool.query(
    `
      SELECT
        s.session_id,
        s.title,
        s.status,
        COUNT(b.basket_id) AS basket_count
      FROM auction_sessions s
      LEFT JOIN auction_baskets b ON b.session_id = s.session_id
      GROUP BY s.session_id, s.title, s.status, s.created_at
      ORDER BY s.created_at DESC
    `
  );
  return result.rows;
}

async function getHomeSnapshot() {
  const result = await pool.query(
    `
      SELECT
        COUNT(*)::int AS session_count,
        COALESCE(MAX(updated_at), TO_TIMESTAMP(0)) AS last_updated
      FROM auction_sessions
    `
  );
  const row = result.rows[0];
  return {
    token: `${row.session_count}:${new Date(row.last_updated).toISOString()}`
  };
}

async function getAuctionDashboard(sessionId) {
  const sessionResult = await pool.query(
    `
      SELECT session_id, title, status, rebid_round
      FROM auction_sessions
      WHERE session_id = $1
    `,
    [sessionId]
  );

  if (!sessionResult.rows.length) {
    throw new Error("Auction session not found.");
  }

  const basketsResult = await pool.query(
    `
      SELECT basket_id, basket_no, description, base_price, details_ready, status, highest_bid, opened_at, closed_at
      FROM auction_baskets
      WHERE session_id = $1
      ORDER BY basket_no NULLS LAST, created_at
    `,
    [sessionId]
  );

  const currentBasket =
    basketsResult.rows.find((basket) => basket.status === "OPEN") ||
    basketsResult.rows.find((basket) => basket.status === "REBID_READY") ||
    basketsResult.rows.find((basket) => basket.status === "LISTED") ||
    basketsResult.rows.find((basket) => basket.status === "REBID_QUEUED") ||
    basketsResult.rows.find((basket) => basket.status === "FINAL_UNSOLD");

  const bidsResult = currentBasket
    ? await pool.query(
        `
          SELECT id, bidder_id, bidder_name, amount, placed_at
          FROM bids
          WHERE basket_id = $1
          ORDER BY amount DESC, placed_at ASC
        `,
        [currentBasket.basket_id]
      )
    : { rows: [] };

  const rebidQueueResult = await pool.query(
    `
      SELECT rq.basket_id, rq.reason, rq.status, rq.queued_at, b.basket_no, b.description
      FROM rebid_queue rq
      JOIN auction_baskets b ON b.basket_id = rq.basket_id
      WHERE rq.session_id = $1
      ORDER BY rq.queued_at ASC
    `,
    [sessionId]
  );

  const buyersResult = await pool.query(
    `SELECT buyer_id, name FROM buyers_projection ORDER BY name ASC`
  );

  return {
    session: sessionResult.rows[0],
    baskets: basketsResult.rows,
    currentBasket,
    bids: bidsResult.rows,
    rebidQueue: rebidQueueResult.rows,
    buyers: buyersResult.rows,
    sales: []
  };
}

async function getAuctionSnapshot(sessionId) {
  const result = await pool.query(
    `
      SELECT
        s.updated_at AS session_updated_at,
        COALESCE(MAX(b.updated_at), TO_TIMESTAMP(0)) AS basket_updated_at,
        COUNT(b.basket_id)::int AS basket_count
      FROM auction_sessions s
      LEFT JOIN auction_baskets b ON b.session_id = s.session_id
      WHERE s.session_id = $1
      GROUP BY s.updated_at
    `,
    [sessionId]
  );

  if (!result.rows.length) {
    throw new Error("Auction session not found.");
  }

  const row = result.rows[0];

  return {
    token: [
      sessionId,
      row.basket_count,
      new Date(row.session_updated_at).toISOString(),
      new Date(row.basket_updated_at).toISOString()
    ].join(":")
  };
}

async function startAuction(sessionId) {
  await withTx(async (client, registerEvent) => {
    const sessionRes = await client.query(
      `SELECT session_id, status FROM auction_sessions WHERE session_id = $1 FOR UPDATE`,
      [sessionId]
    );

    if (!sessionRes.rows.length) {
      throw new Error("Auction session not found.");
    }

    const session = sessionRes.rows[0];
    if (session.status !== "READY") {
      throw new Error(`Only READY sessions can be started. Current status: ${session.status}`);
    }

    const basketsCountRes = await client.query(
      `SELECT COUNT(*)::int AS count FROM auction_baskets WHERE session_id = $1`,
      [sessionId]
    );
    const totalBaskets = basketsCountRes.rows[0].count;
    const startTime = new Date().toISOString();

    await client.query(
      `UPDATE auction_sessions SET status = 'LIVE', start_time = $2, updated_at = NOW() WHERE session_id = $1`,
      [sessionId, startTime]
    );

    const payload = {
      sessionId,
      startTime,
      totalBaskets
    };

    await registerEvent(
      "auction.session.started",
      "auction_session",
      sessionId,
      payload,
      topics.publishedTopics.AUCTION_SESSION_STARTED,
      sessionId
    );
  });

  getIo().emit("homeUpdated", { sessionId, eventType: "auctionStarted" });
  getIo().to(sessionId).emit("sessionProjectionUpdated", { sessionId, eventType: "auctionStarted" });
}

async function openBasket(sessionId, basketId) {
  await withTx(async (client, registerEvent) => {
    const basketResult = await client.query(
      `
        SELECT basket_id, base_price, details_ready, status
        FROM auction_baskets
        WHERE session_id = $1 AND basket_id = $2
        FOR UPDATE
      `,
      [sessionId, basketId]
    );

    if (!basketResult.rows.length) {
      throw new Error("Basket not found.");
    }

    const basket = basketResult.rows[0];

    if (!["LISTED", "REBID_READY"].includes(basket.status)) {
      throw new Error("Only LISTED or REBID_READY baskets can be opened.");
    }

    if (!basket.details_ready || basket.base_price === null) {
      throw new Error("Basket details are not ready yet (missing base price).");
    }

    // Check if another basket is currently open
    const openBasketResult = await client.query(
      `
        SELECT basket_id
        FROM auction_baskets
        WHERE session_id = $1 AND status = 'OPEN'
        LIMIT 1
      `,
      [sessionId]
    );

    if (openBasketResult.rows.length) {
      throw new Error("Another basket is already open in this session.");
    }

    await client.query(
      `
        UPDATE auction_baskets
        SET status = 'OPEN', opened_at = NOW(), closed_at = NULL, updated_at = NOW()
        WHERE basket_id = $1
      `,
      [basketId]
    );

    const payload = {
      sessionId,
      basketId,
      basePrice: Number(basket.base_price)
    };

    await registerEvent(
      "auction.basket.opened",
      "auction_basket",
      basketId,
      payload,
      topics.publishedTopics.AUCTION_BASKET_OPENED,
      sessionId
    );
  });

  getIo().to(sessionId).emit("basketOpened", { sessionId, basketId });
}

async function placeBid(sessionId, basketId, bidData) {
  const amount = Number(bidData.amount);
  const bidderId = String(bidData.bidderId || "").trim();

  if (!bidderId || Number.isNaN(amount)) {
    throw new Error("Bidder id and amount are required.");
  }

  const resultPayload = await withTx(async (client, registerEvent) => {
    // Validate session is LIVE
    const sessionRes = await client.query(
      `SELECT status FROM auction_sessions WHERE session_id = $1`,
      [sessionId]
    );
    if (!sessionRes.rows.length || sessionRes.rows[0].status !== "LIVE") {
      throw new Error("Bidding is only allowed when the auction session is LIVE.");
    }

    // Validate basket is OPEN
    const basketResult = await client.query(
      `
        SELECT basket_id, base_price, status, COALESCE(highest_bid, 0) AS highest_bid
        FROM auction_baskets
        WHERE session_id = $1 AND basket_id = $2
        FOR UPDATE
      `,
      [sessionId, basketId]
    );

    if (!basketResult.rows.length) {
      throw new Error("Basket not found.");
    }

    const basket = basketResult.rows[0];

    if (basket.status !== "OPEN") {
      throw new Error("Basket is not open for bidding.");
    }

    // Validate buyer exists
    const buyerRes = await client.query(
      `SELECT name FROM buyers_projection WHERE buyer_id = $1`,
      [bidderId]
    );
    if (!buyerRes.rows.length) {
      throw new Error("Bidder does not exist in buyers_projection.");
    }
    const bidderName = buyerRes.rows[0].name;

    // Validate bid amount
    const basePrice = Number(basket.base_price);
    const currentHighest = Number(basket.highest_bid);

    if (amount < basePrice) {
      throw new Error(`Bid amount must be greater than or equal to the base price of ${basePrice}.`);
    }

    if (amount <= currentHighest) {
      throw new Error(`Bid amount must be greater than the current highest bid of ${currentHighest}.`);
    }

    const bidInsert = await client.query(
      `
        INSERT INTO bids (session_id, basket_id, bidder_id, bidder_name, amount)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, placed_at
      `,
      [sessionId, basketId, bidderId, bidderName, amount]
    );

    await client.query(
      `
        UPDATE auction_baskets
        SET highest_bid = $2, updated_at = NOW()
        WHERE basket_id = $1
      `,
      [basketId, amount]
    );

    const payload = {
      sessionId,
      basketId,
      buyerId: bidderId,
      bidId: String(bidInsert.rows[0].id),
      bidAmount: amount
    };

    await registerEvent(
      "bid.placed",
      "auction_basket",
      basketId,
      payload,
      topics.publishedTopics.BID_PLACED,
      sessionId
    );

    return payload;
  });

  getIo().to(sessionId).emit("bidPlaced", resultPayload);
  return resultPayload;
}

async function closeBasket(sessionId, basketId) {
  let isSessionFinalized = false;

  await withTx(async (client, registerEvent) => {
    const basketResult = await client.query(
      `
        SELECT basket_id, status
        FROM auction_baskets
        WHERE session_id = $1 AND basket_id = $2
        FOR UPDATE
      `,
      [sessionId, basketId]
    );

    if (!basketResult.rows.length) {
      throw new Error("Basket not found.");
    }

    const basket = basketResult.rows[0];

    if (basket.status !== "OPEN") {
      throw new Error("Only open baskets can be closed.");
    }

    const highestBidResult = await client.query(
      `
        SELECT id, bidder_id, amount
        FROM bids
        WHERE basket_id = $1
        ORDER BY amount DESC, placed_at ASC
        LIMIT 1
      `,
      [basketId]
    );

    if (highestBidResult.rows.length) {
      const winningBid = highestBidResult.rows[0];

      await client.query(
        `
          UPDATE auction_baskets
          SET status = 'SOLD', closed_at = NOW(), updated_at = NOW()
          WHERE basket_id = $1
        `,
        [basketId]
      );

      const payload = {
        sessionId,
        basketId,
        buyerId: winningBid.bidder_id,
        winningBidId: String(winningBid.id),
        salePrice: Number(winningBid.amount)
      };

      await registerEvent(
        "bid.basket.sold",
        "auction_basket",
        basketId,
        payload,
        topics.publishedTopics.BID_BASKET_SOLD,
        sessionId
      );
    } else {
      // Check if this basket has already been in a rebid round (OPENED in rebid_queue)
      const rebidCheck = await client.query(
        `SELECT id FROM rebid_queue WHERE basket_id = $1 AND status = 'OPENED'`,
        [basketId]
      );

      if (rebidCheck.rows.length) {
        // Mark FINAL_UNSOLD
        await client.query(
          `
            UPDATE auction_baskets
            SET status = 'FINAL_UNSOLD', closed_at = NOW(), updated_at = NOW()
            WHERE basket_id = $1
          `,
          [basketId]
        );

        await client.query(
          `
            UPDATE rebid_queue
            SET status = 'FINAL_UNSOLD'
            WHERE basket_id = $1
          `,
          [basketId]
        );
      } else {
        // Mark REBID_QUEUED
        await client.query(
          `
            UPDATE auction_baskets
            SET status = 'REBID_QUEUED', closed_at = NOW(), updated_at = NOW()
            WHERE basket_id = $1
          `,
          [basketId]
        );

        await client.query(
          `
            INSERT INTO rebid_queue (session_id, basket_id, reason, status)
            VALUES ($1, $2, 'No bids received in active window', 'PENDING')
            ON CONFLICT (basket_id)
            DO UPDATE SET status = 'PENDING', queued_at = NOW()
          `,
          [sessionId, basketId]
        );
      }

      const payload = {
        sessionId,
        basketId,
        reason: "NO_BIDS"
      };

      await registerEvent(
        "bid.basket.unsold",
        "auction_basket",
        basketId,
        payload,
        topics.publishedTopics.BID_BASKET_UNSOLD,
        sessionId
      );
    }

    // Check if every basket in the session is SOLD or FINAL_UNSOLD
    const remainingResult = await client.query(
      `
        SELECT COUNT(*)::int AS count
        FROM auction_baskets
        WHERE session_id = $1 AND status NOT IN ('SOLD', 'FINAL_UNSOLD')
      `,
      [sessionId]
    );

    if (remainingResult.rows[0].count === 0) {
      isSessionFinalized = true;

      await client.query(
        `
          UPDATE auction_sessions
          SET status = 'BIDDING_COMPLETED', updated_at = NOW()
          WHERE session_id = $1
        `,
        [sessionId]
      );

      const countsRes = await client.query(
        `
          SELECT
            COUNT(CASE WHEN status = 'SOLD' THEN 1 END)::int AS sold_count,
            COUNT(CASE WHEN status = 'FINAL_UNSOLD' THEN 1 END)::int AS unsold_count
          FROM auction_baskets
          WHERE session_id = $1
        `,
        [sessionId]
      );

      const soldBasketCount = countsRes.rows[0].sold_count;
      const unsoldBasketCount = countsRes.rows[0].unsold_count;
      const totalBaskets = soldBasketCount + unsoldBasketCount;

      const finalizedPayload = {
        sessionId,
        totalBaskets,
        soldBasketCount,
        unsoldBasketCount
      };

      await registerEvent(
        "bid.all.baskets.finalized",
        "auction_session",
        sessionId,
        finalizedPayload,
        topics.publishedTopics.BID_ALL_BASKETS_FINALIZED,
        sessionId
      );
    }
  });

  getIo().to(sessionId).emit("basketClosed", { sessionId, basketId });
  if (isSessionFinalized) {
    getIo().to(sessionId).emit("auctionFinalized", { sessionId });
  }
}

async function openRebidRound(sessionId) {
  await withTx(async (client, registerEvent) => {
    const sessionRes = await client.query(
      `SELECT rebid_round FROM auction_sessions WHERE session_id = $1 FOR UPDATE`,
      [sessionId]
    );

    if (!sessionRes.rows.length) {
      throw new Error("Session not found.");
    }

    const session = sessionRes.rows[0];

    const pendingBaskets = await client.query(
      `SELECT basket_id FROM rebid_queue WHERE session_id = $1 AND status = 'PENDING'`,
      [sessionId]
    );

    if (pendingBaskets.rows.length === 0) {
      throw new Error("No pending unsold baskets in the rebid queue.");
    }

    const basketIds = pendingBaskets.rows.map((r) => r.basket_id);
    const newRoundNumber = session.rebid_round + 1;

    // Increment rebid round count and put session status to LIVE
    await client.query(
      `UPDATE auction_sessions SET status = 'LIVE', rebid_round = $2, updated_at = NOW() WHERE session_id = $1`,
      [sessionId, newRoundNumber]
    );

    // Update rebid queue
    await client.query(
      `UPDATE rebid_queue SET status = 'OPENED' WHERE session_id = $1 AND status = 'PENDING'`,
      [sessionId]
    );

    // Update baskets to REBID_READY
    await client.query(
      `UPDATE auction_baskets SET status = 'REBID_READY', updated_at = NOW() WHERE session_id = $1 AND basket_id = ANY($2)`,
      [sessionId, basketIds]
    );

    const payload = {
      sessionId,
      roundNumber: newRoundNumber,
      basketIds
    };

    await registerEvent(
      "bid.rebid.round.opened",
      "auction_session",
      sessionId,
      payload,
      topics.publishedTopics.BID_REBID_ROUND_OPENED,
      sessionId
    );
  });

  getIo().to(sessionId).emit("sessionProjectionUpdated", { sessionId, eventType: "rebidRoundOpened" });
}

async function handleBuyerRegistered(payload) {
  const buyerId = payload.buyerId || payload.id;
  const name = payload.name || payload.username || payload.displayName || "Unknown Buyer";
  const email = payload.email || null;
  const phone = payload.phone || null;
  const address = payload.address || null;
  const occurredAt = payload.occurredAt || new Date().toISOString();

  await pool.query(
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
      WHERE buyers_projection.occurred_at IS NULL
         OR EXCLUDED.occurred_at >= buyers_projection.occurred_at
    `,
    [buyerId, name, email, phone, address, occurredAt, payload]
  );
  console.log(`Idempotently registered/updated buyer ${buyerId}`);
}

async function handleBasketCreated(payload) {
  const basketId = payload.basketId || payload.id;
  const species = payload.species || payload.description || payload.name || "Unknown Species";
  const quantity = payload.quantity !== undefined ? Number(payload.quantity) : null;
  const unit = payload.unit || null;
  const quality = payload.quality || null;
  const basePrice = payload.basePrice !== undefined ? Number(payload.basePrice) : null;
  const boatName = payload.boatName || null;
  const occurredAt = payload.occurredAt || new Date().toISOString();

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const result = await client.query(
      `
        WITH upserted AS (
          INSERT INTO catalog_baskets_projection (basket_id, species, quantity, unit, quality, base_price, boat_name, occurred_at, raw_payload)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
          ON CONFLICT (basket_id) DO UPDATE SET
            species = EXCLUDED.species,
            quantity = EXCLUDED.quantity,
            unit = EXCLUDED.unit,
            quality = EXCLUDED.quality,
            base_price = EXCLUDED.base_price,
            boat_name = EXCLUDED.boat_name,
            occurred_at = EXCLUDED.occurred_at,
            raw_payload = EXCLUDED.raw_payload
          WHERE catalog_baskets_projection.occurred_at IS NULL
             OR EXCLUDED.occurred_at >= catalog_baskets_projection.occurred_at
          RETURNING basket_id
        )
        SELECT * FROM upserted;
      `,
      [basketId, species, quantity, unit, quality, basePrice, boatName, occurredAt, payload]
    );

    if (result.rows.length > 0) {
      const detailsReady = basePrice !== null;
      await client.query(
        `
          UPDATE auction_baskets
          SET
            description = COALESCE(description, $2),
            base_price = COALESCE(base_price, $3),
            details_ready = details_ready OR $4,
            updated_at = NOW()
          WHERE basket_id = $1
        `,
        [basketId, species, basePrice, detailsReady]
      );
    }

    await client.query("COMMIT");
    console.log(`Idempotently stored basket details and enriched for basket ${basketId}`);
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function handleCatalogPublished(payload) {
  const sessionId = payload.sessionId || payload.id;
  const title = payload.title || payload.name || `Auction Session ${sessionId}`;
  const startTime = payload.startTime || new Date().toISOString();
  const basketIds = Array.isArray(payload.basketIds) ? payload.basketIds : [];

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    await client.query(
      `
        INSERT INTO auction_sessions (session_id, title, status, start_time, updated_at)
        VALUES ($1, $2, 'READY', $3, NOW())
        ON CONFLICT (session_id) DO UPDATE SET
          title = EXCLUDED.title,
          status = CASE
            WHEN auction_sessions.status IN ('LIVE', 'BIDDING_COMPLETED') THEN auction_sessions.status
            ELSE 'READY'
          END,
          start_time = COALESCE(auction_sessions.start_time, EXCLUDED.start_time),
          updated_at = NOW()
      `,
      [sessionId, title, startTime]
    );

    for (let i = 0; i < basketIds.length; i++) {
      const basketId = basketIds[i];
      const basketNo = i + 1;

      const basketProj = await client.query(
        `SELECT species, base_price FROM catalog_baskets_projection WHERE basket_id = $1`,
        [basketId]
      );

      let description = null;
      let basePrice = null;
      let detailsReady = false;

      if (basketProj.rows.length) {
        const row = basketProj.rows[0];
        description = row.species;
        basePrice = row.base_price !== null ? Number(row.base_price) : null;
        detailsReady = basePrice !== null;
      }

      await client.query(
        `
          INSERT INTO auction_baskets (basket_id, session_id, basket_no, description, base_price, details_ready, status, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, 'LISTED', NOW())
          ON CONFLICT (basket_id) DO UPDATE SET
            session_id = EXCLUDED.session_id,
            basket_no = EXCLUDED.basket_no,
            description = COALESCE(auction_baskets.description, EXCLUDED.description),
            base_price = COALESCE(auction_baskets.base_price, EXCLUDED.base_price),
            details_ready = auction_baskets.details_ready OR EXCLUDED.details_ready,
            updated_at = NOW()
        `,
        [basketId, sessionId, basketNo, description, basePrice, detailsReady]
      );
    }

    await client.query("COMMIT");
    console.log(`Idempotently handled catalog published for session ${sessionId}`);

    getIo().emit("homeUpdated", {
      sessionId,
      eventType: "catalogPublished"
    });

    getIo().to(sessionId).emit("sessionProjectionUpdated", {
      sessionId,
      eventType: "catalogPublished"
    });
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

module.exports = {
  flushUnpublishedOutbox,
  getHomeData,
  getHomeSnapshot,
  getAuctionDashboard,
  getAuctionSnapshot,
  startAuction,
  openBasket,
  placeBid,
  closeBasket,
  openRebidRound,
  handleBuyerRegistered,
  handleBasketCreated,
  handleCatalogPublished
};
