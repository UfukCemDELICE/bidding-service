const pool = require("../db/pool");
const producer = require("../kafka/producer");
const topics = require("../kafka/topics");
const { getIo } = require("../sockets/socket");

function createEvent(eventType, aggregateType, aggregateId, payload, topic) {
  return {
    eventType,
    aggregateType,
    aggregateId,
    payload,
    topic
  };
}

async function insertEvents(client, events) {
  for (const event of events) {
    await client.query(
      `
        INSERT INTO event_store (aggregate_type, aggregate_id, event_type, payload)
        VALUES ($1, $2, $3, $4)
      `,
      [event.aggregateType, event.aggregateId, event.eventType, event.payload]
    );
  }
}

async function publishEvents(events) {
  for (const event of events) {
    if (event.topic) {
      await producer.publish(event.topic, event.payload);
    }
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
      GROUP BY s.session_id
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
      SELECT session_id, title, status
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
      SELECT basket_id, basket_no, description, starting_price, status, highest_bid, opened_at, closed_at
      FROM auction_baskets
      WHERE session_id = $1
      ORDER BY basket_no NULLS LAST, created_at
    `,
    [sessionId]
  );

  const currentBasket =
    basketsResult.rows.find((basket) => basket.status === "OPEN") ||
    basketsResult.rows.find((basket) => basket.status === "PENDING") ||
    basketsResult.rows.find((basket) => basket.status === "UNSOLD");

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

  const salesResult = await pool.query(
    `
      SELECT basket_id, winner_id, winning_bid_amount, payment_confirmed, sold_at
      FROM sale_records
      WHERE session_id = $1
      ORDER BY sold_at DESC
    `,
    [sessionId]
  );

  return {
    session: sessionResult.rows[0],
    baskets: basketsResult.rows,
    currentBasket,
    bids: bidsResult.rows,
    rebidQueue: rebidQueueResult.rows,
    sales: salesResult.rows
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

async function openBasket(sessionId, basketId) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const basketResult = await client.query(
      `
        SELECT basket_id, basket_no, description, status
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

    if (!["PENDING", "UNSOLD"].includes(basket.status)) {
      throw new Error("Only pending or unsold baskets can be opened.");
    }

    const openBasketResult = await client.query(
      `
        SELECT basket_id
        FROM auction_baskets
        WHERE session_id = $1 AND status = 'OPEN' AND basket_id <> $2
        LIMIT 1
      `,
      [sessionId, basketId]
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

    const rebidRow = await client.query(
      `
        UPDATE rebid_queue
        SET status = 'OPENED'
        WHERE basket_id = $1 AND status = 'PENDING'
        RETURNING id
      `,
      [basketId]
    );

    const events = [
      createEvent(
        "BasketOpened",
        "auction_basket",
        basketId,
        {
          sessionId,
          basketId,
          basketNo: basket.basket_no,
          description: basket.description
        },
        topics.publishedTopics.BASKET_DETAILS_ANNOUNCED
      )
    ];

    if (rebidRow.rows.length) {
      events.push(
        createEvent(
          "RebidRoundOpened",
          "rebid_queue",
          String(rebidRow.rows[0].id),
          {
            sessionId,
            basketId
          },
          topics.publishedTopics.REBID_ROUND_OPENED
        )
      );
    }

    await insertEvents(client, events);
    await client.query("COMMIT");

    await publishEvents(events);
    getIo().to(sessionId).emit("basketOpened", {
      sessionId,
      basketId
    });
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function placeBid(sessionId, basketId, bidData) {
  const amount = Number(bidData.amount);
  const bidderId = String(bidData.bidderId || "").trim();
  const bidderName = String(bidData.bidderName || "").trim();

  if (!bidderId || !bidderName || Number.isNaN(amount)) {
    throw new Error("Bidder id, bidder name, and amount are required.");
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const basketResult = await client.query(
      `
        SELECT basket_id, basket_no, status, starting_price, COALESCE(highest_bid, 0) AS highest_bid
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

    const currentHighest = Number(basket.highest_bid || basket.starting_price || 0);

    if (amount <= currentHighest) {
      throw new Error("Bid amount must be greater than the current highest bid.");
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
      basketNo: basket.basket_no,
      bidId: bidInsert.rows[0].id,
      bidderId,
      bidderName,
      amount,
      placedAt: bidInsert.rows[0].placed_at
    };

    const events = [
      createEvent("BidPlaced", "auction_basket", basketId, payload, topics.publishedTopics.BID_PLACED)
    ];

    await insertEvents(client, events);
    await client.query("COMMIT");

    await publishEvents(events);
    getIo().to(sessionId).emit("bidPlaced", payload);

    return payload;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function closeBasket(sessionId, basketId) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const basketResult = await client.query(
      `
        SELECT basket_id, basket_no, description, status
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
        SELECT id, bidder_id, bidder_name, amount, placed_at
        FROM bids
        WHERE basket_id = $1
        ORDER BY amount DESC, placed_at ASC
        LIMIT 1
      `,
      [basketId]
    );

    const events = [
      createEvent(
        "BidWindowClosed",
        "auction_basket",
        basketId,
        { sessionId, basketId },
        topics.publishedTopics.WINDOW_CLOSED
      )
    ];

    await client.query(
      `
        UPDATE auction_baskets
        SET closed_at = NOW(), updated_at = NOW()
        WHERE basket_id = $1
      `,
      [basketId]
    );

    let outcome;

    if (highestBidResult.rows.length) {
      const winningBid = highestBidResult.rows[0];

      await client.query(
        `
          UPDATE auction_baskets
          SET status = 'SOLD', highest_bid = $2, updated_at = NOW()
          WHERE basket_id = $1
        `,
        [basketId, winningBid.amount]
      );

      await client.query(
        `
          INSERT INTO sale_records
            (session_id, basket_id, winning_bid_id, winner_id, winning_bid_amount)
          VALUES ($1, $2, $3, $4, $5)
          ON CONFLICT (basket_id)
          DO UPDATE SET
            winning_bid_id = EXCLUDED.winning_bid_id,
            winner_id = EXCLUDED.winner_id,
            winning_bid_amount = EXCLUDED.winning_bid_amount
        `,
        [sessionId, basketId, winningBid.id, winningBid.bidder_id, winningBid.amount]
      );

      outcome = {
        sessionId,
        basketId,
        basketNo: basket.basket_no,
        winningBidId: winningBid.id,
        winnerId: winningBid.bidder_id,
        winnerName: winningBid.bidder_name,
        winningAmount: Number(winningBid.amount)
      };

      events.push(
        createEvent("BasketSold", "auction_basket", basketId, outcome, topics.publishedTopics.BASKET_SOLD),
        createEvent(
          "WinningBidRecorded",
          "sale_record",
          basketId,
          outcome,
          topics.publishedTopics.WINNING_BID_RECORDED
        )
      );
    } else {
      await client.query(
        `
          UPDATE auction_baskets
          SET status = 'UNSOLD', updated_at = NOW()
          WHERE basket_id = $1
        `,
        [basketId]
      );

      await client.query(
        `
          INSERT INTO rebid_queue (session_id, basket_id, reason, status)
          VALUES ($1, $2, $3, 'PENDING')
          ON CONFLICT (basket_id)
          DO UPDATE SET
            reason = EXCLUDED.reason,
            status = 'PENDING',
            queued_at = NOW()
        `,
        [sessionId, basketId, "No bids received in active window"]
      );

      outcome = {
        sessionId,
        basketId,
        basketNo: basket.basket_no,
        reason: "No bids received in active window"
      };

      events.push(
        createEvent("BasketUnsold", "auction_basket", basketId, outcome, topics.publishedTopics.BASKET_UNSOLD),
        createEvent(
          "BasketQueuedForRebid",
          "rebid_queue",
          basketId,
          outcome,
          topics.publishedTopics.BASKET_QUEUED_FOR_REBID
        )
      );
    }

    const nextBasket = await findNextBasket(client, sessionId);

    if (nextBasket && nextBasket.basket_id !== basketId) {
      events.push(
        createEvent(
          "NextBasketReady",
          "auction_session",
          sessionId,
          {
            sessionId,
            basketId: nextBasket.basket_id,
            basketNo: nextBasket.basket_no,
            description: nextBasket.description,
            status: nextBasket.status
          },
          topics.publishedTopics.NEXT_BASKET_READY
        )
      );
    }

    await insertEvents(client, events);
    await client.query("COMMIT");

    await publishEvents(events);
    getIo().to(sessionId).emit("basketClosed", {
      sessionId,
      basketId,
      outcome
    });
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function hasFinalizedAllBaskets(client, sessionId) {
  const result = await client.query(
    `
      SELECT COUNT(*)::int AS remaining
      FROM auction_baskets b
      LEFT JOIN sale_records s ON s.basket_id = b.basket_id
      WHERE b.session_id = $1
        AND (
          b.status IN ('PENDING', 'OPEN')
          OR (b.status = 'SOLD' AND COALESCE(s.payment_confirmed, FALSE) = FALSE)
        )
    `,
    [sessionId]
  );

  return result.rows[0].remaining === 0;
}

async function findNextBasket(client, sessionId) {
  const result = await client.query(
    `
      SELECT basket_id, basket_no, description, status
      FROM auction_baskets
      WHERE session_id = $1 AND status IN ('PENDING', 'UNSOLD')
      ORDER BY
        CASE WHEN status = 'PENDING' THEN 0 ELSE 1 END,
        basket_no NULLS LAST,
        created_at
      LIMIT 1
    `,
    [sessionId]
  );

  return result.rows[0] || null;
}

async function confirmPayment(sessionId, basketId) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const saleResult = await client.query(
      `
        SELECT id, basket_id, winner_id, winning_bid_amount, payment_confirmed
        FROM sale_records
        WHERE session_id = $1 AND basket_id = $2
        FOR UPDATE
      `,
      [sessionId, basketId]
    );

    if (!saleResult.rows.length) {
      throw new Error("Sale record not found for payment confirmation.");
    }

    const sale = saleResult.rows[0];

    if (sale.payment_confirmed) {
      throw new Error("Payment already confirmed.");
    }

    await client.query(
      `
        UPDATE sale_records
        SET payment_confirmed = TRUE
        WHERE id = $1
      `,
      [sale.id]
    );

    const paymentPayload = {
      sessionId,
      basketId,
      winnerId: sale.winner_id,
      amount: Number(sale.winning_bid_amount)
    };

    const events = [
      createEvent(
        "PaymentConfirmed",
        "sale_record",
        String(sale.id),
        paymentPayload,
        topics.publishedTopics.PAYMENT_CONFIRMED
      ),
      createEvent(
        "BasketSaleCompleted",
        "auction_basket",
        basketId,
        paymentPayload,
        topics.publishedTopics.BASKET_SALE_COMPLETED
      )
    ];

    const nextBasket = await findNextBasket(client, sessionId);

    if (nextBasket) {
      events.push(
        createEvent(
          "NextBasketReady",
          "auction_session",
          sessionId,
          {
            sessionId,
            basketId: nextBasket.basket_id,
            basketNo: nextBasket.basket_no,
            description: nextBasket.description,
            status: nextBasket.status
          },
          topics.publishedTopics.NEXT_BASKET_READY
        )
      );
    }

    const finalized = await hasFinalizedAllBaskets(client, sessionId);

    if (finalized) {
      await client.query(
        `
          UPDATE auction_sessions
          SET status = 'FINALIZED', updated_at = NOW()
          WHERE session_id = $1
        `,
        [sessionId]
      );

      events.push(
        createEvent(
          "AllBasketsFinalized",
          "auction_session",
          sessionId,
          { sessionId },
          topics.publishedTopics.ALL_BASKETS_FINALIZED
        )
      );
    }

    await insertEvents(client, events);
    await client.query("COMMIT");

    await publishEvents(events);
    getIo().to(sessionId).emit("paymentConfirmed", paymentPayload);

    if (finalized) {
      getIo().to(sessionId).emit("auctionFinalized", { sessionId });
    }
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

module.exports = {
  getHomeData,
  getHomeSnapshot,
  getAuctionDashboard,
  getAuctionSnapshot,
  openBasket,
  placeBid,
  closeBasket,
  confirmPayment
};
