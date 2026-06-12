const { Kafka } = require("kafkajs");
const dotenv = require("dotenv");

const pool = require("../db/pool");
const { getIo } = require("../sockets/socket");
const topics = require("./topics");

dotenv.config();

const brokers = (process.env.KAFKA_BROKERS || "")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

let consumer;
let started = false;

function parseBoolean(value) {
  return ["1", "true", "yes", "on"].includes(String(value || "").toLowerCase());
}

function parseMultiline(value) {
  return value ? value.replace(/\\n/g, "\n") : undefined;
}

function buildKafkaConfig() {
  const sslEnabled = parseBoolean(process.env.KAFKA_SSL);
  const username = process.env.KAFKA_SASL_USERNAME;
  const password = process.env.KAFKA_SASL_PASSWORD;
  const ca = parseMultiline(process.env.KAFKA_SSL_CA);
  const cert = parseMultiline(process.env.KAFKA_SSL_CERT);
  const key = parseMultiline(process.env.KAFKA_SSL_KEY);

  const ssl =
    sslEnabled
      ? {
          rejectUnauthorized: !parseBoolean(process.env.KAFKA_SSL_REJECT_UNAUTHORIZED_FALSE),
          ca: ca ? [ca] : undefined,
          cert,
          key
        }
      : undefined;

  return {
    clientId: process.env.KAFKA_CLIENT_ID || "bid-service",
    brokers,
    ssl,
    sasl:
      username && password
        ? {
            mechanism: process.env.KAFKA_SASL_MECHANISM || "plain",
            username,
            password
          }
        : undefined
  };
}

function parseMessage(message) {
  try {
    return JSON.parse(message.value.toString());
  } catch (error) {
    console.error("Failed to parse Kafka message:", error.message);
    return null;
  }
}

async function handleListingPublished(payload) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    await client.query(
      `
        INSERT INTO auction_sessions (session_id, title, status, source_payload, updated_at)
        VALUES ($1, $2, COALESCE($3, 'SCHEDULED'), $4, NOW())
        ON CONFLICT (session_id)
        DO UPDATE SET
          title = EXCLUDED.title,
          status = CASE
            WHEN auction_sessions.status IN ('LIVE', 'FINALIZED') THEN auction_sessions.status
            ELSE EXCLUDED.status
          END,
          source_payload = EXCLUDED.source_payload,
          updated_at = NOW()
      `,
      [
        payload.sessionId,
        payload.title || `Auction ${payload.sessionId}`,
        payload.status || "SCHEDULED",
        payload
      ]
    );

    const baskets = Array.isArray(payload.baskets) ? payload.baskets : [];

    for (const basket of baskets) {
      await client.query(
        `
          INSERT INTO auction_baskets
            (basket_id, session_id, basket_no, description, starting_price, source_payload, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, NOW())
          ON CONFLICT (basket_id)
          DO UPDATE SET
            session_id = EXCLUDED.session_id,
            basket_no = EXCLUDED.basket_no,
            description = EXCLUDED.description,
            starting_price = EXCLUDED.starting_price,
            source_payload = EXCLUDED.source_payload,
            updated_at = NOW()
        `,
        [
          basket.basketId,
          payload.sessionId,
          basket.basketNo || null,
          basket.description || "Fish basket",
          basket.startingPrice || 0,
          basket
        ]
      );
    }

    await client.query("COMMIT");
    console.log(`Upserted session ${payload.sessionId} with ${baskets.length} baskets`);

    getIo().emit("homeUpdated", {
      sessionId: payload.sessionId,
      eventType: "listingPublished"
    });

    getIo().to(payload.sessionId).emit("sessionProjectionUpdated", {
      sessionId: payload.sessionId,
      eventType: "listingPublished"
    });
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function handleAuctionLive(payload) {
  await pool.query(
    `
      UPDATE auction_sessions
      SET status = 'LIVE', updated_at = NOW()
      WHERE session_id = $1
    `,
    [payload.sessionId]
  );

  console.log(`Session ${payload.sessionId} marked LIVE`);

  getIo().emit("homeUpdated", {
    sessionId: payload.sessionId,
    eventType: "auctionLive"
  });

  getIo().to(payload.sessionId).emit("sessionProjectionUpdated", {
    sessionId: payload.sessionId,
    eventType: "auctionLive"
  });
}

async function start() {
  if (!brokers.length || started) {
    if (!brokers.length) {
      console.log("Kafka consumer disabled because KAFKA_BROKERS is not configured.");
    }

    return;
  }

  const kafka = new Kafka(buildKafkaConfig());

  consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID || "bid-service-group"
  });

  await consumer.connect();
  await consumer.subscribe({
    topics: Object.values(topics.consumedTopics),
    fromBeginning: true
  });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const payload = parseMessage(message);

      if (!payload) {
        return;
      }

      try {
        switch (topic) {
          case topics.consumedTopics.LISTING_PUBLISHED:
            // Projection update from CLS service into BID-owned tables.
            await handleListingPublished(payload);
            break;
          case topics.consumedTopics.AUCTION_LIVE:
            await handleAuctionLive(payload);
            break;
          case topics.consumedTopics.CUSTOMER_AUTHENTICATED:
          case topics.consumedTopics.CUSTOMER_ELIGIBLE:
            console.log(`Observed auth event on ${topic}:`, payload);
            break;
          default:
            console.log(`Unhandled topic ${topic}`);
        }
      } catch (error) {
        console.error(`Kafka consumer failed on ${topic}:`, error);
      }
    }
  });

  started = true;
  console.log("Kafka consumer started");
}

async function stop() {
  if (consumer && started) {
    await consumer.disconnect();
    started = false;
  }
}

module.exports = {
  start,
  stop
};
