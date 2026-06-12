const dotenv = require("dotenv");

const producer = require("../kafka/producer");
const topics = require("../kafka/topics");

dotenv.config();

async function run() {
  const sessionNumber = process.argv[2] || "001";
  const sessionId = `session-${sessionNumber}`;

  const listingPayload = {
    sessionId,
    title: sessionNumber === "002" ? "Afternoon Fish Auction" : "Morning Fish Auction",
    baskets: [
      {
        basketId: `${sessionId}-basket-001`,
        basketNo: 1,
        description: "Anchovy - 25kg",
        startingPrice: 100
      },
      {
        basketId: `${sessionId}-basket-002`,
        basketNo: 2,
        description: "Sea Bass - 12kg",
        startingPrice: 180
      }
    ]
  };

  const livePayload = {
    sessionId
  };

  await producer.connect();

  // Seed the local BID projections through the same Kafka path used in production.
  await producer.publish(topics.consumedTopics.LISTING_PUBLISHED, listingPayload);
  await producer.publish(topics.consumedTopics.AUCTION_LIVE, livePayload);

  console.log(`Seed events published for ${sessionId}`);
  await producer.disconnect();
}

run().catch(async (error) => {
  console.error("Failed to publish seed events:", error);

  try {
    await producer.disconnect();
  } catch (disconnectError) {
    console.error("Failed to disconnect Kafka producer:", disconnectError.message);
  }

  process.exit(1);
});
