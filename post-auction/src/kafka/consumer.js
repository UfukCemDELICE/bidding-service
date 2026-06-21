const { Kafka } = require("kafkajs");

const fulfillmentService = require("../domain/fulfillment.service");
const { buildKafkaConfig, getKafkaGroupId, hasKafkaBrokers } = require("./config");
const topics = require("./topics");

let consumer;
let started = false;

function parseMessage(message) {
  try {
    return JSON.parse(message.value.toString());
  } catch (error) {
    console.error("Failed to parse Kafka message:", error.message);
    return null;
  }
}

async function start() {
  if (!hasKafkaBrokers() || started) {
    if (!hasKafkaBrokers()) {
      console.log("Kafka consumer disabled because KAFKA_BROKERS is not configured.");
    }
    return;
  }

  const kafka = new Kafka(buildKafkaConfig());

  consumer = kafka.consumer({
    groupId: getKafkaGroupId()
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

      console.log(`Received message on topic ${topic}`);

      try {
        switch (topic) {
          case topics.consumedTopics.USER_BUYER_REGISTERED:
            await fulfillmentService.handleBuyerRegistered(payload);
            break;
          case topics.consumedTopics.USER_MEMBER_REGISTERED:
            await fulfillmentService.handleMemberRegistered(payload);
            break;
          case topics.consumedTopics.CATALOG_BASKET_CREATED:
            await fulfillmentService.handleBasketCreated(payload);
            break;
          case topics.consumedTopics.BID_BASKET_SOLD:
            await fulfillmentService.handleBasketSold(payload);
            break;
          case topics.consumedTopics.BID_ALL_BASKETS_FINALIZED:
            await fulfillmentService.handleAllBasketsFinalized(payload);
            break;
          default:
            console.log(`Unhandled topic ${topic}`);
        }
      } catch (error) {
        console.error(`Kafka consumer failed on topic ${topic}:`, error);
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
