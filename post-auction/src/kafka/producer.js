const { Kafka } = require("kafkajs");
const { buildKafkaConfig, hasKafkaBrokers } = require("./config");
const topics = require("./topics");
const { validateEventOrThrow } = require("./schema-registry");

let producer;
let isConnected = false;

function isAuctionEventTopic(topic) {
  return Object.values(topics.publishedTopics).includes(topic);
}

function buildMessage(payload, key) {
  const messageKey = key || payload.sessionId;

  return {
    key: messageKey ? String(messageKey) : undefined,
    value: JSON.stringify(payload)
  };
}

function getProducer() {
  if (!hasKafkaBrokers()) {
    return null;
  }

  if (!producer) {
    const kafka = new Kafka(buildKafkaConfig());
    producer = kafka.producer();
  }

  return producer;
}

async function connect() {
  const instance = getProducer();

  if (!instance || isConnected) {
    return;
  }

  await instance.connect();
  isConnected = true;
}

function validatePublishContract(topic, payload, key) {
  validateEventOrThrow(topic, payload);

  if (!isAuctionEventTopic(topic)) {
    return;
  }

  if (!payload.sessionId || typeof payload.sessionId !== "string") {
    throw new Error(`Auction event ${topic} must include a string sessionId`);
  }

  if (String(key || payload.sessionId) !== payload.sessionId) {
    throw new Error(`Kafka key for topic ${topic} must exactly match payload.sessionId`);
  }
}

async function publish(topic, payload, key) {
  validatePublishContract(topic, payload, key);
  const instance = getProducer();

  if (!instance) {
    console.log(`Kafka disabled, skipped publish to ${topic}`, payload);
    return;
  }

  if (!isConnected) {
    await connect();
  }

  await instance.send({
    topic,
    messages: [buildMessage(payload, key)]
  });
}

async function disconnect() {
  if (producer && isConnected) {
    await producer.disconnect();
    isConnected = false;
  }
}

module.exports = {
  connect,
  publish,
  disconnect
};
