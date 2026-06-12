const { Kafka } = require("kafkajs");
const dotenv = require("dotenv");

dotenv.config();

const brokers = (process.env.KAFKA_BROKERS || "")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

let producer;
let isConnected = false;

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

function getProducer() {
  if (!brokers.length) {
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

async function publish(topic, payload) {
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
    messages: [
      {
        value: JSON.stringify(payload)
      }
    ]
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
