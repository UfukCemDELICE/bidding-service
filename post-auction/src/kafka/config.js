const dotenv = require("dotenv");

dotenv.config();

function hasKafkaBrokers() {
  return Boolean(process.env.KAFKA_BROKERS);
}

function getKafkaGroupId() {
  return process.env.KAFKA_GROUP_ID;
}

function buildKafkaConfig() {
  return {
    clientId: process.env.KAFKA_CLIENT_ID,
    brokers: [process.env.KAFKA_BROKERS],
    ssl: true,
    sasl: {
      mechanism: "plain",
      username: process.env.KAFKA_SASL_USERNAME,
      password: process.env.KAFKA_SASL_PASSWORD
    }
  };
}

module.exports = {
  hasKafkaBrokers,
  getKafkaGroupId,
  buildKafkaConfig
};
