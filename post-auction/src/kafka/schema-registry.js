const topics = require("./topics");

const schemaRegistry = {
  [topics.publishedTopics.FULFILLMENT_SALE_RECORDED]: require("../../schema/fulfillment.sale.recorded.schema.json"),
  [topics.publishedTopics.FULFILLMENT_PICKUP_SCHEDULED]: require("../../schema/fulfillment.pickup.scheduled.schema.json"),
  [topics.publishedTopics.FULFILLMENT_DELIVERY_CHECKED]: require("../../schema/fulfillment.delivery.checked.schema.json"),
  [topics.publishedTopics.FULFILLMENT_BASKET_COMPLETED]: require("../../schema/fulfillment.basket.completed.schema.json"),
  [topics.publishedTopics.FULFILLMENT_CAPTAIN_PAYMENT_CALCULATED]: require("../../schema/fulfillment.captain.payment.calculated.schema.json"),
  [topics.publishedTopics.FULFILLMENT_AUCTION_CLOSED]: require("../../schema/fulfillment.auction.closed.schema.json")
};

const uuidPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function isDateTime(value) {
  return typeof value === "string" && !Number.isNaN(Date.parse(value));
}

function isEmail(value) {
  return typeof value === "string" && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function validateType(schema, value) {
  switch (schema.type) {
    case "string":
      return typeof value === "string";
    case "number":
      return typeof value === "number" && Number.isFinite(value);
    case "integer":
      return Number.isInteger(value);
    case "array":
      return Array.isArray(value);
    case "object":
      return value !== null && typeof value === "object" && !Array.isArray(value);
    default:
      return true;
  }
}

function validateValue(name, schema, value, errors) {
  if (!validateType(schema, value)) {
    errors.push(`${name} must be of type ${schema.type}`);
    return;
  }

  if (schema.type === "string") {
    if (schema.minLength !== undefined && value.length < schema.minLength) {
      errors.push(`${name} must have at least ${schema.minLength} characters`);
    }

    if (schema.format === "uuid" && !uuidPattern.test(value)) {
      errors.push(`${name} must be a valid UUID`);
    }

    if (schema.format === "date-time" && !isDateTime(value)) {
      errors.push(`${name} must be a valid ISO-8601 timestamp`);
    }

    if (schema.format === "email" && !isEmail(value)) {
      errors.push(`${name} must be a valid email`);
    }
  }

  if ((schema.type === "number" || schema.type === "integer") && schema.minimum !== undefined && value < schema.minimum) {
    errors.push(`${name} must be greater than or equal to ${schema.minimum}`);
  }

  if (
    (schema.type === "number" || schema.type === "integer") &&
    schema.exclusiveMinimum !== undefined &&
    value <= schema.exclusiveMinimum
  ) {
    errors.push(`${name} must be greater than ${schema.exclusiveMinimum}`);
  }

  if (schema.enum && !schema.enum.includes(value)) {
    errors.push(`${name} must be one of: ${schema.enum.join(", ")}`);
  }

  if (schema.type === "array") {
    if (schema.minItems !== undefined && value.length < schema.minItems) {
      errors.push(`${name} must contain at least ${schema.minItems} items`);
    }

    if (schema.items) {
      value.forEach((item, index) => validateValue(`${name}[${index}]`, schema.items, item, errors));
    }
  }
}

function validateEventOrThrow(topic, payload) {
  const schema = schemaRegistry[topic];

  if (!schema) {
    throw new Error(`No schema registered for topic ${topic}`);
  }

  if (schema.type !== "object" || payload === null || typeof payload !== "object" || Array.isArray(payload)) {
    throw new Error(`Payload for topic ${topic} must be an object`);
  }

  const errors = [];
  const properties = schema.properties || {};

  for (const field of schema.required || []) {
    if (payload[field] === undefined) {
      errors.push(`Missing required field: ${field}`);
    }
  }

  if (schema.additionalProperties === false) {
    for (const key of Object.keys(payload)) {
      if (!Object.prototype.hasOwnProperty.call(properties, key)) {
        errors.push(`Unknown field: ${key}`);
      }
    }
  }

  for (const [name, propertySchema] of Object.entries(properties)) {
    if (payload[name] !== undefined) {
      validateValue(name, propertySchema, payload[name], errors);
    }
  }

  if (errors.length) {
    throw new Error(`Schema validation failed for topic ${topic}: ${errors.join("; ")}`);
  }
}

module.exports = {
  validateEventOrThrow
};
