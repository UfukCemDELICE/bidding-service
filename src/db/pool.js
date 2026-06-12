const { Pool } = require("pg");
const dotenv = require("dotenv");

dotenv.config();

function parseBoolean(value) {
  return ["1", "true", "yes", "on"].includes(String(value || "").toLowerCase());
}

function getSslConfig() {
  const sslEnabled =
    parseBoolean(process.env.PGSSL) ||
    ["require", "verify-ca", "verify-full"].includes(String(process.env.PGSSLMODE || "").toLowerCase());

  if (!sslEnabled) {
    return undefined;
  }

  const sslConfig = {
    rejectUnauthorized: !parseBoolean(process.env.PGSSL_REJECT_UNAUTHORIZED_FALSE)
  };

  if (process.env.PGSSL_CA) {
    sslConfig.ca = process.env.PGSSL_CA.replace(/\\n/g, "\n");
  }

  return sslConfig;
}

function buildPoolConfig() {
  const ssl = getSslConfig();

  if (process.env.PGHOST || process.env.PGUSER || process.env.PGDATABASE) {
    return {
      host: process.env.PGHOST || "localhost",
      port: Number(process.env.PGPORT || 5432),
      database: process.env.PGDATABASE || "bidding_service",
      user: process.env.PGUSER || "postgres",
      password: process.env.PGPASSWORD || "postgres",
      ssl
    };
  }

  if (process.env.DATABASE_URL) {
    const databaseUrl = new URL(process.env.DATABASE_URL);

    return {
      host: databaseUrl.hostname,
      port: Number(databaseUrl.port || 5432),
      database: databaseUrl.pathname.replace(/^\//, "") || "postgres",
      user: decodeURIComponent(databaseUrl.username),
      password: decodeURIComponent(databaseUrl.password),
      ssl
    };
  }

  return {
    host: "localhost",
    port: 5432,
    database: "bidding_service",
    user: "postgres",
    password: "postgres",
    ssl
  };
}

const pool = new Pool(buildPoolConfig());

module.exports = pool;
