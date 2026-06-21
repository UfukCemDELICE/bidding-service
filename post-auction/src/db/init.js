const fs = require("fs");
const path = require("path");
const pool = require("./pool");

async function init() {
  const schemaPath = path.join(__dirname, "schema.sql");
  const schemaSql = fs.readFileSync(schemaPath, "utf8");

  await pool.query(schemaSql);
  console.log("Database schema initialized.");
  await pool.end();
}

init().catch(async (error) => {
  console.error("Failed to initialize schema:", error);
  await pool.end();
  process.exit(1);
});
