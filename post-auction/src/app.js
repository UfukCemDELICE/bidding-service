const http = require("http");
const path = require("path");
const express = require("express");
const dotenv = require("dotenv");

const fulfillmentRoutes = require("./routes/fulfillment.routes");
const consumer = require("./kafka/consumer");
const producer = require("./kafka/producer");

dotenv.config();

const app = express();
const server = http.createServer(app);

app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));
app.use("/", fulfillmentRoutes);

app.get("/health", (req, res) => {
  res.json({ status: "ok", service: "post-auction", version: "1.0.0" });
});

app.use((err, req, res, next) => {
  console.error("Unhandled application error:", err);
  res.status(500).json({ status: "error", message: "Unexpected server error." });
});

const port = Number(process.env.PORT || 3000);

async function start() {
  server.listen(port, async () => {
    console.log(`Post-Auction service listening on port ${port}`);
    try {
      await consumer.start();
    } catch (error) {
      console.error("Kafka consumer could not start:", error.message);
    }
  });
}

async function shutdown(signal) {
  console.log(`${signal} received, shutting down Post-Auction service...`);
  try {
    await consumer.stop();
  } catch (error) {
    console.error("Error while stopping Kafka consumer:", error.message);
  }
  try {
    await producer.disconnect();
  } catch (error) {
    console.error("Error while stopping Kafka producer:", error.message);
  }
  server.close(() => process.exit(0));
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

start();
