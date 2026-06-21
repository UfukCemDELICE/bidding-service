const express = require("express");
const controller = require("../controllers/fulfillment.controller");

const router = express.Router();

router.get("/fulfillment", controller.renderFulfillmentDashboard);
router.get("/api/fulfillment/snapshot", controller.getFulfillmentSnapshot);
router.post("/fulfillment/sales/:basketId/pickup", controller.schedulePickup);
router.post("/fulfillment/sales/:basketId/delivery/check", controller.checkDelivery);
router.post("/fulfillment/sales/:basketId/complete", controller.completeBasket);
router.post("/fulfillment/sessions/:sessionId/captain-payments/calculate", controller.calculateCaptainPayments);
router.post("/fulfillment/sessions/:sessionId/close", controller.closeAuction);

module.exports = router;
