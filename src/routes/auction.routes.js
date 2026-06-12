const express = require("express");
const controller = require("../controllers/auction.controller");

const router = express.Router();

router.get("/", controller.renderHome);
router.get("/api/home/snapshot", controller.getHomeSnapshot);
router.get("/auction/:sessionId", controller.renderAuctionDashboard);
router.get("/api/auction/:sessionId/snapshot", controller.getAuctionSnapshot);

router.post("/auction/:sessionId/baskets/:basketId/open", controller.openBasket);
router.post("/auction/:sessionId/baskets/:basketId/bid", controller.placeBid);
router.post("/auction/:sessionId/baskets/:basketId/close", controller.closeBasket);
router.post("/auction/:sessionId/baskets/:basketId/payment/confirm", controller.confirmPayment);

module.exports = router;
