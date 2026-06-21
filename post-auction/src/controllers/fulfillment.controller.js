const fulfillmentService = require("../domain/fulfillment.service");

async function renderFulfillmentDashboard(req, res, next) {
  try {
    const [dashboard, snapshot] = await Promise.all([
      fulfillmentService.getFulfillmentData(),
      fulfillmentService.getFulfillmentSnapshot()
    ]);

    res.render("fulfillment", {
      dashboard,
      snapshotToken: snapshot.token,
      error: null
    });
  } catch (error) {
    next(error);
  }
}

async function getFulfillmentSnapshot(req, res, next) {
  try {
    const snapshot = await fulfillmentService.getFulfillmentSnapshot();
    res.json(snapshot);
  } catch (error) {
    next(error);
  }
}

async function schedulePickup(req, res, next) {
  try {
    await fulfillmentService.schedulePickup(req.params.basketId, req.body.pickupLocation, req.body.pickupTimeWindow);
    res.redirect("/fulfillment");
  } catch (error) {
    next(error);
  }
}

async function checkDelivery(req, res, next) {
  try {
    await fulfillmentService.checkDelivery(req.params.basketId, req.body.address);
    res.redirect("/fulfillment");
  } catch (error) {
    next(error);
  }
}

async function completeBasket(req, res, next) {
  try {
    await fulfillmentService.completeBasket(req.params.basketId);
    res.redirect("/fulfillment");
  } catch (error) {
    next(error);
  }
}

async function calculateCaptainPayments(req, res, next) {
  try {
    await fulfillmentService.calculateCaptainPayments(req.params.sessionId);
    res.redirect("/fulfillment");
  } catch (error) {
    next(error);
  }
}

async function closeAuction(req, res, next) {
  try {
    await fulfillmentService.closeAuction(req.params.sessionId);
    res.redirect("/fulfillment");
  } catch (error) {
    next(error);
  }
}

module.exports = {
  renderFulfillmentDashboard,
  getFulfillmentSnapshot,
  schedulePickup,
  checkDelivery,
  completeBasket,
  calculateCaptainPayments,
  closeAuction
};
