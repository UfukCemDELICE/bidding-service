const biddingService = require("../domain/bidding.service");

async function renderHome(req, res, next) {
  try {
    const [sessions, snapshot] = await Promise.all([
      biddingService.getHomeData(),
      biddingService.getHomeSnapshot()
    ]);

    res.render("index", {
      sessions,
      snapshotToken: snapshot.token,
      error: null
    });
  } catch (error) {
    next(error);
  }
}

async function renderAuctionDashboard(req, res, next) {
  try {
    const [dashboard, snapshot] = await Promise.all([
      biddingService.getAuctionDashboard(req.params.sessionId),
      biddingService.getAuctionSnapshot(req.params.sessionId)
    ]);

    res.render("auction", {
      dashboard,
      snapshotToken: snapshot.token,
      error: null
    });
  } catch (error) {
    next(error);
  }
}

async function getHomeSnapshot(req, res, next) {
  try {
    const snapshot = await biddingService.getHomeSnapshot();
    res.json(snapshot);
  } catch (error) {
    next(error);
  }
}

async function getAuctionSnapshot(req, res, next) {
  try {
    const snapshot = await biddingService.getAuctionSnapshot(req.params.sessionId);
    res.json(snapshot);
  } catch (error) {
    next(error);
  }
}

async function openBasket(req, res) {
  try {
    await biddingService.openBasket(req.params.sessionId, req.params.basketId);
    res.redirect(`/auction/${req.params.sessionId}`);
  } catch (error) {
    res.status(400).send(error.message);
  }
}

async function placeBid(req, res) {
  try {
    const result = await biddingService.placeBid(req.params.sessionId, req.params.basketId, req.body);
    res.format({
      html: () => res.redirect(`/auction/${req.params.sessionId}`),
      json: () => res.json(result)
    });
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
}

async function closeBasket(req, res) {
  try {
    await biddingService.closeBasket(req.params.sessionId, req.params.basketId);
    res.redirect(`/auction/${req.params.sessionId}`);
  } catch (error) {
    res.status(400).send(error.message);
  }
}

async function confirmPayment(req, res) {
  try {
    await biddingService.confirmPayment(req.params.sessionId, req.params.basketId);
    res.redirect(`/auction/${req.params.sessionId}`);
  } catch (error) {
    res.status(400).send(error.message);
  }
}

module.exports = {
  renderHome,
  renderAuctionDashboard,
  getHomeSnapshot,
  getAuctionSnapshot,
  openBasket,
  placeBid,
  closeBasket,
  confirmPayment
};
