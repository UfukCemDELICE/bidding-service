module.exports = {
  consumedTopics: {
    LISTING_PUBLISHED: "cls.auction.listing.published",
    AUCTION_LIVE: "main.online.auction.live",
    CUSTOMER_AUTHENTICATED: "auth.customer.authenticated",
    CUSTOMER_ELIGIBLE: "auth.customer.eligible_to_bid"
  },
  publishedTopics: {
    BASKET_DETAILS_ANNOUNCED: "bid.basket.details.announced",
    BID_PLACED: "bid.bid.placed",
    WINDOW_CLOSED: "bid.window.closed",
    BASKET_SOLD: "bid.basket.sold",
    BASKET_UNSOLD: "bid.basket.unsold",
    BASKET_QUEUED_FOR_REBID: "bid.basket.queued_for_rebid",
    REBID_ROUND_OPENED: "bid.rebid.round.opened",
    WINNING_BID_RECORDED: "bid.winning_bid.recorded",
    PAYMENT_CONFIRMED: "bid.payment.confirmed",
    BASKET_SALE_COMPLETED: "bid.basket.sale.completed",
    NEXT_BASKET_READY: "bid.next_basket.ready",
    ALL_BASKETS_FINALIZED: "bid.all_baskets.finalized"
  }
};
