// Post-Auction & Fulfillment Service — Kafka topic registry.
//
// This service only consumes/produces its own fulfillment domain events.
// It does NOT publish Auction & Bidding events; those belong to the
// Auction service. Post-Auction consumes a small set of upstream events
// to keep local projections in sync.

module.exports = {
  consumedTopics: {
    USER_BUYER_REGISTERED: "user.buyer.registered",
    USER_MEMBER_REGISTERED: "user.member.registered",
    CATALOG_BASKET_CREATED: "catalog.basket.created",
    CATALOG_PUBLISHED: "catalog.published",
    BID_BASKET_SOLD: "bid.basket.sold",
    BID_ALL_BASKETS_FINALIZED: "bid.all.baskets.finalized"
  },
  publishedTopics: {
    FULFILLMENT_SALE_RECORDED: "fulfillment.sale.recorded",
    FULFILLMENT_PICKUP_SCHEDULED: "fulfillment.pickup.scheduled",
    FULFILLMENT_DELIVERY_CHECKED: "fulfillment.delivery.checked",
    FULFILLMENT_BASKET_COMPLETED: "fulfillment.basket.completed",
    FULFILLMENT_CAPTAIN_PAYMENT_CALCULATED: "fulfillment.captain.payment.calculated",
    FULFILLMENT_AUCTION_CLOSED: "fulfillment.auction.closed"
  }
};
