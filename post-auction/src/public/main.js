(function () {
  const body = document.body;
  const page = body ? body.getAttribute("data-page") : null;
  const sessionId = body ? body.getAttribute("data-session-id") : null;
  let snapshotToken = body ? body.getAttribute("data-snapshot-token") : null;

  if (typeof io === "undefined" || !page) {
    return;
  }

  const socket = io();
  const banner = document.getElementById("liveBanner");
  let reloadTimer;

  function refreshSoon(message) {
    if (banner) {
      banner.textContent = message;
      banner.style.display = "block";
    }

    window.clearTimeout(reloadTimer);
    reloadTimer = window.setTimeout(() => {
      window.location.reload();
    }, 800);
  }

  async function pollSnapshot(url) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json"
        }
      });

      if (!response.ok) {
        return;
      }

      const payload = await response.json();

      if (snapshotToken && payload.token !== snapshotToken) {
        refreshSoon("Auction state changed. Refreshing view...");
        return;
      }

      snapshotToken = payload.token;
    } catch (error) {
      // Keep polling silent in the browser; live refresh is best-effort.
    }
  }

  if (sessionId) {
    socket.emit("joinAuction", sessionId);
  }

  if (page === "home") {
    socket.on("homeUpdated", function () {
      refreshSoon("Auction session list changed. Refreshing view...");
    });

    window.setInterval(function () {
      pollSnapshot("/api/home/snapshot");
    }, 3000);

    return;
  }

  if (page === "fulfillment") {
    socket.on("fulfillmentUpdated", function () {
      refreshSoon("Fulfillment state changed. Refreshing view...");
    });

    window.setInterval(function () {
      pollSnapshot("/api/fulfillment/snapshot");
    }, 3000);

    return;
  }

  if (!sessionId) {
    return;
  }

  socket.on("sessionProjectionUpdated", function () {
    refreshSoon("Auction session updated from Kafka. Refreshing view...");
  });

  window.setInterval(function () {
    pollSnapshot("/api/auction/" + sessionId + "/snapshot");
  }, 3000);

  socket.on("basketOpened", function () {
    refreshSoon("Basket opened. Refreshing view...");
  });

  socket.on("bidPlaced", function () {
    refreshSoon("New bid placed. Refreshing view...");
  });

  socket.on("basketClosed", function () {
    refreshSoon("Basket closed. Refreshing view...");
  });

  socket.on("auctionFinalized", function () {
    refreshSoon("Auction finalized. Refreshing view...");
  });
})();
