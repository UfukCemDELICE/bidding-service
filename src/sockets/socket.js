const { Server } = require("socket.io");

let io;

function initializeSocket(server) {
  io = new Server(server);

  io.on("connection", (socket) => {
    socket.on("joinAuction", (sessionId) => {
      socket.join(sessionId);
    });
  });

  return io;
}

function getIo() {
  if (!io) {
    throw new Error("Socket.IO has not been initialized.");
  }

  return io;
}

module.exports = {
  initializeSocket,
  getIo
};
