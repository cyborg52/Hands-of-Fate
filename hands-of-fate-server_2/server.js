const WebSocket = require("ws");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;
const server = new WebSocket.Server({ port: PORT });

// Each entry uses a room code as its key.
const rooms = new Map();

function createRoomCode() {
    let code;

    do {
        code = crypto.randomBytes(3).toString("hex").toUpperCase();
    } while (rooms.has(code));

    return code;
}

function send(socket, message) {
    if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(message));
    }
}

function leaveCurrentRoom(socket) {
    if (!socket.roomCode) {
        return;
    }

    const room = rooms.get(socket.roomCode);

    if (!room) {
        socket.roomCode = null;
        socket.peerId = null;
        return;
    }

    room.players.delete(socket.peerId);

    // Tell the remaining players that someone disconnected.
    for (const playerSocket of room.players.values()) {
        send(playerSocket, {
            type: "peer_left",
            peer_id: socket.peerId
        });
    }

    // Delete empty rooms.
    if (room.players.size === 0) {
        rooms.delete(socket.roomCode);
        console.log(`Deleted empty room ${socket.roomCode}.`);
    }

    socket.roomCode = null;
    socket.peerId = null;
}

server.on("connection", (socket) => {
    socket.roomCode = null;
    socket.peerId = null;

    console.log("A client connected to the signaling server.");

    socket.on("message", (rawData) => {
        let message;

        try {
            message = JSON.parse(rawData.toString());
        } catch {
            send(socket, {
                type: "error",
                message: "Invalid JSON message."
            });
            return;
        }

        switch (message.type) {
            case "create_room": {
                leaveCurrentRoom(socket);

                const roomCode = createRoomCode();

                const room = {
                    hostId: 1,
                    nextPeerId: 2,
                    players: new Map()
                };

                // Godot reserves multiplayer ID 1 for the server.
                socket.roomCode = roomCode;
                socket.peerId = 1;

                room.players.set(1, socket);
                rooms.set(roomCode, room);

                send(socket, {
                    type: "room_created",
                    room_code: roomCode,
                    peer_id: 1
                });

                console.log(`Room ${roomCode} created by peer 1.`);
                break;
            }

            case "join_room": {
                leaveCurrentRoom(socket);

                const roomCode = String(
                    message.room_code || ""
                ).trim().toUpperCase();

                const room = rooms.get(roomCode);

                if (!room) {
                    send(socket, {
                        type: "error",
                        message: "Room not found."
                    });
                    return;
                }

                const existingPeerIds = Array.from(
                    room.players.keys()
                );

                const peerId = room.nextPeerId++;

                socket.roomCode = roomCode;
                socket.peerId = peerId;

                room.players.set(peerId, socket);

                // Tell the joining player which peers already exist.
                send(socket, {
                    type: "room_joined",
                    room_code: roomCode,
                    peer_id: peerId,
                    existing_peers: existingPeerIds
                });

                // Tell existing players about the joining player.
                for (const existingSocket of room.players.values()) {
                    if (existingSocket !== socket) {
                        send(existingSocket, {
                            type: "peer_joined",
                            peer_id: peerId
                        });
                    }
                }

                console.log(
                    `Peer ${peerId} joined room ${roomCode}.`
                );
                break;
            }

            case "offer":
            case "answer":
            case "candidate": {
                if (!socket.roomCode || !socket.peerId) {
                    send(socket, {
                        type: "error",
                        message: "Join a room before signaling."
                    });
                    return;
                }

                const room = rooms.get(socket.roomCode);

                if (!room) {
                    return;
                }

                const targetId = Number(message.target_id);
                const targetSocket = room.players.get(targetId);

                if (!targetSocket) {
                    send(socket, {
                        type: "error",
                        message: `Peer ${targetId} was not found.`
                    });
                    return;
                }

                // Replace target information with sender information
                // before forwarding the message.
                send(targetSocket, {
                    ...message,
                    peer_id: socket.peerId
                });

                break;
            }

            default:
                send(socket, {
                    type: "error",
                    message: "Unknown message type."
                });
        }
    });

    socket.on("close", () => {
        console.log(
            `Peer ${socket.peerId ?? "unknown"} disconnected.`
        );

        leaveCurrentRoom(socket);
    });

    socket.on("error", (error) => {
        console.error("WebSocket error:", error.message);
    });
});

console.log(
    `Signaling server running at ws://localhost:${PORT}`
);