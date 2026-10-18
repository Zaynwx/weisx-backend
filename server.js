const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" }
});

// Bases de dados em memória para teste rápido
const users = new Map(); // username -> { username, password, weisxId, token }
const tokens = new Map(); // token -> username
const rooms = new Map(); // roomCode -> { hostId, participants: Map }

function viewersOf(room, broadcasterId) {
    if (!room.viewers) room.viewers = new Map();
    if (!room.viewers.has(broadcasterId)) room.viewers.set(broadcasterId, new Set());
    return room.viewers.get(broadcasterId);
}

// Rota de Registo
app.post("/api/auth/register", (req, res) => {
    const { username, password } = req.body;
    if (!username || !password) {
        return res.status(400).json({ success: false, message: "Preencha todos os campos." });
    }
    const cleanUser = username.trim().toLowerCase();
    if (users.has(cleanUser)) {
        return res.status(400).json({ success: false, message: "Este nome de usuário já está em uso." });
    }

    const weisxId = "W-" + Math.floor(100000 + Math.random() * 900000);
    const newUser = { username: username.trim(), password, weisxId, recoveryEmail: null, recoveryEmailVerified: false };
    users.set(cleanUser, newUser);

    res.json({ success: true, user: { username: newUser.username, weisxId: newUser.weisxId } });
});

// Rota de Login
app.post("/api/auth/login", (req, res) => {
    const { identifier, password } = req.body;
    if (!identifier || !password) {
        return res.status(400).json({ success: false, message: "Preencha a conta e a senha." });
    }

    const cleanId = identifier.trim().toLowerCase();
    let foundUser = null;

    for (const u of users.values()) {
        if (u.username.toLowerCase() === cleanId || u.weisxId.toLowerCase() === cleanId) {
            foundUser = u;
            break;
        }
    }

    if (!foundUser || foundUser.password !== password) {
        return res.status(401).json({ success: false, message: "Usuário ou senha incorretos." });
    }

    const token = "token-" + Math.random().toString(36).slice(2) + Date.now();
    tokens.set(token, foundUser.username.toLowerCase());

    res.json({
        success: true,
        token,
        user: { username: foundUser.username, weisxId: foundUser.weisxId, recoveryEmail: foundUser.recoveryEmail, recoveryEmailVerified: foundUser.recoveryEmailVerified }
    });
});

// Rota /me (Verificar sessão)
app.get("/api/auth/me", (req, res) => {
    const authHeader = req.headers.authorization;
    if (!authHeader) return res.status(401).json({ success: false });
    const token = authHeader.split(" ")[1];
    const username = tokens.get(token);
    if (!username || !users.has(username)) return res.status(401).json({ success: false });

    const user = users.get(username);
    res.json({ success: true, user: { username: user.username, weisxId: user.weisxId, recoveryEmail: user.recoveryEmail, recoveryEmailVerified: user.recoveryEmailVerified } });
});

app.post("/api/auth/logout", (req, res) => {
    const authHeader = req.headers.authorization;
    if (authHeader) {
        const token = authHeader.split(" ")[1];
        tokens.delete(token);
    }
    res.json({ success: true });
});

// Perfil e outras rotas básicas
app.get("/api/account/profile", (req, res) => {
    const authHeader = req.headers.authorization;
    if (!authHeader) return res.status(401).json({ success: false, message: "Sessão expirada." });
    const token = authHeader.split(" ")[1];
    const username = tokens.get(token);
    if (!username || !users.has(username)) return res.status(401).json({ success: false, message: "Sessão expirada." });

    const user = users.get(username);
    res.json({ success: true, user: { username: user.username, weisxId: user.weisxId, recoveryEmail: user.recoveryEmail, recoveryEmailVerified: user.recoveryEmailVerified } });
});

// Descobre o nome da conta a partir do token de login (ou null se inválido)
function resolveUsernameFromToken(token) {
    if (!token || typeof token !== "string") return null;
    const key = tokens.get(token);
    const user = key ? users.get(key) : null;
    return user ? user.username : null;
}

// WebSocket para Salas e Transmissão
io.on("connection", socket => {
    let currentRoomCode = null;
    let currentUsername = "Convidado";

    socket.on("create-room", (roomCode, auth) => {
        currentUsername = resolveUsernameFromToken(auth && auth.token) || "Convidado";
        currentRoomCode = roomCode;
        socket.join(roomCode);
        rooms.set(roomCode, {
            hostId: socket.id,
            participants: new Map([[socket.id, { id: socket.id, name: currentUsername, isHost: true, broadcasting: false }]])
        });
        socket.emit("room-created", { selfId: socket.id, roomCode });
        updateRoomState(roomCode);
    });

    socket.on("join-room", (roomCode, auth) => {
        const room = rooms.get(roomCode);
        if (!room) {
            socket.emit("room-error", "Sala não encontrada.");
            return;
        }
        currentUsername = resolveUsernameFromToken(auth && auth.token) || "Convidado";
        currentRoomCode = roomCode;
        socket.join(roomCode);
        room.participants.set(socket.id, { id: socket.id, name: currentUsername, isHost: false, broadcasting: false });
        socket.emit("room-joined", { selfId: socket.id, roomCode });
        updateRoomState(roomCode);
    });

    socket.on("start-broadcast", data => {
        if (!currentRoomCode) return;
        const room = rooms.get(currentRoomCode);
        if (!room) return;
        const p = room.participants.get(socket.id);
        if (p) {
            p.broadcasting = true;
            p.viewerLimit = Math.max(0, Math.min(50, parseInt(data && data.viewerLimit, 10) || 0));
            viewersOf(room, socket.id).clear();
            socket.emit("broadcast-started-self");
            updateRoomState(currentRoomCode);
        }
    });

    socket.on("stop-broadcast", () => {
        if (!currentRoomCode) return;
        const room = rooms.get(currentRoomCode);
        if (!room) return;
        const p = room.participants.get(socket.id);
        if (p) {
            p.broadcasting = false;
            p.viewerLimit = 0;
            viewersOf(room, socket.id).clear();
            updateRoomState(currentRoomCode);
            io.to(currentRoomCode).emit("participant-broadcast-stopped", { participantId: socket.id });
        }
    });

    socket.on("watch-stream", data => {
        if (!data || !data.broadcasterId) return;
        const room = currentRoomCode ? rooms.get(currentRoomCode) : null;
        const target = room ? room.participants.get(data.broadcasterId) : null;

        if (room && target) {
            const viewers = viewersOf(room, data.broadcasterId);
            if (target.viewerLimit > 0 && !viewers.has(socket.id) && viewers.size >= target.viewerLimit) {
                socket.emit("watch-error", {
                    broadcasterId: data.broadcasterId,
                    message: "Esta transmissão atingiu o limite de espectadores."
                });
                return;
            }
            viewers.add(socket.id);
        }

        io.to(data.broadcasterId).emit("viewer-request", { viewerId: socket.id });
    });

    socket.on("stop-watching", data => {
        if (!data || !data.broadcasterId) return;
        const room = currentRoomCode ? rooms.get(currentRoomCode) : null;
        if (room) viewersOf(room, data.broadcasterId).delete(socket.id);
        io.to(data.broadcasterId).emit("viewer-left", { viewerId: socket.id });
    });

    socket.on("signal", data => {
        if (!data || !data.targetId) return;
        io.to(data.targetId).emit("signal", {
            sourceId: socket.id,
            type: data.type,
            sdp: data.sdp,
            candidate: data.candidate,
            connectionId: data.connectionId
        });
    });

    function leaveCurrentRoom() {
        if (!currentRoomCode) return;
        const code = currentRoomCode;
        currentRoomCode = null;
        socket.leave(code);

        const room = rooms.get(code);
        if (!room) return;

        if (room.hostId === socket.id) {
            io.to(code).emit("host-left");
            rooms.delete(code);
        } else {
            room.participants.delete(socket.id);
            if (room.viewers) {
                room.viewers.delete(socket.id);
                room.viewers.forEach(set => set.delete(socket.id));
            }
            updateRoomState(code);
            io.to(code).emit("participant-left", { participantId: socket.id });
        }
    }

    socket.on("leave-room", leaveCurrentRoom);
    socket.on("disconnect", leaveCurrentRoom);
});

function updateRoomState(roomCode) {
    const room = rooms.get(roomCode);
    if (!room) return;
    const participantsList = Array.from(room.participants.values());
    io.to(roomCode).emit("room-state", { participants: participantsList });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log("Servidor WEISX rodando na porta " + PORT);
});
