const express = require("express");
const http = require("http");
const crypto = require("crypto");
const util = require("util");
const { Server } = require("socket.io");
const cors = require("cors");
const { Pool } = require("pg");

const scrypt = util.promisify(crypto.scrypt);

if (!process.env.DATABASE_URL) {
    console.error("ERRO: a variável DATABASE_URL não está configurada no Render.");
    process.exit(1);
}

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    max: 5
});

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" }
});

// Salas ficam em memória (não precisam sobreviver a reinícios)
const rooms = new Map(); // roomCode -> { hostId, participants: Map, viewers: Map }

function viewersOf(room, broadcasterId) {
    if (!room.viewers) room.viewers = new Map();
    if (!room.viewers.has(broadcasterId)) room.viewers.set(broadcasterId, new Set());
    return room.viewers.get(broadcasterId);
}

// ---------- Banco de dados (contas e sessões) ----------
const SESSION_DAYS = 90;

async function initDb() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            username_lower TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            weisx_id TEXT NOT NULL UNIQUE,
            recovery_email TEXT,
            recovery_email_verified BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`DELETE FROM sessions WHERE created_at < NOW() - INTERVAL '${SESSION_DAYS} days'`);
}

async function hashPassword(password) {
    const salt = crypto.randomBytes(16).toString("hex");
    const hash = (await scrypt(password, salt, 64)).toString("hex");
    return salt + ":" + hash;
}

async function verifyPassword(password, stored) {
    const [salt, hash] = String(stored).split(":");
    if (!salt || !hash) return false;
    const test = await scrypt(password, salt, 64);
    const expected = Buffer.from(hash, "hex");
    return test.length === expected.length && crypto.timingSafeEqual(test, expected);
}

function publicUser(row) {
    return {
        username: row.username,
        weisxId: row.weisx_id,
        recoveryEmail: row.recovery_email,
        recoveryEmailVerified: row.recovery_email_verified
    };
}

function readToken(req) {
    const header = req.headers.authorization;
    if (!header) return null;
    return header.split(" ")[1] || null;
}

async function getUserByToken(token) {
    if (!token || typeof token !== "string") return null;
    const result = await pool.query(
        `SELECT u.* FROM sessions s JOIN users u ON u.id = s.user_id
         WHERE s.token = $1 AND s.created_at > NOW() - INTERVAL '${SESSION_DAYS} days'`,
        [token]
    );
    return result.rows[0] || null;
}

function serverError(res, error) {
    console.error(error);
    res.status(500).json({ success: false, message: "Erro no servidor. Tente novamente." });
}

// Rota de Registo
app.post("/api/auth/register", async (req, res) => {
    try {
        const { username, password } = req.body || {};
        if (!username || !password) {
            return res.status(400).json({ success: false, message: "Preencha todos os campos." });
        }
        const cleanName = String(username).trim();
        if (!cleanName || cleanName.length > 32 || String(password).length > 128) {
            return res.status(400).json({ success: false, message: "Nome de usuário ou senha em formato inválido." });
        }

        let weisxId;
        for (let i = 0; i < 10; i++) {
            const candidate = "W-" + Math.floor(100000 + Math.random() * 900000);
            const taken = await pool.query("SELECT 1 FROM users WHERE weisx_id = $1", [candidate]);
            if (taken.rowCount === 0) { weisxId = candidate; break; }
        }
        if (!weisxId) return serverError(res, new Error("Sem ID WEISX livre"));

        const passwordHash = await hashPassword(String(password));
        const result = await pool.query(
            `INSERT INTO users (username, username_lower, password_hash, weisx_id)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (username_lower) DO NOTHING
             RETURNING *`,
            [cleanName, cleanName.toLowerCase(), passwordHash, weisxId]
        );
        if (result.rowCount === 0) {
            return res.status(400).json({ success: false, message: "Este nome de usuário já está em uso." });
        }

        const user = result.rows[0];
        res.json({ success: true, user: { username: user.username, weisxId: user.weisx_id } });
    } catch (error) {
        serverError(res, error);
    }
});

// Rota de Login
app.post("/api/auth/login", async (req, res) => {
    try {
        const { identifier, password } = req.body || {};
        if (!identifier || !password) {
            return res.status(400).json({ success: false, message: "Preencha a conta e a senha." });
        }

        const cleanId = String(identifier).trim().toLowerCase();
        const found = await pool.query(
            "SELECT * FROM users WHERE username_lower = $1 OR LOWER(weisx_id) = $1 LIMIT 1",
            [cleanId]
        );
        const user = found.rows[0];

        if (!user || !(await verifyPassword(String(password), user.password_hash))) {
            return res.status(401).json({ success: false, message: "Usuário ou senha incorretos." });
        }

        const token = crypto.randomBytes(32).toString("hex");
        await pool.query("INSERT INTO sessions (token, user_id) VALUES ($1, $2)", [token, user.id]);

        res.json({ success: true, token, user: publicUser(user) });
    } catch (error) {
        serverError(res, error);
    }
});

// Rota /me (Verificar sessão)
app.get("/api/auth/me", async (req, res) => {
    try {
        const user = await getUserByToken(readToken(req));
        if (!user) return res.status(401).json({ success: false });
        res.json({ success: true, user: publicUser(user) });
    } catch (error) {
        serverError(res, error);
    }
});

app.post("/api/auth/logout", async (req, res) => {
    try {
        const token = readToken(req);
        if (token) await pool.query("DELETE FROM sessions WHERE token = $1", [token]);
        res.json({ success: true });
    } catch (error) {
        serverError(res, error);
    }
});

// Perfil
app.get("/api/account/profile", async (req, res) => {
    try {
        const user = await getUserByToken(readToken(req));
        if (!user) return res.status(401).json({ success: false, message: "Sessão expirada." });
        res.json({ success: true, user: publicUser(user) });
    } catch (error) {
        serverError(res, error);
    }
});

// Descobre o nome da conta a partir do token de login (ou null se inválido)
async function resolveUsernameFromToken(token) {
    try {
        const user = await getUserByToken(token);
        return user ? user.username : null;
    } catch (error) {
        console.error("Erro ao validar token do socket:", error);
        return null;
    }
}

// WebSocket para Salas e Transmissão
io.on("connection", socket => {
    let currentRoomCode = null;
    let currentUsername = "Convidado";

    socket.on("create-room", async (roomCode, auth) => {
        currentUsername = (await resolveUsernameFromToken(auth && auth.token)) || "Convidado";
        currentRoomCode = roomCode;
        socket.join(roomCode);
        rooms.set(roomCode, {
            hostId: socket.id,
            participants: new Map([[socket.id, { id: socket.id, name: currentUsername, isHost: true, broadcasting: false }]])
        });
        socket.emit("room-created", { selfId: socket.id, roomCode });
        updateRoomState(roomCode);
    });

    socket.on("join-room", async (roomCode, auth) => {
        const room = rooms.get(roomCode);
        if (!room) {
            socket.emit("room-error", "Sala não encontrada.");
            return;
        }
        currentUsername = (await resolveUsernameFromToken(auth && auth.token)) || "Convidado";
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
initDb()
    .then(() => {
        console.log("Banco de dados conectado.");
        server.listen(PORT, () => {
            console.log("Servidor WEISX rodando na porta " + PORT);
        });
    })
    .catch(error => {
        console.error("Não foi possível iniciar o banco de dados:", error);
        process.exit(1);
    });
