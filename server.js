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

// ---------- Servidor principal WEISX: canais, membros e chat ----------
let mainServerId = null;
let channelCache = [];

async function loadChannels() {
    const result = await pool.query(
        "SELECT id, name FROM channels WHERE server_id = $1 ORDER BY position, id",
        [mainServerId]
    );
    channelCache = result.rows;
}

function buildWorkspaceState() {
    return {
        server: { id: mainServerId, name: "WEISX" },
        channels: channelCache.map(channel => {
            const room = rooms.get("ch:" + channel.id);
            const users = room
                ? Array.from(room.participants.values()).map(p => ({ id: p.id, name: p.name, broadcasting: !!p.broadcasting }))
                : [];
            return { id: channel.id, name: channel.name, users };
        })
    };
}

function broadcastWorkspace() {
    io.to("srv:main").emit("workspace-state", buildWorkspaceState());
}

// Garante que o usuário é membro do servidor WEISX e devolve o cargo dele
async function ensureMainMembership(user) {
    const server = (await pool.query("SELECT id, owner_id FROM servers WHERE id = $1", [mainServerId])).rows[0];

    if (!server.owner_id) {
        const wanted = (process.env.OWNER_USERNAME || "").trim().toLowerCase();
        const ownerRow = wanted
            ? (await pool.query("SELECT id FROM users WHERE username_lower = $1", [wanted])).rows[0]
            : (await pool.query("SELECT id FROM users ORDER BY id LIMIT 1")).rows[0];
        if (ownerRow) {
            await pool.query("UPDATE servers SET owner_id = $1 WHERE id = $2 AND owner_id IS NULL", [ownerRow.id, mainServerId]);
            await pool.query(
                `INSERT INTO server_members (server_id, user_id, role) VALUES ($1, $2, 'owner')
                 ON CONFLICT (server_id, user_id) DO UPDATE SET role = 'owner'`,
                [mainServerId, ownerRow.id]
            );
        }
    }

    await pool.query(
        "INSERT INTO server_members (server_id, user_id, role) VALUES ($1, $2, 'member') ON CONFLICT (server_id, user_id) DO NOTHING",
        [mainServerId, user.id]
    );
    const member = (await pool.query(
        "SELECT role FROM server_members WHERE server_id = $1 AND user_id = $2", [mainServerId, user.id]
    )).rows[0];
    return member ? member.role : "member";
}

async function canManageChannels(socket) {
    if (!socket.data.user) return false;
    const result = await pool.query(
        "SELECT role FROM server_members WHERE server_id = $1 AND user_id = $2",
        [mainServerId, socket.data.user.id]
    );
    return !!result.rows[0] && ["owner", "admin"].includes(result.rows[0].role);
}

async function loadMessages(channelId) {
    const result = await pool.query(
        "SELECT id, username, content, kind, created_at FROM messages WHERE channel_id = $1 ORDER BY id DESC LIMIT 50",
        [channelId]
    );
    return result.rows.reverse().map(row => ({
        id: row.id, kind: row.kind, username: row.username, text: row.content, at: row.created_at
    }));
}

async function pruneMessages(channelId) {
    await pool.query(
        `DELETE FROM messages WHERE channel_id = $1 AND id <= (
            SELECT id FROM messages WHERE channel_id = $1 ORDER BY id DESC OFFSET 500 LIMIT 1
         )`,
        [channelId]
    );
}

// Linha de sistema no chat do canal ("fulano entrou" / "fulano saiu")
async function postSystem(channelId, username, kind) {
    const saved = await pool.query(
        "INSERT INTO messages (channel_id, username, content, kind) VALUES ($1, $2, '', $3) RETURNING id, created_at",
        [channelId, username, kind]
    );
    await pruneMessages(channelId);
    io.to("ch:" + channelId).emit("chat-message", {
        channelId, id: saved.rows[0].id, kind, username, text: "", at: saved.rows[0].created_at
    });
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

    await pool.query(`
        CREATE TABLE IF NOT EXISTS servers (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            owner_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
            password_hash TEXT,
            is_main BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS channels (
            id SERIAL PRIMARY KEY,
            server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS server_members (
            server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            role TEXT NOT NULL DEFAULT 'member',
            PRIMARY KEY (server_id, user_id)
        )
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS messages (
            id BIGSERIAL PRIMARY KEY,
            channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
            user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
            username TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);

    await pool.query("ALTER TABLE messages ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'msg'");

    let main = (await pool.query("SELECT id FROM servers WHERE is_main LIMIT 1")).rows[0];
    if (!main) {
        main = (await pool.query("INSERT INTO servers (name, is_main) VALUES ('WEISX', TRUE) RETURNING id")).rows[0];
    }
    mainServerId = main.id;

    const count = (await pool.query("SELECT COUNT(*)::int AS n FROM channels WHERE server_id = $1", [mainServerId])).rows[0].n;
    if (count === 0) {
        const defaults = ["Geral", "Resenha", "Duo", "Trio", "Squad"];
        for (let i = 0; i < defaults.length; i++) {
            await pool.query("INSERT INTO channels (server_id, name, position) VALUES ($1, $2, $3)", [mainServerId, defaults[i], i + 1]);
        }
    }
    await loadChannels();
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
            const who = room.participants.get(socket.id);
            room.participants.delete(socket.id);
            if (room.channelId && who) {
                postSystem(room.channelId, who.name, "leave").catch(error => console.error("Erro no aviso de saída:", error));
            }
            if (room.viewers) {
                room.viewers.delete(socket.id);
                room.viewers.forEach(set => set.delete(socket.id));
            }
            if (room.channelId && room.participants.size === 0) {
                rooms.delete(code);
                broadcastWorkspace();
            } else {
                updateRoomState(code);
            }
            io.to(code).emit("participant-left", { participantId: socket.id });
        }
    }

    // ----- Servidor WEISX: canais permanentes e chat -----
    socket.on("open-workspace", async data => {
        try {
            const user = await getUserByToken(data && data.token);
            if (!user) {
                socket.emit("workspace-error", "Sessão inválida. Entre novamente.");
                return;
            }
            socket.data.user = { id: user.id, username: user.username };
            socket.join("srv:main");
            const role = await ensureMainMembership(user);
            socket.emit("workspace-ready", { role, state: buildWorkspaceState() });
        } catch (error) {
            console.error("Erro ao abrir o servidor WEISX:", error);
            socket.emit("workspace-error", "Não foi possível carregar o servidor. Tente novamente.");
        }
    });

    socket.on("close-workspace", () => {
        leaveCurrentRoom();
        socket.data.user = null;
        socket.leave("srv:main");
    });

    socket.on("join-channel", async data => {
        try {
            if (!socket.data.user) {
                socket.emit("room-error", "Sessão inválida. Entre novamente.");
                return;
            }
            const channelId = parseInt(data && data.channelId, 10);
            const channel = channelCache.find(c => c.id === channelId);
            if (!channel) {
                socket.emit("room-error", "Canal não encontrado.");
                return;
            }

            leaveCurrentRoom();

            const code = "ch:" + channelId;
            let room = rooms.get(code);
            if (!room) {
                room = { hostId: null, channelId, participants: new Map(), viewers: new Map() };
                rooms.set(code, room);
            }
            currentRoomCode = code;
            currentUsername = socket.data.user.username;
            socket.join(code);
            room.participants.set(socket.id, { id: socket.id, name: currentUsername, isHost: false, broadcasting: false });

            socket.emit("room-joined", { selfId: socket.id, roomCode: code, channelId, channelName: channel.name });
            socket.emit("chat-history", { channelId, messages: await loadMessages(channelId) });
            updateRoomState(code);
            await postSystem(channelId, currentUsername, "join");
        } catch (error) {
            console.error("Erro ao entrar no canal:", error);
            socket.emit("room-error", "Não foi possível entrar no canal.");
        }
    });

    socket.on("chat-send", async data => {
        try {
            const room = currentRoomCode ? rooms.get(currentRoomCode) : null;
            if (!room || !room.channelId || !socket.data.user) return;
            const text = String((data && data.text) || "").trim().slice(0, 500);
            if (!text) return;

            const now = Date.now();
            if (now - (socket.data.lastChat || 0) < 300) return;
            socket.data.lastChat = now;

            const saved = await pool.query(
                "INSERT INTO messages (channel_id, user_id, username, content) VALUES ($1, $2, $3, $4) RETURNING id, created_at",
                [room.channelId, socket.data.user.id, socket.data.user.username, text]
            );
            await pruneMessages(room.channelId);
            io.to(currentRoomCode).emit("chat-message", {
                channelId: room.channelId,
                kind: "msg",
                id: saved.rows[0].id,
                username: socket.data.user.username,
                text,
                at: saved.rows[0].created_at
            });
        } catch (error) {
            console.error("Erro ao enviar mensagem:", error);
        }
    });

    socket.on("channel-create", async data => {
        try {
            if (!(await canManageChannels(socket))) {
                socket.emit("workspace-error", "Você não tem permissão para criar canais.");
                return;
            }
            const name = String((data && data.name) || "").trim().slice(0, 32);
            if (!name) return;
            if (channelCache.length >= 30) {
                socket.emit("workspace-error", "Limite de 30 canais atingido.");
                return;
            }
            await pool.query(
                `INSERT INTO channels (server_id, name, position)
                 VALUES ($1, $2, COALESCE((SELECT MAX(position) FROM channels WHERE server_id = $1), 0) + 1)`,
                [mainServerId, name]
            );
            await loadChannels();
            broadcastWorkspace();
        } catch (error) {
            console.error("Erro ao criar canal:", error);
            socket.emit("workspace-error", "Não foi possível criar o canal.");
        }
    });

    socket.on("channel-rename", async data => {
        try {
            if (!(await canManageChannels(socket))) {
                socket.emit("workspace-error", "Você não tem permissão para renomear canais.");
                return;
            }
            const channelId = parseInt(data && data.channelId, 10);
            const name = String((data && data.name) || "").trim().slice(0, 32);
            if (!name || !channelCache.find(c => c.id === channelId)) return;
            await pool.query("UPDATE channels SET name = $1 WHERE id = $2 AND server_id = $3", [name, channelId, mainServerId]);
            await loadChannels();
            broadcastWorkspace();
        } catch (error) {
            console.error("Erro ao renomear canal:", error);
        }
    });

    socket.on("channel-delete", async data => {
        try {
            if (!(await canManageChannels(socket))) {
                socket.emit("workspace-error", "Você não tem permissão para excluir canais.");
                return;
            }
            const channelId = parseInt(data && data.channelId, 10);
            if (!channelCache.find(c => c.id === channelId)) return;

            const code = "ch:" + channelId;
            io.to(code).emit("channel-deleted", { channelId });
            io.in(code).socketsLeave(code);
            rooms.delete(code);

            await pool.query("DELETE FROM channels WHERE id = $1 AND server_id = $2", [channelId, mainServerId]);
            await loadChannels();
            broadcastWorkspace();
        } catch (error) {
            console.error("Erro ao excluir canal:", error);
        }
    });

    socket.on("leave-room", leaveCurrentRoom);
    socket.on("disconnect", leaveCurrentRoom);
});

function updateRoomState(roomCode) {
    const room = rooms.get(roomCode);
    if (!room) return;
    const participantsList = Array.from(room.participants.values());
    io.to(roomCode).emit("room-state", { participants: participantsList });
    if (room.channelId) broadcastWorkspace();
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
