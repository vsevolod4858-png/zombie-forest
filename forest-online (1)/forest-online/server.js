// Сервер для «Выжить в лесу»: раздаёт игру и соединяет игроков по WebSocket. Без зависимостей.
"use strict";
const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, "public");
const MIME = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css", ".png": "image/png", ".ico": "image/x-icon", ".json": "application/json" };

const server = http.createServer((req, res) => {
  let p = decodeURIComponent((req.url || "/").split("?")[0]);
  if (p === "/healthz") { res.writeHead(200); return res.end("ok"); }
  if (p === "/") p = "/index.html";
  const f = path.normalize(path.join(PUBLIC, p));
  if (!f.startsWith(PUBLIC)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (e, d) => {
    if (e) { res.writeHead(404); return res.end("not found"); }
    res.writeHead(200, { "Content-Type": MIME[path.extname(f)] || "application/octet-stream", "Cache-Control": "no-cache" });
    res.end(d);
  });
});

/* ---------- минимальный WebSocket (RFC 6455) ---------- */
const MAX_FRAME = 32 * 1024;
function accept(key) { return crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64"); }
function frame(op, payload) {
  const b = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  const n = b.length;
  let h;
  if (n < 126) h = Buffer.from([0x80 | op, n]);
  else if (n < 65536) h = Buffer.from([0x80 | op, 126, n >> 8, n & 255]);
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(n, 6); }
  return Buffer.concat([h, b]);
}
const clients = new Set();
let nextId = 1;

server.on("upgrade", (req, socket) => {
  if ((req.url || "").split("?")[0] !== "/ws" || !req.headers["sec-websocket-key"]) { socket.destroy(); return; }
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept(req.headers["sec-websocket-key"]) + "\r\n\r\n");
  socket.setNoDelay(true);
  const c = { id: "p" + (nextId++) + crypto.randomBytes(2).toString("hex"), socket, buf: Buffer.alloc(0), frag: [], rooms: new Map(), alive: true, tokens: 80, last: Date.now() };
  clients.add(c);
  send(c, { t: "hello", id: c.id });
  socket.on("data", (d) => {
    c.buf = Buffer.concat([c.buf, d]);
    try { parse(c); } catch (_) { kill(c); }
  });
  socket.on("close", () => kill(c));
  socket.on("error", () => kill(c));
});

function parse(c) {
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    const fin = (b[0] & 0x80) !== 0, op = b[0] & 15, masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 127, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { throw new Error("too big"); }
    if (len > MAX_FRAME || !masked) throw new Error("bad frame");
    if (b.length < off + 4 + len) return;
    const mask = b.slice(off, off + 4), data = Buffer.from(b.slice(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
    c.buf = b.slice(off + 4 + len);
    if (op === 8) { try { c.socket.write(frame(8, Buffer.alloc(0))); } catch (_) {} kill(c); return; }
    if (op === 9) { c.socket.write(frame(10, data)); continue; }
    if (op === 10) { c.alive = true; continue; }
    if (op === 0 || op === 1) {
      c.frag.push(data);
      if (c.frag.reduce((a, x) => a + x.length, 0) > MAX_FRAME) throw new Error("too big");
      if (fin) { const text = Buffer.concat(c.frag).toString("utf8"); c.frag = []; onMessage(c, text); }
    }
  }
}

function send(c, obj) { try { c.socket.write(frame(1, JSON.stringify(obj))); } catch (_) {} }

/* ---------- комнаты ---------- */
const rooms = new Map(); // name -> Map(id -> client)
const NAME_RE = /^[A-Za-z0-9~_-]{1,16}$/;
const MAX_PEERS = 32;

function clean(patch, cur) {
  const out = Object.assign({}, cur);
  for (const k of Object.keys(patch)) {
    if (!/^[a-zA-Z0-9_]{1,8}$/.test(k)) continue;
    if (patch[k] === null) delete out[k]; else out[k] = patch[k];
  }
  return out;
}
function broadcast(room, obj, except) {
  const m = rooms.get(room); if (!m) return;
  const s = frame(1, JSON.stringify(obj));
  for (const cl of m.values()) if (cl !== except) { try { cl.socket.write(s); } catch (_) {} }
}
function leave(c, room) {
  const m = rooms.get(room);
  if (!c.rooms.has(room)) return;
  c.rooms.delete(room);
  if (m) { m.delete(c.id); broadcast(room, { t: "pl", room, peer: c.id }); if (!m.size) rooms.delete(room); }
}
function kill(c) {
  if (!clients.has(c)) return;
  clients.delete(c);
  for (const r of Array.from(c.rooms.keys())) leave(c, r);
  try { c.socket.destroy(); } catch (_) {}
}

function onMessage(c, text) {
  const now = Date.now();
  c.tokens = Math.min(120, c.tokens + (now - c.last) / 1000 * 60); c.last = now;
  if (--c.tokens < 0) return;           // слишком часто — молча отбрасываем
  let m; try { m = JSON.parse(text); } catch (_) { return; }
  if (!m || typeof m.room !== "string" || !NAME_RE.test(m.room)) return;
  const room = m.room;
  if (m.t === "join") {
    if (c.rooms.has(room)) return send(c, { t: "joined", room });
    if (c.rooms.size >= 4) return send(c, { t: "err", room, code: "limit_reached" });
    let mm = rooms.get(room);
    if (!mm) { mm = new Map(); rooms.set(room, mm); }
    if (mm.size >= MAX_PEERS) return send(c, { t: "err", room, code: "limit_reached" });
    c.rooms.set(room, {});
    const list = [];
    for (const o of mm.values()) list.push({ peer: o.id, presence: o.rooms.get(room) });
    mm.set(c.id, c);
    send(c, { t: "sync", room, peers: list });
    broadcast(room, { t: "pj", room, peer: c.id, presence: {} }, c);
  } else if (m.t === "leave") {
    leave(c, room);
  } else if (m.t === "pres") {
    if (!c.rooms.has(room) || !m.patch || typeof m.patch !== "object") return;
    const next = clean(m.patch, c.rooms.get(room));
    if (JSON.stringify(next).length > 6000) return;
    c.rooms.set(room, next);
    broadcast(room, { t: "p", room, peer: c.id, patch: m.patch }, c);
  } else if (m.t === "emit") {
    if (!c.rooms.has(room) || typeof m.topic !== "string" || m.topic.length > 48) return;
    if (JSON.stringify(m.data === undefined ? null : m.data).length > 4000) return;
    broadcast(room, { t: "ev", room, topic: m.topic, data: m.data, peer: c.id }, c);
  }
}

// пинг, чтобы Render не обрывал неактивные соединения
setInterval(() => {
  for (const c of Array.from(clients)) {
    if (!c.alive) { kill(c); continue; }
    c.alive = false;
    try { c.socket.write(frame(9, Buffer.alloc(0))); } catch (_) {}
  }
}, 25000);

server.listen(PORT, () => console.log("Forest server on :" + PORT));
module.exports = server;
