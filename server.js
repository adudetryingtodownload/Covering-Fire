/* Squad Battlefield co-op relay server (v1).
   Plain Node, no packages needed. It does NOT run the game: it keeps a list of open
   games (rooms), lets players host/join them, and passes messages between players in
   the same room. The host's browser runs the real game.
   Built for 2 players per room, but rooms carry a "max" so 3-4 players can come later. */
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const MAX_MSG = 4 * 1024 * 1024;           // biggest message we accept (4 MB)
const rooms = new Map();                    // roomId -> {id,name,hostId,max,started,members:Map(id->client)}
const clients = new Map();                  // clientId -> client
let nextId = 1;

/* ---------- HTTP (Render health check + a simple status page) ---------- */
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
  res.end('Squad Battlefield co-op server OK. Open games: ' + rooms.size + ', players online: ' + clients.size + '\n');
});

/* ---------- WebSocket handshake ---------- */
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { id: 'p' + (nextId++), socket, buf: Buffer.alloc(0), frags: [], room: null, name: 'Player', browsing: false, alive: true };
  clients.set(c.id, c);
  socket.on('data', d => onData(c, d));
  socket.on('close', () => drop(c));
  socket.on('error', () => drop(c));
  send(c, { t: 'hello', you: c.id });
});

/* ---------- WebSocket frames ---------- */
function onData(c, d) {
  c.buf = Buffer.concat([c.buf, d]);
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1];
    const fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
    if (len > MAX_MSG) { c.socket.destroy(); return; }
    const mOff = off; if (masked) off += 4;
    if (c.buf.length < off + len) return;
    let payload = c.buf.subarray(off, off + len);
    if (masked) { const m = c.buf.subarray(mOff, mOff + 4); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
    c.buf = c.buf.subarray(off + len);
    if (op === 0x8) { try { c.socket.end(frame(Buffer.alloc(0), 0x8)); } catch (e) {} drop(c); return; }
    if (op === 0x9) { rawSend(c, frame(payload, 0xA)); continue; }   // ping -> pong
    if (op === 0xA) { c.alive = true; continue; }                     // pong
    if (op === 0x1 || op === 0x2 || op === 0x0) {
      c.frags.push(payload);
      if (fin) { const txt = Buffer.concat(c.frags).toString('utf8'); c.frags = []; handle(c, txt); }
    }
  }
}
function frame(payload, op) {
  const len = payload.length; let head;
  if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
  else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  head[0] = 0x80 | op;
  return Buffer.concat([head, payload]);
}
function rawSend(c, buf) { if (!c.socket.destroyed) { try { c.socket.write(buf); } catch (e) {} } }
function send(c, obj) { rawSend(c, frame(Buffer.from(JSON.stringify(obj)), 0x1)); }

/* ---------- Rooms ---------- */
function roomList() {
  return [...rooms.values()].map(r => ({ id: r.id, name: r.name, players: r.members.size, max: r.max, started: r.started,
    host: (r.members.get(r.hostId) || {}).name || '' }));
}
function pushList() { const list = roomList(); for (const c of clients.values()) if (c.browsing) send(c, { t: 'rooms', rooms: list }); }
function members(r) { return [...r.members.values()].map(m => ({ id: m.id, name: m.name, host: m.id === r.hostId })); }
function toRoom(r, obj, exceptId) { for (const m of r.members.values()) if (m.id !== exceptId) send(m, obj); }
function clean(s, n) { return String(s == null ? '' : s).replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, n); }

function leaveRoom(c, why) {
  const r = c.room && rooms.get(c.room); c.room = null;
  if (!r) return;
  r.members.delete(c.id);
  if (r.hostId === c.id || r.members.size === 0) {          // host left: the game is over for everyone
    toRoom(r, { t: 'closed', why: why || 'The host left the game.' });
    for (const m of r.members.values()) m.room = null;
    rooms.delete(r.id);
  } else {
    toRoom(r, { t: 'peerLeft', id: c.id, name: c.name, members: members(r) });
  }
  pushList();
}
function drop(c) {
  if (!clients.has(c.id)) return;
  clients.delete(c.id);
  leaveRoom(c, 'The host disconnected.');
  try { c.socket.destroy(); } catch (e) {}
}

function handle(c, txt) {
  let m; try { m = JSON.parse(txt); } catch (e) { return; }
  if (!m || typeof m.t !== 'string') return;
  const r = c.room && rooms.get(c.room);
  switch (m.t) {
    case 'browse':                                         // show me the list of open games (and keep it updated)
      c.browsing = true; if (m.name) c.name = clean(m.name, 20) || c.name;
      send(c, { t: 'rooms', rooms: roomList() }); break;
    case 'stopBrowse': c.browsing = false; break;
    case 'host': {
      if (r) leaveRoom(c);
      c.name = clean(m.name, 20) || c.name; c.browsing = false;
      const id = crypto.randomBytes(4).toString('hex');
      const max = Math.max(2, Math.min(4, m.max | 0 || 2));
      const room = { id, name: clean(m.room, 32) || (c.name + "'s game"), hostId: c.id, max, started: false, members: new Map([[c.id, c]]) };
      rooms.set(id, room); c.room = id;
      send(c, { t: 'joined', room: { id, name: room.name, max }, you: c.id, host: true, members: members(room) });
      pushList(); break;
    }
    case 'join': {
      const room = rooms.get(m.room);
      if (!room) { send(c, { t: 'joinFail', why: 'That game is gone.' }); break; }
      if (room.started) { send(c, { t: 'joinFail', why: 'That game has already started.' }); break; }
      if (room.members.size >= room.max) { send(c, { t: 'joinFail', why: 'That game is full.' }); break; }
      if (r) leaveRoom(c);
      c.name = clean(m.name, 20) || c.name; c.browsing = false;
      room.members.set(c.id, c); c.room = room.id;
      send(c, { t: 'joined', room: { id: room.id, name: room.name, max: room.max }, you: c.id, host: false, members: members(room) });
      toRoom(room, { t: 'peerJoined', id: c.id, name: c.name, members: members(room) }, c.id);
      pushList(); break;
    }
    case 'leave': leaveRoom(c, 'The host closed the game.'); break;
    case 'started':                                        // host: game began, hide it from the join list
      if (r && r.hostId === c.id) { r.started = !!m.on; pushList(); } break;
    case 'kick':
      if (r && r.hostId === c.id && r.members.has(m.id) && m.id !== c.id) {
        const k = r.members.get(m.id); send(k, { t: 'closed', why: 'The host removed you from the game.' }); leaveRoom(k);
      } break;
    case 'msg':                                            // pass a game message on: to one player (m.to) or everyone else
      if (!r) break;
      if (m.to) { const tgt = r.members.get(m.to); if (tgt) send(tgt, { t: 'msg', from: c.id, d: m.d }); }
      else toRoom(r, { t: 'msg', from: c.id, d: m.d }, c.id);
      break;
    case 'ping': send(c, { t: 'pong', at: m.at }); break;
  }
}

/* Drop dead connections, and keep connections busy so proxies don't close them. */
setInterval(() => {
  for (const c of clients.values()) {
    if (!c.alive) { drop(c); continue; }
    c.alive = false; rawSend(c, frame(Buffer.alloc(0), 0x9));
  }
}, 25000);

server.listen(PORT, () => console.log('Co-op relay listening on port ' + PORT));
