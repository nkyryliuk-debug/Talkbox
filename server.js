/**
 * TALKBOX — WebSocket Signaling Server
 * mit Keep-Alive Self-Ping (verhindert Render Free-Tier Sleep)
 */

const http = require('http');
const fs   = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const RENDER_URL = process.env.RENDER_EXTERNAL_URL || null;

// ── HTTP Server ──────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
    const file = path.join(__dirname, 'index.html');
    fs.readFile(file, (err, data) => {
      if (err) { res.writeHead(404); res.end('index.html not found'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }
  // Health check endpoint (auch für Keep-Alive genutzt)
  if (req.url === '/ping') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('pong');
    return;
  }
  res.writeHead(404);
  res.end('Not found');
});

// ── Keep-Alive Self-Ping ─────────────────────────────────────
// Pingt sich alle 10 Minuten selbst an, damit Render nicht einschläft
function startKeepAlive() {
  if (!RENDER_URL) {
    console.log('[keep-alive] Kein RENDER_EXTERNAL_URL gesetzt — Self-Ping deaktiviert (lokal)');
    return;
  }
  const url = RENDER_URL.replace(/\/$/, '') + '/ping';
  console.log(`[keep-alive] Self-Ping aktiv → ${url} (alle 10 Min)`);

  setInterval(() => {
    const proto = url.startsWith('https') ? require('https') : require('http');
    const req = proto.get(url, (res) => {
      console.log(`[keep-alive] Ping OK — ${new Date().toISOString()}`);
    });
    req.on('error', (e) => console.warn('[keep-alive] Ping Fehler:', e.message));
    req.end();
  }, 10 * 60 * 1000); // 10 Minuten
}

// ── WebSocket Signaling ──────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer });

// clients: Map<id, { ws, nickname, channel }>
const clients = new Map();

function send(ws, msg) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

function broadcast(channelNum, msg, excludeId = null) {
  clients.forEach((client, id) => {
    if (id !== excludeId && client.channel === channelNum) send(client.ws, msg);
  });
}

function sendPresenceList(channelNum) {
  const users = [];
  clients.forEach((c, id) => { if (c.channel === channelNum) users.push({ id, nickname: c.nickname }); });
  clients.forEach((c) => { if (c.channel === channelNum) send(c.ws, { type: 'peers', users }); });
}

wss.on('connection', (ws) => {
  let myId = null;

  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case 'join': {
        myId = msg.id || crypto.randomUUID();
        const channel = parseInt(msg.channel) || 1;
        const nickname = (msg.nickname || 'USER').toUpperCase().slice(0, 12);
        clients.set(myId, { ws, nickname, channel });
        send(ws, { type: 'joined', id: myId, channel, nickname });
        broadcast(channel, { type: 'peer_joined', id: myId, nickname }, myId);
        sendPresenceList(channel);
        console.log(`[+] ${nickname} (${myId.slice(0,8)}) → Kanal ${channel}  [${clients.size} online]`);
        break;
      }
      case 'channel_change': {
        if (!myId || !clients.has(myId)) break;
        const c = clients.get(myId);
        const oldChannel = c.channel;
        const newChannel = parseInt(msg.channel) || 1;
        broadcast(oldChannel, { type: 'peer_left', id: myId }, myId);
        sendPresenceList(oldChannel);
        c.channel = newChannel;
        broadcast(newChannel, { type: 'peer_joined', id: myId, nickname: c.nickname }, myId);
        sendPresenceList(newChannel);
        send(ws, { type: 'channel_changed', channel: newChannel });
        console.log(`[~] ${c.nickname} wechselt zu Kanal ${newChannel}`);
        break;
      }
      case 'offer':
      case 'answer':
      case 'ice': {
        if (!msg.to) break;
        const target = clients.get(msg.to);
        if (target) send(target.ws, { ...msg, from: myId, nickname: clients.get(myId)?.nickname });
        break;
      }
      case 'talking': {
        if (!myId || !clients.has(myId)) break;
        const c = clients.get(myId);
        broadcast(c.channel, { type: 'talking', from: myId, nickname: c.nickname, active: msg.active }, myId);
        break;
      }
    }
  });

  ws.on('close', () => {
    if (!myId || !clients.has(myId)) return;
    const c = clients.get(myId);
    console.log(`[-] ${c.nickname} disconnected  [${clients.size - 1} online]`);
    broadcast(c.channel, { type: 'peer_left', id: myId, nickname: c.nickname });
    clients.delete(myId);
    sendPresenceList(c.channel);
  });

  ws.on('error', (e) => console.error('WS error:', e.message));
});

httpServer.listen(PORT, () => {
  console.log(`\n╔══════════════════════════════════════╗`);
  console.log(`║   TALKBOX Signaling Server läuft     ║`);
  console.log(`║   http://localhost:${PORT}              ║`);
  console.log(`╚══════════════════════════════════════╝\n`);
  startKeepAlive();
});
