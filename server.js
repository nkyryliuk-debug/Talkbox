/**
 * TALKBOX — WebSocket Signaling Server
 * Leitet WebRTC-Signale zwischen Clients weiter (Offer/Answer/ICE).
 * Kein Audio läuft über diesen Server — nur Steuernachrichten.
 *
 * Start: node server.js
 * Optional PORT: PORT=3000 node server.js
 */

const http = require('http');
const fs   = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

// ── HTTP Server (liefert index.html aus) ─────────────────────
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
  res.writeHead(404);
  res.end('Not found');
});

// ── WebSocket Signaling ──────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer });

// clients: Map<id, { ws, nickname, channel }>
const clients = new Map();

function send(ws, msg) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

function broadcast(channelNum, msg, excludeId = null) {
  clients.forEach((client, id) => {
    if (id !== excludeId && client.channel === channelNum) {
      send(client.ws, msg);
    }
  });
}

function sendPresenceList(channelNum) {
  const users = [];
  clients.forEach((c, id) => {
    if (c.channel === channelNum) users.push({ id, nickname: c.nickname });
  });
  clients.forEach((c) => {
    if (c.channel === channelNum) {
      send(c.ws, { type: 'peers', users });
    }
  });
}

wss.on('connection', (ws) => {
  let myId = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      case 'join': {
        myId = msg.id || crypto.randomUUID();
        const channel = parseInt(msg.channel) || 1;
        const nickname = (msg.nickname || 'USER').toUpperCase().slice(0, 12);
        clients.set(myId, { ws, nickname, channel });
        // Confirm join + give them their ID
        send(ws, { type: 'joined', id: myId, channel, nickname });
        // Tell everyone else someone joined
        broadcast(channel, { type: 'peer_joined', id: myId, nickname }, myId);
        // Send current peer list to new client
        sendPresenceList(channel);
        console.log(`[+] ${nickname} (${myId.slice(0,8)}) → Kanal ${channel}  [${clients.size} online]`);
        break;
      }

      case 'channel_change': {
        if (!myId || !clients.has(myId)) break;
        const c = clients.get(myId);
        const oldChannel = c.channel;
        const newChannel = parseInt(msg.channel) || 1;
        // Notify old channel
        broadcast(oldChannel, { type: 'peer_left', id: myId }, myId);
        sendPresenceList(oldChannel);
        // Switch
        c.channel = newChannel;
        broadcast(newChannel, { type: 'peer_joined', id: myId, nickname: c.nickname }, myId);
        sendPresenceList(newChannel);
        send(ws, { type: 'channel_changed', channel: newChannel });
        console.log(`[~] ${c.nickname} wechselt zu Kanal ${newChannel}`);
        break;
      }

      // WebRTC relay — einfach weiterleiten
      case 'offer':
      case 'answer':
      case 'ice': {
        if (!msg.to) break;
        const target = clients.get(msg.to);
        if (target) {
          send(target.ws, { ...msg, from: myId, nickname: clients.get(myId)?.nickname });
        }
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
    console.log(`[-] ${c.nickname} (${myId.slice(0,8)}) disconnected  [${clients.size - 1} online]`);
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
  console.log(`║   Strg+C zum Stoppen                 ║`);
  console.log(`╚══════════════════════════════════════╝\n`);
});
