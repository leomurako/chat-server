const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);

// Render などの本番環境では static を使わない（GitHub Pages から配信するため）
// ローカル確認用として public/ も配信する
app.use(express.static(path.join(__dirname, 'public')));

// ヘルスチェック（Render が死活監視に使う）
app.get('/health', (_req, res) => res.send('ok'));

const wss = new WebSocket.Server({ server });

const history = [];
const MAX_HISTORY = 20;

function broadcast(data, excludeWs = null) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(ws => {
    if (ws !== excludeWs && ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  });
}

wss.on('connection', (ws, req) => {
  let username = null;

  ws.on('message', raw => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    if (data.type === 'join') {
      username = (data.username || '名無し').trim().slice(0, 20);

      // 履歴を送る
      ws.send(JSON.stringify({ type: 'history', messages: history }));

      const joinMsg = {
        type: 'system',
        text: `${username} さんが入室しました`,
        count: wss.clients.size,
      };
      ws.send(JSON.stringify(joinMsg));
      broadcast(joinMsg, ws);

    } else if (data.type === 'message') {
      if (!username) return;
      const text = (data.text || '').trim().slice(0, 500);
      if (!text) return;

      const now = new Date();
      const time = now.toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit' });

      const chatMsg = { type: 'message', username, text, time };
      history.push(chatMsg);
      if (history.length > MAX_HISTORY) history.shift();

      // 送信者含め全員に配信
      broadcast(chatMsg);
      ws.send(JSON.stringify(chatMsg));
    }
  });

  ws.on('close', () => {
    if (username) {
      broadcast({
        type: 'system',
        text: `${username} さんが退室しました`,
        count: wss.clients.size,
      });
    }
  });

  ws.on('error', () => ws.terminate());
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
