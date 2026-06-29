const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// 静的ファイルを public/ から配信
app.use(express.static(path.join(__dirname, 'public')));

// 接続中のクライアントを管理 { ws, username }
const clients = new Map();

// 直近のメッセージ履歴（最大20件）
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

function broadcastAll(data) {
  broadcast(data, null);
}

function getOnlineCount() {
  return wss.clients.size;
}

wss.on('connection', (ws) => {
  let username = null;

  ws.on('message', (raw) => {
    let data;
    try {
      data = JSON.parse(raw);
    } catch {
      return;
    }

    if (data.type === 'join') {
      // 入室
      username = data.username.trim().slice(0, 20) || '名無し';
      clients.set(ws, username);

      // 過去のメッセージ履歴を送る
      ws.send(JSON.stringify({ type: 'history', messages: history }));

      // 入室通知
      const joinMsg = {
        type: 'system',
        text: `${username} さんが入室しました`,
        count: getOnlineCount(),
      };
      ws.send(JSON.stringify(joinMsg));
      broadcast(joinMsg, ws);

    } else if (data.type === 'message') {
      if (!username) return;
      const text = data.text.trim().slice(0, 500);
      if (!text) return;

      const now = new Date();
      const time = now.toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit' });

      const chatMsg = {
        type: 'message',
        username,
        text,
        time,
      };

      // 履歴に追加
      history.push(chatMsg);
      if (history.length > MAX_HISTORY) history.shift();

      broadcastAll(chatMsg);
    }
  });

  ws.on('close', () => {
    if (username) {
      clients.delete(ws);
      const leaveMsg = {
        type: 'system',
        text: `${username} さんが退室しました`,
        count: getOnlineCount(),
      };
      broadcastAll(leaveMsg);
    }
  });

  ws.on('error', () => ws.terminate());
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`チャットサーバー起動中 → http://localhost:${PORT}`);
});
