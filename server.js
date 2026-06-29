const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_req, res) => res.send('ok'));

// ========== インメモリストア ==========
const users = new Map();   // username -> { passwordHash, joinedRooms[] }
const rooms = new Map();   // roomCode -> { id, name, icon, passwordHash, members[], messages[] }
const sessions = new Map();// ws -> { username, roomCode }

function hash(str) {
  return crypto.createHash('sha256').update(str).digest('hex');
}

function genCode() {
  return crypto.randomBytes(3).toString('hex').toUpperCase(); // 6文字
}

function genMsgId() {
  return crypto.randomBytes(6).toString('hex');
}

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}

function broadcastRoom(roomCode, data, excludeWs = null) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(ws => {
    const s = sessions.get(ws);
    if (s && s.roomCode === roomCode && ws !== excludeWs && ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  });
}

function broadcastRoomAll(roomCode, data) {
  broadcastRoom(roomCode, data, null);
}

function onlineCount(roomCode) {
  let c = 0;
  wss.clients.forEach(ws => {
    const s = sessions.get(ws);
    if (s && s.roomCode === roomCode && ws.readyState === WebSocket.OPEN) c++;
  });
  return c;
}

// ========== WSハンドラ ==========
wss.on('connection', ws => {
  sessions.set(ws, { username: null, roomCode: null });

  ws.on('message', raw => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }
    const session = sessions.get(ws);

    // --- 認証 ---
    if (data.type === 'auth') {
      const { username, password, isRegister } = data;
      if (!username || !password) return send(ws, { type: 'authError', text: 'ユーザー名とパスワードを入力してください' });

      const uname = username.trim().slice(0, 20);
      const ph = hash(password);

      if (isRegister) {
        if (users.has(uname)) return send(ws, { type: 'authError', text: 'そのユーザー名はすでに使われています' });
        users.set(uname, { passwordHash: ph, joinedRooms: [] });
      } else {
        const u = users.get(uname);
        if (!u) return send(ws, { type: 'authError', text: 'ユーザーが見つかりません' });
        if (u.passwordHash !== ph) return send(ws, { type: 'authError', text: 'パスワードが違います' });
      }

      session.username = uname;
      const u = users.get(uname);

      // 参加中ルームの一覧を返す
      const joinedRoomList = u.joinedRooms
        .map(code => {
          const r = rooms.get(code);
          if (!r) return null;
          return { code, name: r.name, icon: r.icon };
        })
        .filter(Boolean);

      send(ws, { type: 'authOk', username: uname, joinedRooms: joinedRoomList });
      return;
    }

    // --- 以下は認証済み必須 ---
    if (!session.username) return send(ws, { type: 'error', text: '未認証です' });

    // --- ルーム作成 ---
    if (data.type === 'createRoom') {
      const { name, icon, password } = data;
      if (!name) return send(ws, { type: 'roomError', text: 'ルーム名を入力してください' });

      const code = genCode();
      const ph = password ? hash(password) : null;
      rooms.set(code, {
        code,
        name: name.trim().slice(0, 30),
        icon: icon || '💬',
        passwordHash: ph,
        members: [session.username],
        messages: [],
      });

      const u = users.get(session.username);
      if (!u.joinedRooms.includes(code)) u.joinedRooms.push(code);

      send(ws, { type: 'roomCreated', code, name: name.trim(), icon: icon || '💬' });
      return;
    }

    // --- ルーム参加 ---
    if (data.type === 'joinRoom') {
      const { code, password } = data;
      const room = rooms.get(code);
      if (!room) return send(ws, { type: 'roomError', text: 'ルームが見つかりません' });
      if (room.passwordHash && hash(password || '') !== room.passwordHash) {
        return send(ws, { type: 'roomError', text: 'パスワードが違います' });
      }

      if (!room.members.includes(session.username)) room.members.push(session.username);
      const u = users.get(session.username);
      if (!u.joinedRooms.includes(code)) u.joinedRooms.push(code);

      send(ws, { type: 'roomJoined', code, name: room.name, icon: room.icon });
      return;
    }

    // --- チャット画面に入る ---
    if (data.type === 'enterRoom') {
      const { code } = data;
      const room = rooms.get(code);
      if (!room) return send(ws, { type: 'error', text: 'ルームが見つかりません' });
      if (!room.members.includes(session.username)) return send(ws, { type: 'error', text: '参加していないルームです' });

      session.roomCode = code;

      // 履歴送信
      send(ws, { type: 'history', messages: room.messages.slice(-100) });

      const joinMsg = { type: 'system', text: `${session.username} さんが入室しました`, count: onlineCount(code) };
      send(ws, joinMsg);
      broadcastRoom(code, joinMsg, ws);
      return;
    }

    // --- 以下はルーム入室済み必須 ---
    if (!session.roomCode) return;
    const room = rooms.get(session.roomCode);
    if (!room) return;

    // --- メッセージ送信 ---
    if (data.type === 'message') {
      const text = (data.text || '').trim().slice(0, 500);
      if (!text) return;

      const now = new Date();
      const time = now.toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit' });
      const msg = {
        id: genMsgId(),
        type: 'message',
        username: session.username,
        text,
        time,
        replyTo: data.replyTo || null,  // { id, username, text }
        editHistory: [],
        recalled: false,
      };

      room.messages.push(msg);
      if (room.messages.length > 200) room.messages.shift();

      broadcastRoomAll(session.roomCode, msg);
      return;
    }

    // --- メッセージ編集 ---
    if (data.type === 'editMessage') {
      const { msgId, newText } = data;
      const msg = room.messages.find(m => m.id === msgId);
      if (!msg || msg.username !== session.username || msg.recalled) return;

      const trimmed = (newText || '').trim().slice(0, 500);
      if (!trimmed || trimmed === msg.text) return;

      msg.editHistory.push({ text: msg.text, editedAt: new Date().toISOString() });
      msg.text = trimmed;
      msg.edited = true;

      broadcastRoomAll(session.roomCode, {
        type: 'messageEdited',
        msgId,
        newText: trimmed,
        editHistory: msg.editHistory,
      });
      return;
    }

    // --- メッセージ取り消し ---
    if (data.type === 'recallMessage') {
      const { msgId } = data;
      const msg = room.messages.find(m => m.id === msgId);
      if (!msg || msg.username !== session.username || msg.recalled) return;

      msg.recalled = true;
      msg.text = '';

      broadcastRoomAll(session.roomCode, {
        type: 'messageRecalled',
        msgId,
        username: session.username,
      });
      return;
    }
  });

  ws.on('close', () => {
    const session = sessions.get(ws);
    if (session && session.username && session.roomCode) {
      broadcastRoom(session.roomCode, {
        type: 'system',
        text: `${session.username} さんが退室しました`,
        count: onlineCount(session.roomCode),
      }, ws);
    }
    sessions.delete(ws);
  });

  ws.on('error', () => ws.terminate());
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server on port ${PORT}`));
