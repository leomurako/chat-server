const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// 画像アップロードは1枚あたり最大6MB（base64化で約4.5MB相当の画像まで許容）
app.use(express.json({ limit: '6mb' }));
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_req, res) => res.send('ok'));

// ========== 画像アップロード（HTTP経由） ==========
const UPLOADS_DIR = path.join(__dirname, 'public', 'uploads');
if (!fs.existsSync(UPLOADS_DIR)) fs.mkdirSync(UPLOADS_DIR, { recursive: true });

const ALLOWED_IMAGE_TYPES = {
  'image/jpeg': 'jpg',
  'image/png': 'png',
  'image/gif': 'gif',
  'image/webp': 'webp',
};
const MAX_IMAGE_BYTES = 5 * 1024 * 1024; // 5MB

app.post('/upload', (req, res) => {
  try {
    const { dataUrl } = req.body || {};
    if (!dataUrl || typeof dataUrl !== 'string') {
      return res.status(400).json({ error: '画像データがありません' });
    }
    const match = dataUrl.match(/^data:(image\/[a-zA-Z+]+);base64,(.+)$/);
    if (!match) return res.status(400).json({ error: '画像形式が不正です' });

    const mime = match[1];
    const ext = ALLOWED_IMAGE_TYPES[mime];
    if (!ext) return res.status(400).json({ error: '対応していない画像形式です（jpg/png/gif/webpのみ）' });

    const base64 = match[2];
    const buffer = Buffer.from(base64, 'base64');
    if (buffer.length > MAX_IMAGE_BYTES) {
      return res.status(400).json({ error: '画像サイズが大きすぎます（5MBまで）' });
    }

    const filename = `${Date.now()}_${crypto.randomBytes(4).toString('hex')}.${ext}`;
    fs.writeFileSync(path.join(UPLOADS_DIR, filename), buffer);

    res.json({ url: `/uploads/${filename}` });
  } catch (e) {
    console.error('upload error:', e.message);
    res.status(500).json({ error: 'アップロードに失敗しました' });
  }
});

// ========== 永続化（JSONファイル） ==========
const DATA_DIR = path.join(__dirname, 'data');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const ROOMS_FILE = path.join(DATA_DIR, 'rooms.json');

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

function loadJSON(file, fallback) {
  try {
    if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (e) { console.error('load error', file, e.message); }
  return fallback;
}

let saveTimer = null;
function scheduleSave() {
  if (saveTimer) clearTimeout(saveTimer);
  saveTimer = setTimeout(saveAll, 1000); // 1秒後にまとめて保存
}

function saveAll() {
  ensureDataDir();
  try {
    fs.writeFileSync(USERS_FILE, JSON.stringify(Object.fromEntries(users), null, 2));
    const roomsObj = {};
    rooms.forEach((v, k) => { roomsObj[k] = v; });
    fs.writeFileSync(ROOMS_FILE, JSON.stringify(roomsObj, null, 2));
  } catch (e) { console.error('save error', e.message); }
}

// ========== インメモリストア（起動時にファイルから読み込む） ==========
ensureDataDir();
const users = new Map(Object.entries(loadJSON(USERS_FILE, {})));
const rooms = new Map(Object.entries(loadJSON(ROOMS_FILE, {})));
// messagesのreadByをSetに復元
rooms.forEach(room => {
  if (!room.messages) room.messages = [];
  room.messages.forEach(msg => {
    msg.readBy = new Set(Array.isArray(msg.readBy) ? msg.readBy : []);
    if (!msg.editHistory) msg.editHistory = [];
  });
});
const sessions = new Map(); // ws -> { username, roomCode }

console.log(`Loaded ${users.size} users, ${rooms.size} rooms`);

// ========== ユーティリティ ==========
function hashStr(str) {
  return crypto.createHash('sha256').update(str).digest('hex');
}
function genCode() {
  return crypto.randomBytes(3).toString('hex').toUpperCase();
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
function broadcastRoomAll(roomCode, data) { broadcastRoom(roomCode, data, null); }
function onlineInRoom(roomCode) {
  let c = 0;
  wss.clients.forEach(ws => {
    const s = sessions.get(ws);
    if (s && s.roomCode === roomCode && ws.readyState === WebSocket.OPEN) c++;
  });
  return c;
}

// メッセージをJSON送信用にシリアライズ（SetをArrayに）
function serializeMsg(msg) {
  return { ...msg, readBy: Array.from(msg.readBy || []) };
}

// ========== WSハンドラ ==========
wss.on('connection', ws => {
  sessions.set(ws, { username: null, roomCode: null });

  ws.on('message', raw => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }
    const session = sessions.get(ws);

    // ---- 認証 ----
    if (data.type === 'auth') {
      const username = (data.username || '').trim().slice(0, 20);
      const password = data.password || '';
      if (!username || !password) return send(ws, { type: 'authError', text: 'ユーザー名とパスワードを入力してください' });
      const ph = hashStr(password);

      if (data.isRegister) {
        if (users.has(username)) return send(ws, { type: 'authError', text: 'そのユーザー名はすでに使われています' });
        users.set(username, { passwordHash: ph, joinedRooms: [] });
        scheduleSave();
      } else {
        const u = users.get(username);
        if (!u) return send(ws, { type: 'authError', text: 'ユーザーが見つかりません' });
        if (u.passwordHash !== ph) return send(ws, { type: 'authError', text: 'パスワードが違います' });
      }

      session.username = username;
      const u = users.get(username);
      if (!u.lastRead) u.lastRead = {}; // { roomCode: timestamp }

      const joinedRoomList = (u.joinedRooms || []).map(code => {
        const r = rooms.get(code);
        if (!r) return null;
        const lastReadAt = u.lastRead[code] || 0;
        const unreadCount = (r.messages || []).filter(m =>
          !m.recalled && m.username !== username && (m.createdAt || 0) > lastReadAt
        ).length;
        return { code, name: r.name, icon: r.icon, unreadCount };
      }).filter(Boolean);

      send(ws, { type: 'authOk', username, joinedRooms: joinedRoomList });
      return;
    }

    if (!session.username) return send(ws, { type: 'error', text: '未認証です' });

    // ---- ルーム作成 ----
    if (data.type === 'createRoom') {
      const name = (data.name || '').trim().slice(0, 30);
      if (!name) return send(ws, { type: 'roomError', text: 'ルーム名を入力してください' });
      const code = genCode();
      const ph = data.password ? hashStr(data.password) : null;
      rooms.set(code, {
        code, name, icon: data.icon || '💬',
        passwordHash: ph,
        members: [session.username],
        messages: [],
      });
      const u = users.get(session.username);
      if (!u.joinedRooms.includes(code)) u.joinedRooms.push(code);
      scheduleSave();
      send(ws, { type: 'roomCreated', code, name, icon: data.icon || '💬' });
      return;
    }

    // ---- ルーム参加 ----
    if (data.type === 'joinRoom') {
      const code = (data.code || '').toUpperCase();
      const room = rooms.get(code);
      if (!room) return send(ws, { type: 'roomError', text: 'ルームが見つかりません' });
      if (room.passwordHash && hashStr(data.password || '') !== room.passwordHash)
        return send(ws, { type: 'roomError', text: 'パスワードが違います' });
      if (!room.members.includes(session.username)) room.members.push(session.username);
      const u = users.get(session.username);
      if (!u.joinedRooms.includes(code)) u.joinedRooms.push(code);
      scheduleSave();
      send(ws, { type: 'roomJoined', code, name: room.name, icon: room.icon });
      return;
    }

    // ---- チャット画面へ ----
    if (data.type === 'enterRoom') {
      const code = data.code;
      const room = rooms.get(code);
      if (!room) return send(ws, { type: 'error', text: 'ルームが見つかりません' });
      if (!room.members.includes(session.username)) return send(ws, { type: 'error', text: '参加していないルームです' });
      session.roomCode = code;

      // 既読位置を更新（このルームの未読を0にする）
      const u = users.get(session.username);
      if (u) {
        if (!u.lastRead) u.lastRead = {};
        u.lastRead[code] = Date.now();
        scheduleSave();
      }

      send(ws, { type: 'history', messages: room.messages.slice(-100).map(serializeMsg) });
      const sysMsg = { type: 'system', text: `${session.username} さんが入室しました`, count: onlineInRoom(code) };
      send(ws, sysMsg);
      broadcastRoom(code, sysMsg, ws);
      return;
    }

    // ---- ルーム退出（チャット画面を閉じてホームに戻る） ----
    if (data.type === 'leaveRoomView') {
      session.roomCode = null;
      return;
    }

    if (!session.roomCode) return;
    const room = rooms.get(session.roomCode);
    if (!room) return;

    // ---- メッセージ送信（テキスト or 画像） ----
    if (data.type === 'message') {
      const text = (data.text || '').trim().slice(0, 500);
      const imageUrl = typeof data.imageUrl === 'string' ? data.imageUrl : null;

      // テキストも画像もない場合は無視
      if (!text && !imageUrl) return;

      // imageUrlは自分のサーバーが発行した /uploads/ 配下のみ許可（不正な外部URL注入を防止）
      if (imageUrl && !/^\/uploads\/[a-zA-Z0-9_.\-]+$/.test(imageUrl)) return;

      const time = new Date().toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit' });
      const msg = {
        id: genMsgId(),
        type: 'message',
        username: session.username,
        text, time,
        createdAt: Date.now(),
        imageUrl,
        replyTo: data.replyTo || null,
        editHistory: [],
        recalled: false,
        readBy: new Set([session.username]),
      };
      room.messages.push(msg);
      if (room.messages.length > 200) room.messages.shift();

      // ルームの最終更新時刻を記録（未読バッジ計算用）
      room.lastMessageAt = Date.now();

      scheduleSave();
      broadcastRoomAll(session.roomCode, serializeMsg(msg));

      // このルームを「今開いていない」参加メンバーに未読バッジ更新を通知
      room.members.forEach(memberName => {
        if (memberName === session.username) return;
        wss.clients.forEach(clientWs => {
          const s = sessions.get(clientWs);
          if (s && s.username === memberName && s.roomCode !== session.roomCode && clientWs.readyState === WebSocket.OPEN) {
            send(clientWs, {
              type: 'unreadUpdate',
              roomCode: session.roomCode,
              roomName: room.name,
              roomIcon: room.icon,
              fromUser: session.username,
              preview: imageUrl ? '📷 画像' : text,
            });
          }
        });
      });

      // メッセージを送ったら、そのユーザー自身のタイピング状態は強制終了
      broadcastRoom(session.roomCode, { type: 'typing', username: session.username, isTyping: false }, ws);
      return;
    }

    // ---- タイピング中通知 ----
    if (data.type === 'typing') {
      broadcastRoom(session.roomCode, {
        type: 'typing',
        username: session.username,
        isTyping: !!data.isTyping,
      }, ws);
      return;
    }

    // ---- 既読 ----
    if (data.type === 'readMessage') {
      const { msgId } = data;
      const msg = room.messages.find(m => m.id === msgId);
      if (!msg || msg.recalled) return;
      if (!msg.readBy) msg.readBy = new Set();
      if (msg.readBy.has(session.username)) return; // 重複防止
      msg.readBy.add(session.username);
      scheduleSave();
      broadcastRoomAll(session.roomCode, {
        type: 'readUpdate',
        msgId,
        readBy: Array.from(msg.readBy),
        count: msg.readBy.size,
      });
      return;
    }

    // ---- 編集 ----
    if (data.type === 'editMessage') {
      const msg = room.messages.find(m => m.id === data.msgId);
      if (!msg || msg.username !== session.username || msg.recalled) return;
      const newText = (data.newText || '').trim().slice(0, 500);
      if (!newText || newText === msg.text) return;
      if (!msg.editHistory) msg.editHistory = [];
      msg.editHistory.push({ text: msg.text, editedAt: new Date().toISOString() });
      msg.text = newText;
      msg.edited = true;
      scheduleSave();
      broadcastRoomAll(session.roomCode, {
        type: 'messageEdited',
        msgId: data.msgId,
        newText,
        editHistory: msg.editHistory,
      });
      return;
    }

    // ---- 取り消し ----
    if (data.type === 'recallMessage') {
      const msg = room.messages.find(m => m.id === data.msgId);
      if (!msg || msg.username !== session.username || msg.recalled) return;
      msg.recalled = true;
      msg.text = '';
      scheduleSave();
      broadcastRoomAll(session.roomCode, {
        type: 'messageRecalled',
        msgId: data.msgId,
        username: session.username,
      });
      return;
    }
  });

  ws.on('close', () => {
    const s = sessions.get(ws);
    if (s && s.username && s.roomCode) {
      broadcastRoom(s.roomCode, {
        type: 'system',
        text: `${s.username} さんが退室しました`,
        count: onlineInRoom(s.roomCode),
      }, ws);
    }
    sessions.delete(ws);
  });

  ws.on('error', () => ws.terminate());
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server on port ${PORT}`));
