(() => {
  const $ = id => document.getElementById(id);

  // --- DOM要素 ---
  const loginScreen   = $('login-screen');
  const chatScreen    = $('chat-screen');
  const usernameInput = $('username-input');
  const joinBtn       = $('join-btn');
  const loginError    = $('login-error');
  const messageList   = $('message-list');
  const messageInput  = $('message-input');
  const sendBtn       = $('send-btn');
  const onlineCount   = $('online-count');

  let ws = null;
  let myUsername = '';

  // --- ログイン処理 ---
  function join() {
    const name = usernameInput.value.trim();
    if (!name) {
      loginError.textContent = 'ニックネームを入力してください';
      return;
    }
    loginError.textContent = '';
    myUsername = name;
    connectWebSocket();
  }

  joinBtn.addEventListener('click', join);
  usernameInput.addEventListener('keydown', e => {
    if (e.key === 'Enter') join();
  });

  // --- WebSocket接続 ---
  function connectWebSocket() {
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${protocol}//${location.host}`;
    ws = new WebSocket(url);

    ws.addEventListener('open', () => {
      // 入室メッセージを送る
      ws.send(JSON.stringify({ type: 'join', username: myUsername }));

      // チャット画面へ切り替え
      loginScreen.classList.add('hidden');
      chatScreen.classList.remove('hidden');
      messageInput.focus();
    });

    ws.addEventListener('message', e => {
      const data = JSON.parse(e.data);
      handleMessage(data);
    });

    ws.addEventListener('close', () => {
      showBanner('接続が切れました。ページを再読み込みしてください。');
    });

    ws.addEventListener('error', () => {
      loginError.textContent = 'サーバーに接続できませんでした';
      ws = null;
    });
  }

  // --- メッセージ処理 ---
  function handleMessage(data) {
    if (data.type === 'history') {
      // 過去ログを一括表示
      data.messages.forEach(msg => renderMessage(msg));
      scrollToBottom();
      return;
    }

    if (data.type === 'system') {
      renderSystem(data.text);
      if (data.count !== undefined) {
        onlineCount.textContent = data.count;
      }
      scrollToBottom();
      return;
    }

    if (data.type === 'message') {
      renderMessage(data);
      scrollToBottom();
      return;
    }
  }

  // --- チャットバブルの描画 ---
  function renderMessage(data) {
    const isSelf = data.username === myUsername;

    const row = document.createElement('div');
    row.className = `msg-row ${isSelf ? 'self' : 'other'}`;

    if (!isSelf) {
      const avatar = document.createElement('div');
      avatar.className = 'msg-avatar';
      avatar.textContent = data.username.charAt(0);
      row.appendChild(avatar);
    }

    const group = document.createElement('div');
    group.className = 'msg-group';

    if (!isSelf) {
      const nameEl = document.createElement('div');
      nameEl.className = 'msg-username';
      nameEl.textContent = data.username;
      group.appendChild(nameEl);
    }

    const bubble = document.createElement('div');
    bubble.className = 'msg-bubble';
    bubble.textContent = data.text;  // textContentでXSS防止
    group.appendChild(bubble);

    if (data.time) {
      const time = document.createElement('div');
      time.className = 'msg-time';
      time.textContent = data.time;
      group.appendChild(time);
    }

    row.appendChild(group);
    messageList.appendChild(row);
  }

  // --- システムメッセージの描画 ---
  function renderSystem(text) {
    const el = document.createElement('div');
    el.className = 'msg-system';
    el.textContent = text;
    messageList.appendChild(el);
  }

  // --- メッセージ送信 ---
  function sendMessage() {
    const text = messageInput.value.trim();
    if (!text || !ws || ws.readyState !== WebSocket.OPEN) return;

    ws.send(JSON.stringify({ type: 'message', text }));
    messageInput.value = '';
    messageInput.focus();
  }

  sendBtn.addEventListener('click', sendMessage);
  messageInput.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  });

  // --- ユーティリティ ---
  function scrollToBottom() {
    messageList.scrollTop = messageList.scrollHeight;
  }

  function showBanner(text) {
    const banner = document.createElement('div');
    banner.className = 'banner';
    banner.textContent = text;
    chatScreen.insertBefore(banner, chatScreen.firstChild);
  }
})();
