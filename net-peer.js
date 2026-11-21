/* ================= 联机：PeerJS 点对点（公共云只做握手撮合） =================
   房主的浏览器就是服务器：本地跑 Room（index.html 内联的 Room 模块），
   PeerJS DataConnection 只传消息；协议与 server/PROTOCOL.md 的 JSON 完全相同。
   房主：new Peer('chuiniu-<4位码>')；客人：new Peer() 后 peer.connect('chuiniu-<码>')。
   不用注册、不用绑卡、不用部署服务器。代价见 web/PEERJS-README.md。
   浏览器里定义全局 Net；Node 里 module.exports = Net（测试用）。
   测试覆盖：?peerhost=host:port[/path] 或环境变量 PEER_HOST，指向本地 PeerServer。 */
(function () {
'use strict';

var PEER_PREFIX = 'chuiniu-';
var SWEEP_MS = 1000;            // 房主扫表间隔（断线宽限 / 代打都靠它）
var JOIN_TIMEOUT_MS = 5000;     // 客人 join 无回应后报错
var HOST_SILENT_MS = 30000;     // 客人 30 秒收不到房主消息 → 房主离线
var CONNECT_TIMEOUT_MS = 15000; // 云握手超时（走重连）
var MAX_CODE_TRIES = 4;         // 房主初始建房 peer id 被占时换码重试次数
var MAX_ID_RETRIES = 5;         // 房主重连时同码被占（旧注册未释放）的重试次数

function peerOpts() {
  // 本地测试：?peerhost=localhost:9000 或 PEER_HOST=localhost:9000
  var raw = null;
  try {
    if (typeof window !== 'undefined' && window.location) {
      raw = new URLSearchParams(window.location.search).get('peerhost');
    }
  } catch (e) { /* 忽略 */ }
  if (!raw && typeof process !== 'undefined' && process.env) raw = process.env.PEER_HOST || null;
  if (!raw) return null;
  var parts = String(raw).split('/');
  var hp = parts[0].split(':');
  return {
    host: hp[0],
    port: hp[1] ? parseInt(hp[1], 10) : 9000,
    path: parts[1] ? '/' + parts[1] : '/',
    secure: false,
  };
}

function randomConnId() {
  return 'c-' + Math.random().toString(36).slice(2, 10);
}

function now() { return Date.now(); }

var Net = {
  status: 'idle', // idle | connecting | open | reconnecting | closed
  wantOpen: false,
  everOpened: false,
  // 房间上下文（setup 填入）
  code: null, name: '', playerId: null, isHost: false,
  connId: null,
  // 房主侧
  peer: null, room: null, sweepTimer: null, conns: null, // playerId -> DataConnection
  createTries: 0, idRetries: 0,
  // 客人侧
  conn: null, joined: false, everJoined: false, joinTimer: null,
  lastSeen: 0, hostWatchTimer: null, hostGoneFired: false,
  retryDelay: 1000, retryTimer: null, connectTimer: null,
  onMessage: null, // function(msg)：state/created/joined/error/host-gone
  onStatus: null,  // function(status)
  onOpen: null,    // function()，每次房间会话建立时调用（含重连）

  setStatus: function (s) {
    this.status = s;
    if (this.onStatus) this.onStatus(s);
  },
  setPath: function (path) { /* 兼容旧接口：PeerJS 版无路径概念 */ },

  setup: function (opts) {
    this.code = opts.code || null;
    this.name = opts.name || '';
    this.playerId = opts.playerId;
    this.isHost = !!opts.isHost;
    this.connId = randomConnId();
    this.joined = false;
    this.everJoined = false;
    this.hostGoneFired = false;
    this.room = null;
    this.conns = {};
    this.createTries = 0;
    this.idRetries = 0;
  },

  connect: function () {
    this.wantOpen = true;
    this.retryDelay = 1000;
    this._clearRetryTimer();
    if (this.isHost) this._openHost();
    else this._openGuest();
  },

  _clearRetryTimer: function () {
    if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = null; }
  },
  _clearConnectTimer: function () {
    if (this.connectTimer) { clearTimeout(this.connectTimer); this.connectTimer = null; }
  },
  _clearJoinTimer: function () {
    if (this.joinTimer) { clearTimeout(this.joinTimer); this.joinTimer = null; }
  },
  _clearHostWatch: function () {
    if (this.hostWatchTimer) { clearInterval(this.hostWatchTimer); this.hostWatchTimer = null; }
  },
  _clearSweep: function () {
    if (this.sweepTimer) { clearInterval(this.sweepTimer); this.sweepTimer = null; }
  },
  _destroyPeer: function (peer) {
    try { if (peer) peer.destroy(); } catch (e) { /* 忽略 */ }
    if (this.peer === peer) this.peer = null;
  },

  _scheduleReconnect: function () {
    var self = this;
    if (!this.wantOpen || this.retryTimer) return;
    this.setStatus('reconnecting');
    this.retryTimer = setTimeout(function () {
      self.retryTimer = null;
      if (!self.wantOpen) return;
      if (self.isHost) self._openHost();
      else self._openGuest();
    }, this.retryDelay);
    this.retryDelay = Math.min(this.retryDelay * 2, 10000);
  },

  // ---- 房主 ----
  _openHost: function () {
    var self = this;
    if (!this.wantOpen) return;
    if (this.peer) this._destroyPeer(this.peer);
    this.setStatus(this.everOpened ? 'reconnecting' : 'connecting');
    this.createTries++;
    var opts = peerOpts() || {};
    var peer = new Peer(PEER_PREFIX + this.code, opts);
    this.peer = peer;
    this._clearConnectTimer();
    this.connectTimer = setTimeout(function () {
      if (self.peer === peer) self._onHostPeerStall(peer);
    }, CONNECT_TIMEOUT_MS);
    peer.on('open', function (id) { self._onHostOpen(peer, id); });
    peer.on('connection', function (conn) { self._onHostConnection(conn); });
    peer.on('disconnected', function () { self._onHostDisconnected(peer); });
    peer.on('error', function (err) { self._onHostPeerError(peer, err); });
  },
  _onHostPeerStall: function (peer) {
    // 握手 15 秒没 open：当作断线处理，走重连
    if (this.peer !== peer) return;
    this._destroyPeer(peer);
    this._scheduleReconnect();
  },
  _onHostDisconnected: function (peer) {
    // 信令断了，P2P 数据连接本身还活着：先原 id 重连，别动牌桌
    if (this.peer !== peer || !this.wantOpen) return;
    this.setStatus('reconnecting');
    try { peer.reconnect(); } catch (e) {
      this._destroyPeer(peer);
      this._scheduleReconnect();
    }
  },
  _onHostOpen: function (peer, id) {
    if (this.peer !== peer) return;
    this._clearConnectTimer();
    this.everOpened = true;
    this.retryDelay = 1000;
    this.idRetries = 0;
    this.setStatus('open');
    if (!this.room) {
      var self = this;
      var hooks = {
        send: function (playerId, obj) {
          if (playerId === self.playerId) {
            if (self.onMessage) self.onMessage(obj);
            return;
          }
          var conn = self.conns[playerId];
          if (conn && conn.open) {
            try { conn.send(obj); } catch (e) { /* 忽略 */ }
          }
        },
        close: function (playerId, connId) {
          // 顶号：关掉旧会话的连接
          var conn = self.conns[playerId];
          if (conn && conn.open) {
            try { conn.close(); } catch (e) { /* 忽略 */ }
          }
        },
      };
      this.room = new Room(this.code, hooks);
      this.room.create(this.name, this.playerId, this.connId);
    } else {
      this.room.broadcast();
    }
    this._startSweep();
    if (this.onOpen) this.onOpen();
  },
  _onHostPeerError: function (peer, err) {
    if (this.peer !== peer) return;
    var type = err && err.type;
    if (type === 'unavailable-id') {
      this._clearConnectTimer();
      this._destroyPeer(peer);
      if (this.room) {
        // 房间已开：旧注册可能还没释放，同码重试，别换码（换码会丢掉整桌）
        if (this.idRetries < MAX_ID_RETRIES) {
          this.idRetries++;
          var self = this;
          setTimeout(function () { if (self.wantOpen) self._openHost(); }, 1000);
        } else {
          this._scheduleReconnect();
        }
      } else if (this.createTries < MAX_CODE_TRIES) {
        // 初始建房：房码已被别人占用，换个码重试（UI 经 'created' 拿到新码）
        this.code = generateCode();
        this._openHost();
      } else {
        if (this.onMessage) {
          this.onMessage({ type: 'error', code: 'CODE_TAKEN', message: '房间号被占用，请重试' });
        }
        this.close();
      }
      return;
    }
    // 网络 / 服务器错误：重建 peer（同码，房间已开则走同码重试）
    if (type === 'network' || type === 'server-error' ||
        type === 'socket-error' || type === 'socket-closed') {
      this._clearConnectTimer();
      this._destroyPeer(peer);
      this._scheduleReconnect();
    }
    // 其余错误（如 browser-incompatible）不自动重连，等 UI 处理
  },
  _onHostConnection: function (conn) {
    var self = this;
    conn.on('data', function (data) { self._onHostData(conn, data); });
    conn.on('close', function () { self._onHostConnClose(conn); });
    conn.on('error', function () { /* close 会跟进处理 */ });
  },
  _onHostData: function (conn, data) {
    if (!this.room || !data || typeof data.type !== 'string') return;
    if (data.playerId) {
      conn._playerId = data.playerId;
      conn._connId = data.connId || null;
      this.conns[data.playerId] = conn;
    }
    this.room.onMessage(data.playerId, data.connId, data);
  },
  _onHostConnClose: function (conn) {
    if (conn._playerId && this.conns[conn._playerId] === conn) {
      delete this.conns[conn._playerId];
    }
    if (conn._playerId && this.room) {
      this.room.onDisconnect(conn._playerId, conn._connId);
    }
  },
  _startSweep: function () {
    var self = this;
    if (this.sweepTimer) return;
    this.sweepTimer = setInterval(function () {
      if (self.room) self.room.sweep(); // 断线宽限到期 / 代打（广播在内）
    }, SWEEP_MS);
  },

  // ---- 客人 ----
  _openGuest: function () {
    var self = this;
    if (!this.wantOpen) return;
    if (this.peer) this._destroyPeer(this.peer);
    this.setStatus(this.everOpened ? 'reconnecting' : 'connecting');
    var opts = peerOpts() || {};
    var peer = new Peer(opts);
    this.peer = peer;
    this._clearConnectTimer();
    this.connectTimer = setTimeout(function () {
      if (self.peer === peer) self._onGuestPeerStall(peer);
    }, CONNECT_TIMEOUT_MS);
    peer.on('open', function (id) { self._onGuestOpen(peer, id); });
    peer.on('disconnected', function () { self._onGuestDisconnected(peer); });
    peer.on('error', function (err) { self._onGuestPeerError(peer, err); });
  },
  _onGuestPeerStall: function (peer) {
    if (this.peer !== peer) return;
    this._destroyPeer(peer);
    this._scheduleReconnect();
  },
  _onGuestDisconnected: function (peer) {
    // 信令断了，P2P 数据连接本身还活着：先原 id 重连，别动牌桌
    if (this.peer !== peer || !this.wantOpen) return;
    this.setStatus('reconnecting');
    try { peer.reconnect(); } catch (e) {
      this._destroyPeer(peer);
      this._scheduleReconnect();
    }
  },
  _onGuestOpen: function (peer, id) {
    if (this.peer !== peer) return;
    this._clearConnectTimer();
    this.everOpened = true;
    this.retryDelay = 1000;
    if (!this.conn || !this.conn.open) {
      this._connectToHost();
    } else {
      this.setStatus('open');
    }
  },
  _connectToHost: function () {
    var self = this;
    if (!this.peer || !this.wantOpen) return;
    var conn;
    try {
      conn = this.peer.connect(PEER_PREFIX + this.code, {
        reliable: true,
        metadata: { playerId: this.playerId },
      });
    } catch (e) {
      this._failJoin('连接失败，请重试');
      return;
    }
    this.conn = conn;
    conn.on('open', function () {
      if (self.conn !== conn) return;
      self.lastSeen = now();
      self.setStatus('open');
      self._sendJoin();
      self._startHostWatch();
      if (self.onOpen) self.onOpen();
    });
    conn.on('data', function (data) { self._onGuestData(data); });
    conn.on('close', function () { self._onGuestConnClose(conn); });
    conn.on('error', function (err) {
      if (err && err.type === 'peer-unavailable' && !self.joined) {
        self._failJoin('房间不存在或已过期');
      }
    });
  },
  _onGuestPeerError: function (peer, err) {
    if (this.peer !== peer) return;
    var type = err && err.type;
    if (type === 'peer-unavailable' && !this.joined) {
      this._clearConnectTimer();
      this._failJoin('房间不存在或已过期');
      return;
    }
    if (type === 'network' || type === 'server-error' ||
        type === 'socket-error' || type === 'socket-closed') {
      this._clearConnectTimer();
      this._destroyPeer(peer);
      this._scheduleReconnect();
    }
  },
  _sendJoin: function () {
    var self = this;
    if (!this.wantOpen || this.joined || !this.conn || !this.conn.open) return;
    try {
      this.conn.send({ type: 'join', name: this.name, playerId: this.playerId, connId: this.connId });
    } catch (e) { /* 忽略 */ }
    this._clearJoinTimer();
    this.joinTimer = setTimeout(function () {
      if (!self.wantOpen || self.joined) return;
      self._failJoin('加入超时，请重试');
    }, JOIN_TIMEOUT_MS);
  },
  _onGuestData: function (data) {
    if (!data || typeof data.type !== 'string') return;
    if (data.type === 'state') {
      this.lastSeen = now();
      this.joined = true;
      this.everJoined = true;
      this._clearJoinTimer();
      if (this.onMessage) this.onMessage(data);
      return;
    }
    if (data.type === 'joined') {
      this.lastSeen = now();
      this.joined = true;
      this.everJoined = true;
      this._clearJoinTimer();
      if (this.onMessage) this.onMessage(data);
      return;
    }
    this.lastSeen = now();
    if (this.onMessage) this.onMessage(data);
  },
  _onGuestConnClose: function (conn) {
    if (this.conn !== conn) return;
    this.conn = null;
    this.joined = false; // 重连后要重新发 join（同一 playerId 恢复座位）
    this._clearJoinTimer();
    if (!this.wantOpen) {
      this.setStatus('closed');
      return;
    }
    // 数据连接断了：重建 peer 再连（同一 playerId 恢复座位）；
    // 房主已不在时重连会报 ROOM_NOT_FOUND（进过房的报 host-gone）
    this._destroyPeer(this.peer);
    this._scheduleReconnect();
  },
  _startHostWatch: function () {
    var self = this;
    if (this.hostWatchTimer) return;
    this.hostWatchTimer = setInterval(function () {
      // 只在已连接且 30 秒无房主消息时判离（重连中不触发误判）
      if (self.status !== 'open' || self.hostGoneFired) return;
      if (now() - self.lastSeen > HOST_SILENT_MS) {
        self.hostGoneFired = true;
        if (self.onMessage) self.onMessage({ type: 'host-gone' });
      }
    }, 5000);
  },
  _failJoin: function (message) {
    if (!this.wantOpen) return;
    if (this.everJoined) {
      // 曾经进过房，现在连不上房主：房主走了
      if (this.onMessage) this.onMessage({ type: 'host-gone' });
    } else if (this.onMessage) {
      this.onMessage({ type: 'error', code: 'ROOM_NOT_FOUND', message: message });
    }
    this.close();
  },

  // ---- 通用 ----
  send: function (obj) {
    if (!this.wantOpen) return false;
    if (this.isHost && this.room) {
      // 房主的动作直接走本地 Room（不绕网络，零延迟）
      this.room.onMessage(this.playerId, this.connId, obj);
      return true;
    }
    if (this.conn && this.conn.open) {
      // 房主按消息里的 playerId/connId 认人：每条消息都带上
      var m = { playerId: this.playerId, connId: this.connId };
      for (var k in obj) m[k] = obj[k];
      try { this.conn.send(m); return true; } catch (e) { return false; }
    }
    return false;
  },
  close: function () {
    this.wantOpen = false;
    this._clearRetryTimer();
    this._clearConnectTimer();
    this._clearJoinTimer();
    this._clearHostWatch();
    this._clearSweep();
    this.room = null;
    this.conns = {};
    this.conn = null;
    if (this.peer) this._destroyPeer(this.peer);
    this.setStatus('closed');
  },
};

if (typeof module !== 'undefined' && module.exports) module.exports = Net;
else if (typeof window !== 'undefined') window.Net = Net;

})();
