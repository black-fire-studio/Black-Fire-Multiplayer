/* =====================================================================
   BLACK FIRE — Servidor Multiplayer (Modo Bomba / P&D)
   -----------------------------------------------------------------
   ESTA É UMA EVOLUÇÃO do servidor que já funciona (WebSocket + rounds
   + bomba já testados entre dois dispositivos reais). A lógica de
   ROUND/BOMBA (startRound/endRound/tickRound/aliveCount/teamCount/
   spawnFor) foi mantida INTACTA — só a identidade do jogador e a
   estrutura ao redor dela (servidor → canal → sala) mudaram, pra dar
   suporte a contagem real, host dinâmico, pronto/iniciar e reconexão.

   HIERARQUIA:
   Jogador -> 1 Servidor -> 1 Canal -> 0 ou 1 Sala -> (dentro da sala) Partida

   Nada aqui usa números falsos: toda contagem exibida vem de contar de
   verdade quantos jogadores conectados estão em cada servidor/canal/sala.
   ===================================================================== */

const { WebSocketServer } = require("ws");
const http = require("http");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;

// Rede de segurança: sem isso, qualquer erro inesperado em qualquer lugar do
// código derrubaria o processo Node inteiro (e junto, todas as salas e
// jogadores em memória — todo mundo cairia sem aviso). Com isso, o erro só é
// logado e o servidor continua rodando.
process.on("uncaughtException", (err) => console.error("[FATAL EVITADO] uncaughtException:", err));
process.on("unhandledRejection", (err) => console.error("[FATAL EVITADO] unhandledRejection:", err));

/* ---------------------- REGRAS DE PARTIDA (P&D) — INTOCADAS ---------------------- */
const ROUND_TIME = 180;        // segundos por round
const ROUNDS_TO_WIN = 6;       // primeiro a 6 rounds vence a partida
const BOMB_PLANT_TIME = 2.5;   // segundos pra plantar
const BOMB_DEFUSE_TIME = 5;    // segundos pra desarmar
const BOMB_FUSE_TIME = 40;     // segundos até explodir depois de plantada
const BOMB_SITE = { x: 0, z: -10, radius: 2.4 };

const TICK_MS = 200;           // lógica de round (timer, elimina, bomba)
const STATE_BROADCAST_MS = 80; // posição/vida de todo mundo, dentro da partida

/* ---------------------- CONFIGURAÇÃO DA INFRAESTRUTURA (servidor/canal/sala) ----------------------
   Números centralizados aqui — nunca espalhados pelo código. */
const SERVER_MAX_PLAYERS = 100;     // capacidade de UM servidor (soma de todos os canais dele)
const CHANNELS_PER_SERVER = 10;     // cada servidor tem 10 canais
const CHANNEL_MAX_PLAYERS = SERVER_MAX_PLAYERS; // um canal nunca passa disso NA PRÁTICA, porque o
                                     // próprio servidor já rejeita entrada ao chegar em 100 no total
                                     // (ver JOIN_SERVER) — então os dois nunca ficam inconsistentes.
const ROOM_MAX_PLAYERS = 16;        // 8x8
const ROOM_TEAM_MAX = 8;
const MIN_PLAYERS_TO_START = 2;     // configurável — não é regra fixa do jogo, é só o mínimo técnico
                                     // pra iniciar uma partida (ajuste aqui se quiser exigir mais).
const RECONNECT_GRACE_MS = 20000;   // tempo que um jogador desconectado "segura o lugar" antes de
                                     // ser removido de vez (servidor/canal/sala) — evita jogador fantasma
                                     // sem punir quem só teve uma queda de conexão rápida.

const SERVER_DEFS = [
  "Cadetes", "Fogo Cruzado", "Tropa de Elite", "Forças Especiais", "Operação Sombria",
  "Linha de Frente", "Comando de Guerra", "Zona de Combate", "Alto Comando", "Operação Black Fire"
].map((name, i) => ({ id: "srv" + (i + 1), name }));

/* ---------------------- ESTADO EM MEMÓRIA (fonte única da verdade) ---------------------- */
// players: sessionToken -> player (o sessionToken sobrevive a uma reconexão; o ws muda a cada conexão)
const players = new Map();
// rooms: roomId -> room (salas dentro de um canal — viram partida quando status vira IN_PROGRESS)
const rooms = new Map();

function newPlayer(sessionToken, ws, nick) {
  return {
    sessionToken, ws, nick,
    connectionState: "connected", disconnectedAt: null,
    serverId: null, channelId: null, roomId: null,
    ready: false, isHostOf: null,
    // campos usados dentro de uma partida (mantidos com os mesmos nomes de antes)
    team: null, x: 0, y: 1.7, z: 0, yaw: 0, pitch: 0,
    hp: 100, alive: true, kills: 0, deaths: 0,
    connected: true, // usado pelas funções de round (aliveCount/teamCount) — ver seção de rounds
    joinedAt: Date.now(), lastSeen: Date.now()
  };
}
function newRoom(roomId, channelId, name, hostId, mode, map) {
  return {
    id: roomId, channelId, name, hostId, mode: mode || "P&D", map: map || "Distrito Industrial",
    maxPlayers: ROOM_MAX_PLAYERS, status: "WAITING", createdAt: Date.now(),
    players: new Map(), // playerId -> { id, roomJoinedAt } (dados completos ficam em `players`)
    round: {
      active: false, roundActive: false, num: 0, scoreBL: 0, scoreGR: 0,
      roundTimeLeft: ROUND_TIME,
      bomb: { planted: false, timer: BOMB_FUSE_TIME, plantProgress: 0, defuseProgress: 0 },
      lastPlantTickAt: 0, lastDefuseTickAt: 0
    }
  };
}

function genId(prefix) { return prefix + Date.now().toString(36) + Math.random().toString(36).slice(2, 7); }
function send(ws, obj) { if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj)); }

/* ---------------------- CONTAGENS REAIS (nunca inventadas) ---------------------- */
function playersInServer(serverId) {
  var list = [];
  for (const p of players.values()) if (p.serverId === serverId && p.connectionState !== "removed") list.push(p);
  return list;
}
function playersInChannel(channelId) {
  var list = [];
  for (const p of players.values()) if (p.channelId === channelId && p.connectionState !== "removed") list.push(p);
  return list;
}
function serverListSnapshot() {
  return SERVER_DEFS.map(function (s) {
    var current = playersInServer(s.id).length;
    return { id: s.id, name: s.name, current: current, max: SERVER_MAX_PLAYERS, status: current >= SERVER_MAX_PLAYERS ? "CHEIO" : "LIVRE" };
  });
}
function channelListSnapshot(serverId) {
  var out = [];
  for (var i = 1; i <= CHANNELS_PER_SERVER; i++) {
    var channelId = serverId + "_ch" + i; // globalmente único — evita colidir com "Canal 1" de outro servidor
    var current = playersInChannel(channelId).length;
    out.push({ id: channelId, num: i, name: "Black Fire - Canal " + i, current: current, max: CHANNEL_MAX_PLAYERS, status: current >= CHANNEL_MAX_PLAYERS ? "CHEIO" : "LIVRE" });
  }
  return out;
}
function roomListSnapshot(serverId, channelId) {
  var out = [];
  for (const room of rooms.values()) {
    if (room.channelId !== channelId) continue;
    var current = countConnectedInRoom(room);
    out.push({ id: room.id, name: room.name, mode: room.mode, map: room.map, current: current, max: room.maxPlayers, status: room.status });
  }
  return out;
}
function countConnectedInRoom(room) {
  var n = 0;
  for (const pid of room.players.keys()) {
    var p = players.get(pid);
    if (p && p.connectionState !== "removed") n++;
  }
  return n;
}
function teamCountInRoom(room, team) {
  var n = 0;
  for (const pid of room.players.keys()) {
    var p = players.get(pid);
    if (p && p.connectionState !== "removed" && p.team === team) n++;
  }
  return n;
}

/* ---------------------- BROADCAST ---------------------- */
function broadcastToChannel(channelId, obj, exceptToken) {
  var msg = JSON.stringify(obj);
  for (const p of playersInChannel(channelId)) {
    if (p.sessionToken === exceptToken) continue;
    if (p.ws && p.ws.readyState === p.ws.OPEN) p.ws.send(msg);
  }
}
function broadcastToRoom(room, obj, exceptToken) {
  var msg = JSON.stringify(obj);
  for (const pid of room.players.keys()) {
    var p = players.get(pid);
    if (!p || p.sessionToken === exceptToken) continue;
    if (p.ws && p.ws.readyState === p.ws.OPEN) p.ws.send(msg);
  }
}

function publicPlayer(p) {
  return {
    id: p.sessionToken, nick: p.nick, team: p.team,
    x: p.x, y: p.y, z: p.z, yaw: p.yaw, pitch: p.pitch,
    hp: p.hp, alive: p.alive, kills: p.kills, deaths: p.deaths,
    ready: p.ready, connected: p.connectionState === "connected"
  };
}
function roomSnapshot(room) {
  var playerList = [];
  for (const pid of room.players.keys()) {
    var p = players.get(pid);
    if (p) playerList.push(publicPlayer(p));
  }
  return {
    id: room.id, name: room.name, mode: room.mode, map: room.map,
    maxPlayers: room.maxPlayers, status: room.status, hostId: room.hostId,
    players: playerList,
    round: {
      active: room.round.active, roundActive: room.round.roundActive, num: room.round.num,
      scoreBL: room.round.scoreBL, scoreGR: room.round.scoreGR,
      roundTimeLeft: room.round.roundTimeLeft, bomb: room.round.bomb
    }
  };
}

/* ---------------------- HOST DINÂMICO ---------------------- */
function pickNewHost(room) {
  // jogador conectado há mais tempo DENTRO DA SALA vira o novo host — nunca aleatório.
  var best = null;
  for (const pid of room.players.keys()) {
    var p = players.get(pid);
    if (!p || p.connectionState === "removed") continue;
    var meta = room.players.get(pid);
    if (!best || meta.roomJoinedAt < room.players.get(best).roomJoinedAt) best = pid;
  }
  room.hostId = best; // null se a sala ficou vazia
  return best;
}

/* ---------------------- CICLO DE VIDA DA SALA ---------------------- */
function destroyRoomIfEmpty(room) {
  if (countConnectedInRoom(room) > 0) return false;
  rooms.delete(room.id);
  broadcastToChannel(room.channelId, { type: "ROOM_REMOVED", roomId: room.id });
  return true;
}

function removePlayerFromRoom(p, notify) {
  if (!p.roomId) return;
  var room = rooms.get(p.roomId);
  p.roomId = null; p.ready = false; p.team = null;
  if (!room) return;
  room.players.delete(p.sessionToken);

  if (destroyRoomIfEmpty(room)) return;

  var hostChanged = false;
  if (room.hostId === p.sessionToken) {
    pickNewHost(room);
    hostChanged = true;
  }
  if (notify) {
    broadcastToRoom(room, { type: "PLAYER_LEFT", roomId: room.id, id: p.sessionToken, snapshot: roomSnapshot(room) });
    if (hostChanged) broadcastToRoom(room, { type: "HOST_CHANGED", roomId: room.id, hostId: room.hostId });
  }
  broadcastToChannel(room.channelId, { type: "ROOM_UPDATED", room: { id: room.id, name: room.name, mode: room.mode, map: room.map, current: countConnectedInRoom(room), max: room.maxPlayers, status: room.status } });
}

function leaveChannel(p, notify) {
  removePlayerFromRoom(p, notify);
  if (p.serverId && p.channelId && notify) {
    broadcastToChannel(p.channelId, { type: "PLAYER_LEFT_CHANNEL", id: p.sessionToken }, p.sessionToken);
  }
  p.channelId = null;
}
function leaveServer(p, notify) {
  leaveChannel(p, notify);
  p.serverId = null;
}

/* ---------------------- ROUNDS / BOMBA — LÓGICA INTOCADA ---------------------- */
function spawnFor(team) {
  return team === "BL"
    ? { x: (Math.random() * 6 - 3), y: 1.7, z: 8 + Math.random() * 4 }
    : { x: (Math.random() * 6 - 3), y: 1.7, z: -18 - Math.random() * 4 };
}
function aliveCount(room, team) {
  var n = 0;
  for (const pid of room.players.keys()) {
    var p = players.get(pid);
    if (p && p.connected && p.alive && p.team === team) n++;
  }
  return n;
}
function teamCount(room, team) {
  var n = 0;
  for (const pid of room.players.keys()) {
    var p = players.get(pid);
    if (p && p.connected && p.team === team) n++;
  }
  return n;
}

function startRound(room) {
  const r = room.round;
  r.num++;
  r.roundActive = true;
  r.roundTimeLeft = ROUND_TIME;
  r.bomb = { planted: false, timer: BOMB_FUSE_TIME, plantProgress: 0, defuseProgress: 0 };
  r.lastPlantTickAt = 0; r.lastDefuseTickAt = 0;

  for (const pid of room.players.keys()) {
    var p = players.get(pid);
    if (!p || !p.connected) continue;
    p.hp = 100; p.alive = true;
    const sp = spawnFor(p.team);
    p.x = sp.x; p.y = sp.y; p.z = sp.z; p.yaw = 0; p.pitch = 0;
  }
  broadcastToRoom(room, { type: "roundStart", roomId: room.id, round: r.num, scoreBL: r.scoreBL, scoreGR: r.scoreGR, players: roomSnapshot(room).players });
}

function endRound(room, winner, reason) {
  const r = room.round;
  if (!r.roundActive) return;
  r.roundActive = false;
  if (winner === "BL") r.scoreBL++; else r.scoreGR++;
  broadcastToRoom(room, { type: "roundEnd", roomId: room.id, winner, reason, scoreBL: r.scoreBL, scoreGR: r.scoreGR });

  if (r.scoreBL >= ROUNDS_TO_WIN || r.scoreGR >= ROUNDS_TO_WIN) {
    const matchWinner = r.scoreBL >= ROUNDS_TO_WIN ? "BL" : "GR";
    r.active = false;
    room.status = "FINISHED";
    broadcastToRoom(room, { type: "matchEnd", roomId: room.id, winner: matchWinner, scoreBL: r.scoreBL, scoreGR: r.scoreGR });
    broadcastToChannel(room.channelId, { type: "ROOM_UPDATED", room: { id: room.id, name: room.name, mode: room.mode, map: room.map, current: countConnectedInRoom(room), max: room.maxPlayers, status: room.status } });
  } else {
    setTimeout(() => { if (rooms.get(room.id) === room && r.active !== false) startRound(room); }, 3500);
  }
}

function tickRound(room) {
  const r = room.round;
  if (!r.active || !r.roundActive) return;

  const blAlive = aliveCount(room, "BL");
  const grAlive = aliveCount(room, "GR");
  const blTotal = teamCount(room, "BL");
  const grTotal = teamCount(room, "GR");

  if (blTotal > 0 && blAlive === 0) { endRound(room, "GR", "elimination"); return; }
  if (grTotal > 0 && grAlive === 0) { endRound(room, "BL", "elimination"); return; }

  const dtSec = TICK_MS / 1000;
  const now = Date.now();

  if (!r.bomb.planted) {
    r.roundTimeLeft -= dtSec;
    if (now - r.lastPlantTickAt > TICK_MS * 1.5) r.bomb.plantProgress = Math.max(0, r.bomb.plantProgress - dtSec * 2);
    if (r.bomb.plantProgress >= BOMB_PLANT_TIME) {
      r.bomb.planted = true; r.bomb.timer = BOMB_FUSE_TIME;
      broadcastToRoom(room, { type: "bombPlanted", roomId: room.id });
    }
    if (r.roundTimeLeft <= 0) { endRound(room, "GR", "timeout"); return; }
  } else {
    r.bomb.timer -= dtSec;
    if (now - r.lastDefuseTickAt > TICK_MS * 1.5) r.bomb.defuseProgress = Math.max(0, r.bomb.defuseProgress - dtSec * 1.5);
    if (r.bomb.defuseProgress >= BOMB_DEFUSE_TIME) { endRound(room, "GR", "defuse"); return; }
    if (r.bomb.timer <= 0) { endRound(room, "BL", "explode"); return; }
  }
}

/* ---------------------- LIMPEZA DEFINITIVA (fim do grace period de reconexão) ---------------------- */
function fullyRemovePlayer(p) {
  p.connectionState = "removed";
  removePlayerFromRoom(p, true);
  if (p.serverId && p.channelId) broadcastToChannel(p.channelId, { type: "PLAYER_LEFT_CHANNEL", id: p.sessionToken }, p.sessionToken);
  players.delete(p.sessionToken);
}

/* ---------------------- HTTP + WEBSOCKET ---------------------- */
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("BLACK FIRE multiplayer server ativo. Conecte via WebSocket.");
});
httpServer.on("error", (err) => console.error("[HTTP] erro no servidor:", err));
httpServer.on("upgrade", (req) => console.log("[UPGRADE] pedido de handshake WebSocket recebido:", req.url));

const wss = new WebSocketServer({ server: httpServer });
wss.on("error", (err) => console.error("[WSS] erro no servidor WebSocket:", err));

wss.on("connection", (ws, req) => {
  console.log("[WSS] cliente conectado:", req.socket.remoteAddress);
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  let me = null; // player atual desta conexão (setado em CONNECT)

  function currentRoom() { return me && me.roomId ? rooms.get(me.roomId) : null; }

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    try {
      mpHandleClientMessage(msg);
    } catch (err) {
      console.error("[WSS] erro processando mensagem tipo '" + (msg && msg.type) + "':", err);
      // não deixa um bug numa mensagem derrubar a conexão nem o processo inteiro
    }

    /* ---- sessão / identidade ---- */
    function mpHandleClientMessage(msg) {
    if (msg.type === "CONNECT") {
      var token = msg.sessionToken;
      var existing = token ? players.get(token) : null;
      if (existing && existing.connectionState !== "removed") {
        // RECONEXÃO: reaproveita o mesmo jogador, sem duplicar.
        clearTimeout(existing._removeTimer);
        existing.ws = ws; existing.connectionState = "connected"; existing.connected = true;
        existing.disconnectedAt = null; existing.lastSeen = Date.now();
        me = existing;
        send(ws, { type: "CONNECTED", sessionToken: me.sessionToken, resumed: true,
          serverId: me.serverId, channelId: me.channelId, roomId: me.roomId,
          room: currentRoom() ? roomSnapshot(currentRoom()) : null });
        if (currentRoom()) broadcastToRoom(currentRoom(), { type: "PLAYER_RECONNECTED", id: me.sessionToken }, me.sessionToken);
        return;
      }
      var newToken = token || genId("sess");
      me = newPlayer(newToken, ws, String(msg.nick || "Jogador").slice(0, 12));
      players.set(newToken, me);
      send(ws, { type: "CONNECTED", sessionToken: newToken, resumed: false });
      return;
    }

    if (!me) return; // precisa mandar CONNECT antes de qualquer outra coisa

    me.lastSeen = Date.now();

    /* ---- servidores ---- */
    if (msg.type === "GET_SERVERS") { send(ws, { type: "SERVER_LIST", servers: serverListSnapshot() }); return; }

    if (msg.type === "JOIN_SERVER") {
      var serverDef = SERVER_DEFS.find(function (s) { return s.id === msg.serverId; });
      if (!serverDef) { send(ws, { type: "ERROR", code: "SERVER_NOT_FOUND" }); return; }
      if (playersInServer(serverDef.id).length >= SERVER_MAX_PLAYERS) { send(ws, { type: "ERROR", code: "SERVER_FULL" }); return; }
      leaveServer(me, true);
      me.serverId = serverDef.id;
      send(ws, { type: "SERVER_STATE", serverId: serverDef.id, channels: channelListSnapshot(serverDef.id) });
      return;
    }
    if (msg.type === "LEAVE_SERVER") { leaveServer(me, true); return; }

    /* ---- canais ---- */
    if (msg.type === "GET_CHANNELS") {
      if (!me.serverId) { send(ws, { type: "ERROR", code: "NOT_IN_SERVER" }); return; }
      send(ws, { type: "CHANNEL_LIST", channels: channelListSnapshot(me.serverId) });
      return;
    }
    if (msg.type === "JOIN_CHANNEL") {
      if (!me.serverId) { send(ws, { type: "ERROR", code: "NOT_IN_SERVER" }); return; }
      var channelId = me.serverId + "_ch" + msg.channelNum;
      if (playersInChannel(channelId).length >= CHANNEL_MAX_PLAYERS) { send(ws, { type: "ERROR", code: "CHANNEL_FULL" }); return; }
      leaveChannel(me, true);
      me.channelId = channelId;
      broadcastToChannel(channelId, { type: "PLAYER_JOINED_CHANNEL", id: me.sessionToken, nick: me.nick }, me.sessionToken);
      send(ws, { type: "CHANNEL_STATE", channelId: channelId, rooms: roomListSnapshot(me.serverId, channelId) });
      return;
    }
    if (msg.type === "LEAVE_CHANNEL") { leaveChannel(me, true); return; }

    /* ---- chat do canal ---- */
    if (msg.type === "CHAT_MESSAGE") {
      if (!me.serverId || !me.channelId) return;
      var text = String(msg.text || "").slice(0, 200);
      if (!text) return;
      broadcastToChannel(me.channelId, { type: "CHAT_MESSAGE", nick: me.nick, text: text, ts: Date.now() });
      return;
    }

    /* ---- salas ---- */
    if (msg.type === "GET_ROOMS") {
      if (!me.serverId || !me.channelId) { send(ws, { type: "ERROR", code: "NOT_IN_CHANNEL" }); return; }
      send(ws, { type: "ROOM_LIST", rooms: roomListSnapshot(me.serverId, me.channelId) });
      return;
    }

    if (msg.type === "CREATE_ROOM") {
      if (!me.serverId || !me.channelId) { send(ws, { type: "ERROR", code: "NOT_IN_CHANNEL" }); return; }
      if (me.roomId) { send(ws, { type: "ERROR", code: "ALREADY_IN_ROOM" }); return; }
      var roomId; do { roomId = genId("room"); } while (rooms.has(roomId)); // garante ID único de verdade
      var roomName = String(msg.name || ("BLACK FIRE #" + Math.floor(1000 + Math.random() * 9000))).slice(0, 30);
      var room = newRoom(roomId, me.channelId, roomName, me.sessionToken, "P&D", "Distrito Industrial");
      rooms.set(roomId, room);
      joinRoomInternal(me, room, msg.team, ws);
      broadcastToChannel(me.channelId, { type: "ROOM_CREATED", room: { id: room.id, name: room.name, mode: room.mode, map: room.map, current: 1, max: room.maxPlayers, status: room.status } }, me.sessionToken);
      return;
    }

    if (msg.type === "JOIN_ROOM") {
      if (!me.serverId || !me.channelId) { send(ws, { type: "ERROR", code: "NOT_IN_CHANNEL" }); return; }
      if (me.roomId) { send(ws, { type: "ERROR", code: "ALREADY_IN_ROOM" }); return; }
      var targetRoom = rooms.get(msg.roomId);
      if (!targetRoom || targetRoom.channelId !== me.channelId) { send(ws, { type: "ERROR", code: "ROOM_NOT_FOUND" }); return; }
      if (targetRoom.status !== "WAITING") { send(ws, { type: "ERROR", code: "ROOM_ALREADY_STARTED" }); return; }
      // checagem síncrona (sem await entre checar e adicionar) — evita corrida na última vaga
      if (countConnectedInRoom(targetRoom) >= targetRoom.maxPlayers) { send(ws, { type: "ERROR", code: "ROOM_FULL" }); return; }
      joinRoomInternal(me, targetRoom, msg.team, ws);
      return;
    }

    if (msg.type === "LEAVE_ROOM") { removePlayerFromRoom(me, true); return; }

    if (msg.type === "SET_READY" || msg.type === "SET_NOT_READY") {
      var r1 = currentRoom();
      if (!r1) return;
      me.ready = (msg.type === "SET_READY");
      broadcastToRoom(r1, { type: me.ready ? "PLAYER_READY" : "PLAYER_NOT_READY", id: me.sessionToken });
      return;
    }

    if (msg.type === "START_GAME") {
      var r2 = currentRoom();
      if (!r2) { send(ws, { type: "ERROR", code: "ROOM_NOT_FOUND" }); return; }
      if (r2.hostId !== me.sessionToken) { send(ws, { type: "ERROR", code: "NOT_HOST" }); return; }
      if (r2.status !== "WAITING") { send(ws, { type: "ERROR", code: "ALREADY_STARTED" }); return; }
      var everyoneReady = true, total = 0;
      for (const pid of r2.players.keys()) {
        var pp = players.get(pid);
        if (!pp || pp.connectionState === "removed") continue;
        total++;
        if (pid !== r2.hostId && !pp.ready) everyoneReady = false; // host também precisa estar pronto (checado abaixo)
      }
      var hostPlayer = players.get(r2.hostId);
      if (!hostPlayer || !hostPlayer.ready) everyoneReady = false;
      if (total < MIN_PLAYERS_TO_START) { send(ws, { type: "ERROR", code: "NOT_ENOUGH_PLAYERS" }); return; }
      if (!everyoneReady) { send(ws, { type: "ERROR", code: "PLAYERS_NOT_READY" }); return; }

      r2.status = "STARTING";
      broadcastToRoom(r2, { type: "GAME_STARTING", roomId: r2.id });
      broadcastToChannel(me.channelId, { type: "ROOM_UPDATED", room: { id: r2.id, name: r2.name, mode: r2.mode, map: r2.map, current: countConnectedInRoom(r2), max: r2.maxPlayers, status: r2.status } });
      setTimeout(function () {
        if (rooms.get(r2.id) !== r2) return;
        r2.status = "IN_PROGRESS";
        r2.round.active = true; r2.round.scoreBL = 0; r2.round.scoreGR = 0; r2.round.num = 0;
        broadcastToRoom(r2, { type: "GAME_STARTED", roomId: r2.id });
        broadcastToChannel(me.channelId, { type: "ROOM_UPDATED", room: { id: r2.id, name: r2.name, mode: r2.mode, map: r2.map, current: countConnectedInRoom(r2), max: r2.maxPlayers, status: r2.status } });
        startRound(r2);
      }, 1500);
      return;
    }

    /* ---- dentro da partida (round/bomba) — protocolo já testado, mantido ---- */
    if (msg.type === "move") {
      me.x = msg.x; me.y = msg.y; me.z = msg.z; me.yaw = msg.yaw; me.pitch = msg.pitch;
      return;
    }
    if (msg.type === "shoot") {
      var room3 = currentRoom(); if (!room3) return;
      var target = players.get(msg.targetId);
      if (!target || !target.alive || !target.connected || target.roomId !== room3.id) return;
      const dmg = Math.max(1, Math.min(150, Number(msg.damage) || 0));
      target.hp -= dmg;
      broadcastToRoom(room3, { type: "hit", targetId: target.sessionToken, sourceId: me.sessionToken, hp: Math.max(0, target.hp) });
      if (target.hp <= 0 && target.alive) {
        target.alive = false; target.hp = 0; target.deaths++;
        me.kills++;
        broadcastToRoom(room3, { type: "playerDown", id: target.sessionToken, killerId: me.sessionToken });
      }
      return;
    }
    if (msg.type === "plantTick") {
      var room4 = currentRoom(); if (!room4) return;
      if (me.team !== "BL" || !me.alive || !room4.round.roundActive || room4.round.bomb.planted) return;
      room4.round.bomb.plantProgress = Math.min(BOMB_PLANT_TIME, room4.round.bomb.plantProgress + 0.2);
      room4.round.lastPlantTickAt = Date.now();
      return;
    }
    if (msg.type === "defuseTick") {
      var room5 = currentRoom(); if (!room5) return;
      if (me.team !== "GR" || !me.alive || !room5.round.roundActive || !room5.round.bomb.planted) return;
      room5.round.bomb.defuseProgress = Math.min(BOMB_DEFUSE_TIME, room5.round.bomb.defuseProgress + 0.2);
      room5.round.lastDefuseTickAt = Date.now();
      return;
    }
    } // fim de mpHandleClientMessage
  });

  ws.on("close", () => {
    if (!me) return;
    me.connectionState = "disconnected";
    me.connected = false;
    me.disconnectedAt = Date.now();
    if (currentRoom()) broadcastToRoom(currentRoom(), { type: "PLAYER_DISCONNECTED", id: me.sessionToken }, me.sessionToken);
    // segura o lugar por RECONNECT_GRACE_MS; se não voltar, remove de vez (sala/canal/servidor).
    var pRef = me;
    pRef._removeTimer = setTimeout(function () {
      if (pRef.connectionState === "disconnected") fullyRemovePlayer(pRef);
    }, RECONNECT_GRACE_MS);
  });
  ws.on("error", (err) => console.error("[WSS] erro numa conexão:", err.message));
});

function joinRoomInternal(p, room, wantedTeam, ws) {
  var team = wantedTeam === "GR" ? "GR" : "BL";
  if (teamCountInRoom(room, team) >= ROOM_TEAM_MAX) {
    var other = team === "BL" ? "GR" : "BL";
    if (teamCountInRoom(room, other) < ROOM_TEAM_MAX) team = other;
    else { send(ws, { type: "ERROR", code: "ROOM_FULL" }); return; }
  }
  p.roomId = room.id; p.team = team; p.ready = false;
  p.hp = 100; p.alive = true; p.kills = 0; p.deaths = 0; p.connected = true;
  room.players.set(p.sessionToken, { id: p.sessionToken, roomJoinedAt: Date.now() });
  if (!room.hostId) room.hostId = p.sessionToken;

  send(ws, { type: "ROOM_JOINED", room: roomSnapshot(room) });
  broadcastToRoom(room, { type: "PLAYER_JOINED", player: publicPlayer(p) }, p.sessionToken);
  broadcastToChannel(p.channelId, { type: "ROOM_UPDATED", room: { id: room.id, name: room.name, mode: room.mode, map: room.map, current: countConnectedInRoom(room), max: room.maxPlayers, status: room.status } });
}

/* ---------------------- LOOPS DO SERVIDOR ---------------------- */
setInterval(() => { for (const room of rooms.values()) tickRound(room); }, TICK_MS);
setInterval(() => {
  for (const room of rooms.values()) {
    if (room.players.size === 0) continue;
    broadcastToRoom(room, { type: "state", roomId: room.id, snapshot: roomSnapshot(room) });
  }
}, STATE_BROADCAST_MS);
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) { console.log("[WSS] conexão sem resposta ao ping — encerrando."); return ws.terminate(); }
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

httpServer.listen(PORT, "0.0.0.0", () => {
  console.log("BLACK FIRE multiplayer server rodando na porta " + PORT + " (0.0.0.0)");
});
