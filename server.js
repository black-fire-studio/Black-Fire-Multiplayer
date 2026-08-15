/* =====================================================================
   BLACK FIRE — Servidor Multiplayer (Modo Bomba / P&D)
   -----------------------------------------------------------------
   Servidor WebSocket autoritativo para salas de até 8x8 (BL vs GR).
   Roda o sistema de rounds (sem respawn), plantio/desarme da bomba,
   placar de rounds e elimina/revive jogadores — exatamente as mesmas
   regras já implementadas no cliente (blackfire.html), agora do lado
   do servidor, pra valer entre dispositivos diferentes.

   SIMPLIFICAÇÕES ASSUMIDAS (deixadas claras de propósito):
   - Posição/rotação dos jogadores: o cliente informa a própria posição
     a cada ~100ms e o servidor apenas retransmite pros outros da sala
     (não há validação de movimento/colisão no servidor). Suficiente
     pra jogar entre amigos; não é anti-cheat de verdade.
   - Acerto de tiro: o cliente que atirou decide localmente quem foi
     atingido (raycast já faz isso no jogo) e manda um aviso de "acertei
     o jogador X". O servidor confia nesse aviso, valida se o alvo está
     vivo/na sala e aplica o dano. Não recalcula a trajetória do tiro.
   - Plantar/desarmar: o cliente avisa repetidamente "estou plantando"
     /"estou desarmando" enquanto acha que está dentro do raio do site;
     o servidor acumula o progresso a partir desses avisos (sem
     reconferir a posição real do jogador).
   Isso é o suficiente pra multiplayer real funcionar entre PC e
   Android — só não é hardened contra um cliente malicioso.
   ===================================================================== */

const { WebSocketServer } = require("ws");
const http = require("http");

const PORT = process.env.PORT || 8080;

/* ---------------------- REGRAS (espelham o cliente) ---------------------- */
const ROUND_TIME = 180;        // segundos por round
const ROUNDS_TO_WIN = 6;       // primeiro a 6 rounds vence a partida
const BOMB_PLANT_TIME = 2.5;   // segundos pra plantar
const BOMB_DEFUSE_TIME = 5;    // segundos pra desarmar
const BOMB_FUSE_TIME = 40;     // segundos até explodir depois de plantada
const BOMB_SITE = { x: 0, z: -10, radius: 2.4 };
const TEAM_SIZE_MAX = 8;

const TICK_MS = 200;           // lógica de round (timer, elimina, bomba)
const STATE_BROADCAST_MS = 80; // posição/vida de todo mundo

/* ---------------------- ESTADO EM MEMÓRIA ---------------------- */
// rooms: Map<roomId, Room>
const rooms = new Map();

function newRoom(roomId) {
  return {
    id: roomId,
    players: new Map(), // id -> player
    round: {
      active: false,      // true depois que alguém apertou "Iniciar Partida"
      roundActive: false,
      num: 0,
      scoreBL: 0,
      scoreGR: 0,
      roundTimeLeft: ROUND_TIME,
      bomb: { planted: false, timer: BOMB_FUSE_TIME, plantProgress: 0, defuseProgress: 0 },
      lastPlantTickAt: 0,
      lastDefuseTickAt: 0
    }
  };
}

function spawnFor(team) {
  // BL nasce perto do próprio lado do mapa, GR do lado oposto — só cosmético,
  // o cliente já tem seu próprio mapa 3D; isso é só um ponto de partida.
  return team === "BL"
    ? { x: (Math.random() * 6 - 3), y: 1.7, z: 8 + Math.random() * 4 }
    : { x: (Math.random() * 6 - 3), y: 1.7, z: -18 - Math.random() * 4 };
}

function teamCount(room, team) {
  let n = 0;
  for (const p of room.players.values()) if (p.team === team && p.connected) n++;
  return n;
}
function aliveCount(room, team) {
  let n = 0;
  for (const p of room.players.values()) if (p.team === team && p.connected && p.alive) n++;
  return n;
}

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}
function broadcast(room, obj, exceptId) {
  const msg = JSON.stringify(obj);
  for (const p of room.players.values()) {
    if (p.id === exceptId) continue;
    if (p.ws.readyState === p.ws.OPEN) p.ws.send(msg);
  }
}

function publicPlayer(p) {
  return {
    id: p.id, nick: p.nick, team: p.team,
    x: p.x, y: p.y, z: p.z, yaw: p.yaw, pitch: p.pitch,
    hp: p.hp, alive: p.alive, kills: p.kills, deaths: p.deaths,
    connected: p.connected
  };
}
function roomSnapshot(room) {
  return {
    players: Array.from(room.players.values()).map(publicPlayer),
    round: {
      active: room.round.active,
      roundActive: room.round.roundActive,
      num: room.round.num,
      scoreBL: room.round.scoreBL,
      scoreGR: room.round.scoreGR,
      roundTimeLeft: room.round.roundTimeLeft,
      bomb: room.round.bomb
    }
  };
}

/* ---------------------- ROUNDS / BOMBA ---------------------- */
function startRound(room) {
  const r = room.round;
  r.num++;
  r.roundActive = true;
  r.roundTimeLeft = ROUND_TIME;
  r.bomb = { planted: false, timer: BOMB_FUSE_TIME, plantProgress: 0, defuseProgress: 0 };
  r.lastPlantTickAt = 0; r.lastDefuseTickAt = 0;

  for (const p of room.players.values()) {
    if (!p.connected) continue;
    p.hp = 100; p.alive = true;
    const sp = spawnFor(p.team);
    p.x = sp.x; p.y = sp.y; p.z = sp.z; p.yaw = 0; p.pitch = 0;
  }
  broadcast(room, { type: "roundStart", round: r.num, scoreBL: r.scoreBL, scoreGR: r.scoreGR, players: Array.from(room.players.values()).map(publicPlayer) });
}

function endRound(room, winner, reason) {
  const r = room.round;
  if (!r.roundActive) return;
  r.roundActive = false;
  if (winner === "BL") r.scoreBL++; else r.scoreGR++;
  broadcast(room, { type: "roundEnd", winner, reason, scoreBL: r.scoreBL, scoreGR: r.scoreGR });

  if (r.scoreBL >= ROUNDS_TO_WIN || r.scoreGR >= ROUNDS_TO_WIN) {
    const matchWinner = r.scoreBL >= ROUNDS_TO_WIN ? "BL" : "GR";
    r.active = false;
    broadcast(room, { type: "matchEnd", winner: matchWinner, scoreBL: r.scoreBL, scoreGR: r.scoreGR });
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

  // só considera "eliminado" se o time tinha gente conectada e agora ninguém vivo
  if (blTotal > 0 && blAlive === 0) { endRound(room, "GR", "elimination"); return; }
  if (grTotal > 0 && grAlive === 0) { endRound(room, "BL", "elimination"); return; }

  const dtSec = TICK_MS / 1000;
  const now = Date.now();

  if (!r.bomb.planted) {
    r.roundTimeLeft -= dtSec;
    // decai o progresso de plantio se não chegou nenhum aviso recente
    if (now - r.lastPlantTickAt > TICK_MS * 1.5) {
      r.bomb.plantProgress = Math.max(0, r.bomb.plantProgress - dtSec * 2);
    }
    if (r.bomb.plantProgress >= BOMB_PLANT_TIME) {
      r.bomb.planted = true;
      r.bomb.timer = BOMB_FUSE_TIME;
      broadcast(room, { type: "bombPlanted" });
    }
    if (r.roundTimeLeft <= 0) { endRound(room, "GR", "timeout"); return; }
  } else {
    r.bomb.timer -= dtSec;
    if (now - r.lastDefuseTickAt > TICK_MS * 1.5) {
      r.bomb.defuseProgress = Math.max(0, r.bomb.defuseProgress - dtSec * 1.5);
    }
    if (r.bomb.defuseProgress >= BOMB_DEFUSE_TIME) { endRound(room, "GR", "defuse"); return; }
    if (r.bomb.timer <= 0) { endRound(room, "BL", "explode"); return; }
  }
}

/* ---------------------- HTTP + WEBSOCKET ---------------------- */
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("BLACK FIRE multiplayer server ativo. Conecte via WebSocket.");
});

// Loga qualquer erro de baixo nível do servidor HTTP (porta ocupada, etc.)
httpServer.on("error", (err) => {
  console.error("[HTTP] erro no servidor:", err);
});

// Loga toda tentativa de upgrade (handshake do WebSocket) que chega — se isso
// nunca aparecer no log do Render quando o cliente tenta conectar, o problema
// é a requisição não estar chegando até o app (proxy/URL errada), não o código.
httpServer.on("upgrade", (req) => {
  console.log("[UPGRADE] pedido de handshake WebSocket recebido:", req.url);
});

const wss = new WebSocketServer({ server: httpServer });

wss.on("error", (err) => {
  console.error("[WSS] erro no servidor WebSocket:", err);
});

wss.on("connection", (ws, req) => {
  console.log("[WSS] cliente conectado:", req.socket.remoteAddress);

  // Keep-alive: hospedagens como o Render derrubam conexões ociosas depois de
  // ~55s sem tráfego. Sem isso, o WebSocket cai sozinho mesmo tendo conectado
  // certinho — por isso cada conexão manda um "ping" a cada 25s e é fechada
  // se não responder (evita sockets zumbis acumulando na sala).
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  let room = null;
  let me = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    if (msg.type === "join") {
      const roomId = String(msg.roomId || "sala1").slice(0, 24);
      const nick = String(msg.nick || "Jogador").slice(0, 12);
      let team = msg.team === "BL" ? "BL" : "GR";

      if (!rooms.has(roomId)) rooms.set(roomId, newRoom(roomId));
      room = rooms.get(roomId);

      if (teamCount(room, team) >= TEAM_SIZE_MAX) {
        const other = team === "BL" ? "GR" : "BL";
        if (teamCount(room, other) < TEAM_SIZE_MAX) team = other;
        else { send(ws, { type: "error", message: "Sala cheia (8x8)." }); ws.close(); return; }
      }

      const id = "p" + Math.random().toString(36).slice(2, 10);
      const sp = spawnFor(team);
      me = { id, ws, nick, team, connected: true,
        x: sp.x, y: sp.y, z: sp.z, yaw: 0, pitch: 0,
        hp: 100, alive: true, kills: 0, deaths: 0 };
      room.players.set(id, me);

      send(ws, { type: "joined", id, roomId, team, snapshot: roomSnapshot(room) });
      broadcast(room, { type: "playerJoined", player: publicPlayer(me) }, id);
      return;
    }

    if (!room || !me) return; // precisa ter dado join antes de qualquer outra coisa

    if (msg.type === "move") {
      me.x = msg.x; me.y = msg.y; me.z = msg.z; me.yaw = msg.yaw; me.pitch = msg.pitch;
      return;
    }

    if (msg.type === "shoot") {
      const target = room.players.get(msg.targetId);
      if (!target || !target.alive || !target.connected) return;
      const dmg = Math.max(1, Math.min(150, Number(msg.damage) || 0));
      target.hp -= dmg;
      broadcast(room, { type: "hit", targetId: target.id, sourceId: me.id, hp: Math.max(0, target.hp) });
      if (target.hp <= 0 && target.alive) {
        target.alive = false; target.hp = 0; target.deaths++;
        me.kills++;
        broadcast(room, { type: "playerDown", id: target.id, killerId: me.id });
      }
      return;
    }

    if (msg.type === "plantTick") {
      if (me.team !== "BL" || !me.alive || !room.round.roundActive || room.round.bomb.planted) return;
      room.round.bomb.plantProgress = Math.min(BOMB_PLANT_TIME, room.round.bomb.plantProgress + 0.2);
      room.round.lastPlantTickAt = Date.now();
      return;
    }
    if (msg.type === "defuseTick") {
      if (me.team !== "GR" || !me.alive || !room.round.roundActive || !room.round.bomb.planted) return;
      room.round.bomb.defuseProgress = Math.min(BOMB_DEFUSE_TIME, room.round.bomb.defuseProgress + 0.2);
      room.round.lastDefuseTickAt = Date.now();
      return;
    }

    if (msg.type === "startMatch") {
      if (room.round.active) return; // já em andamento
      room.round.active = true;
      room.round.scoreBL = 0; room.round.scoreGR = 0; room.round.num = 0;
      startRound(room);
      return;
    }
  });

  ws.on("close", () => {
    if (!room || !me) return;
    me.connected = false;
    broadcast(room, { type: "playerLeft", id: me.id });
    room.players.delete(me.id);
    // sala vazia -> limpa da memória
    let anyone = false;
    for (const p of room.players.values()) if (p.connected) { anyone = true; break; }
    if (!anyone) rooms.delete(room.id);
  });

  ws.on("error", (err) => {
    console.error("[WSS] erro numa conexão:", err.message);
  });
});

/* ---------------------- LOOPS DO SERVIDOR ---------------------- */
setInterval(() => { for (const room of rooms.values()) tickRound(room); }, TICK_MS);
setInterval(() => {
  for (const room of rooms.values()) {
    if (room.players.size === 0) continue;
    broadcast(room, { type: "state", snapshot: roomSnapshot(room) });
  }
}, STATE_BROADCAST_MS);

// Ping/pong a cada 25s: sem isso, proxies como o do Render derrubam conexões
// WebSocket que ficam paradas por muito tempo (ex.: alguém parado no lobby
// esperando a partida começar), mesmo com o servidor rodando normalmente.
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) { console.log("[WSS] conexão sem resposta ao ping — encerrando."); return ws.terminate(); }
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

// 0.0.0.0 é obrigatório em hospedagens como o Render: o processo precisa
// escutar em todas as interfaces, não só em localhost/127.0.0.1, senão o
// proxy deles não consegue encaminhar as requisições (HTTP normal ou
// upgrade de WebSocket) até o app.
httpServer.listen(PORT, "0.0.0.0", () => {
  console.log("BLACK FIRE multiplayer server rodando na porta " + PORT + " (0.0.0.0)");
});
