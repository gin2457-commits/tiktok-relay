// =====================================================================
//  JAPAN vs KOREA LIVE BATTLE 中継サーバー
//  役割: TikTok LIVEのギフトを受け取り、ゲーム画面(WebSocket)へ転送する
//  ※ 非公式ライブラリ(TikTok-Live-Connector)を使います。仕様変更で止まることがあります。
//  ※ 合言葉・APIキーはこのファイルに書かず、Renderの「Environment」に設定します。
// =====================================================================
import http from "node:http";
import crypto from "node:crypto";
import { WebSocketServer } from "ws";
import * as TLC from "tiktok-live-connector";

const { TikTokLiveConnection } = TLC;
const WebcastEvent = TLC.WebcastEvent ?? {};
const ControlEvent = TLC.ControlEvent ?? {};
const EV = {
  GIFT: WebcastEvent.GIFT ?? "gift",
  STREAM_END: WebcastEvent.STREAM_END ?? "streamEnd",
  CONNECTED: ControlEvent.CONNECTED ?? "connected",
  DISCONNECTED: ControlEvent.DISCONNECTED ?? "disconnected",
  ERROR: ControlEvent.ERROR ?? "error",
};

// ---- 環境変数（Renderの Environment で設定。コードには書かない）----
const PORT = Number(process.env.PORT) || 3000;
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || ""; // 必須: 接続操作の合言葉
const DEFAULT_USER = process.env.TIKTOK_USERNAME || ""; // 任意: 起動時に自動接続するユーザー名
const EULER_API_KEY = process.env.EULER_API_KEY || ""; // 任意: 署名サービスのAPIキー
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean); // 任意: 例 https://ユーザー名.github.io

const VALID_NAME = /^[A-Za-z0-9._]{2,40}$/;
const log = (...a) => console.log(new Date().toISOString(), ...a);

// ---- 状態 ----
const status = { tiktok: "idle", username: "", detail: "" };
let conn = null; // 今のTikTok接続
let wanted = ""; // 接続したいユーザー名（空なら接続しない）
let session = 0; // 古い接続のイベントを無視するための番号
let retryTimer = null;
const streaks = new Map(); // 連続ギフトの前回カウント
const giftPrices = new Map(); // giftId -> 1個あたりの価値
const loggedGifts = new Set();

// ---- 便利関数 ----
const sha = (s) => crypto.createHash("sha256").update(String(s)).digest();
function tokenOk(t) {
  if (!ADMIN_TOKEN) return false; // 合言葉が未設定なら操作は全て拒否
  return crypto.timingSafeEqual(sha(t ?? ""), sha(ADMIN_TOKEN));
}
const cleanName = (s) => String(s ?? "").trim().replace(/^@/, "");
const shortErr = (e) => String(e?.message ?? e).replace(/\s+/g, " ").slice(0, 120);

// ---- HTTP + WebSocket ----
const server = http.createServer((req, res) => {
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, {
      "content-type": "application/json; charset=utf-8",
      "access-control-allow-origin": "*",
    });
    res.end(JSON.stringify({ ok: true, ...status, clients: wss.clients.size }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({
  server,
  maxPayload: 4096,
  verifyClient: (info) =>
    ALLOWED_ORIGINS.length === 0 || ALLOWED_ORIGINS.includes(info.origin),
});

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function broadcast(obj) {
  const s = JSON.stringify(obj);
  for (const ws of wss.clients) if (ws.readyState === 1) ws.send(s);
}
function setStatus(tiktok, username, detail = "") {
  Object.assign(status, { tiktok, username, detail });
  log("[status]", tiktok, username, detail);
  broadcast({ type: "status", ...status });
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));
  ws.on("error", () => {});
  send(ws, { type: "hello", controlEnabled: Boolean(ADMIN_TOKEN) });
  send(ws, { type: "status", ...status });

  ws.on("message", (raw) => {
    let m;
    try {
      m = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (!m || typeof m !== "object") return;
    if (m.type === "ping") return send(ws, { type: "pong", t: Date.now() });

    if (m.type === "connect" || m.type === "disconnect" || m.type === "simulate") {
      if (!tokenOk(m.token)) {
        return send(ws, { type: "error", message: "合言葉(ADMIN_TOKEN)が違うか、サーバーに未設定です" });
      }
      if (m.type === "disconnect") {
        stopTikTok();
        return setStatus("idle", "", "TikTok未接続");
      }
      if (m.type === "simulate") {
        // サーバー経由の通信確認用。TikTokの本物のギフトではありません（test:true）。
        const coins = Math.min(100000, Math.max(1, Number(m.coins) || 1));
        return broadcast({
          type: "gift",
          id: crypto.randomUUID(),
          test: true,
          user: "サーバーテスト",
          giftName: String(m.giftName || "Rose").slice(0, 40),
          giftId: 0,
          coins,
          count: 1,
          priceKnown: true,
        });
      }
      const name = cleanName(m.username);
      if (!VALID_NAME.test(name)) {
        return send(ws, { type: "error", message: "TikTokユーザー名の形式が正しくありません" });
      }
      if (name === wanted && conn) return broadcast({ type: "status", ...status }); // 既に接続中
      startTikTok(name);
    }
  });
});

// 30秒ごとに死んだ接続を掃除
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch {}
  }
  const now = Date.now();
  for (const [k, v] of streaks) if (now - v.t > 120000) streaks.delete(k);
}, 30000);

// ---- TikTok接続 ----
function stopTikTok() {
  session++;
  wanted = "";
  clearTimeout(retryTimer);
  if (conn) {
    try {
      conn.disconnect();
    } catch {}
    conn = null;
  }
  streaks.clear();
}

function scheduleRetry(my, ms) {
  clearTimeout(retryTimer);
  retryTimer = setTimeout(() => {
    const n = wanted;
    if (my === session && n) startTikTok(n);
  }, ms);
}

async function startTikTok(name) {
  stopTikTok();
  wanted = name;
  const my = ++session;
  setStatus("connecting", name, "接続中…");
  const opts = {};
  if (EULER_API_KEY) opts.signApiKey = EULER_API_KEY;
  let c;
  try {
    c = new TikTokLiveConnection(name, opts);
  } catch (e) {
    setStatus("error", name, shortErr(e));
    return;
  }
  conn = c;

  c.on(EV.GIFT, (d) => {
    if (my === session) {
      try {
        onGift(d);
      } catch (e) {
        log("[gift error]", shortErr(e));
      }
    }
  });
  c.on(EV.STREAM_END, () => {
    if (my !== session) return;
    wanted = "";
    setStatus("ended", name, "ライブが終了しました");
  });
  c.on(EV.DISCONNECTED, () => {
    if (my !== session || !wanted) return;
    setStatus("disconnected", name, "切断されました。自動で再接続します");
    scheduleRetry(my, 15000);
  });
  c.on(EV.ERROR, (e) => log("[tiktok error]", shortErr(e)));

  try {
    const st = await c.connect();
    if (my !== session) {
      try {
        c.disconnect();
      } catch {}
      return;
    }
    setStatus("connected", name, `ルームID ${st?.roomId ?? "?"}`);
    loadGiftPrices(c, my);
  } catch (e) {
    if (my !== session) return;
    setStatus("error", name, shortErr(e) + "（30秒後に再試行）");
    scheduleRetry(my, 30000);
  }
}

async function loadGiftPrices(c, my) {
  try {
    const list = await c.fetchAvailableGifts();
    if (my !== session || !Array.isArray(list)) return;
    for (const g of list) giftPrices.set(String(g.id), Number(g.diamond_count) || 0);
    log("[gifts] 価格表を取得:", giftPrices.size, "件");
  } catch (e) {
    log("[gifts] 価格表の取得に失敗（無視して続行）:", shortErr(e));
  }
}

// ギフト1個あたりの価値を探す（取得元は未確認のため複数の場所を順に探す）
function priceFor(d, giftId) {
  const gd = d.giftDetails ?? d.gift ?? {};
  const cands = [
    ["giftDetails.diamondCount", gd.diamondCount],
    ["data.diamondCount", d.diamondCount],
    ["extendedGiftInfo.diamond_count", d.extendedGiftInfo?.diamond_count],
    ["priceTable", giftPrices.get(String(giftId))],
  ];
  for (const [via, v] of cands) {
    const n = Number(v);
    if (Number.isFinite(n) && n > 0) return { per: n, via };
  }
  return { per: 1, via: "unknown(1で代用)" };
}

function onGift(d) {
  const gd = d.giftDetails ?? d.gift ?? {};
  const giftId = d.giftId ?? gd.id ?? gd.giftId ?? 0;
  const giftName = String(gd.giftName ?? d.giftName ?? `gift${giftId}`).slice(0, 40);
  const userKey = String(d.user?.userId ?? d.user?.uniqueId ?? "unknown");
  const nick = String(d.user?.nickname || d.user?.uniqueId || "視聴者").slice(0, 30);
  const type = gd.giftType ?? d.giftType;
  const rc = Math.max(1, Number(d.repeatCount) || 1);

  // 連続ギフト(giftType=1)は「前回との差分」だけを送る → 連打中もリアルタイムに反応、二重加算なし
  let count = rc;
  if (type === 1) {
    const key = `${userKey}:${giftId}`;
    const prev = streaks.get(key)?.n ?? 0;
    count = rc - prev;
    if (d.repeatEnd) streaks.delete(key);
    else streaks.set(key, { n: rc, t: Date.now() });
    if (count <= 0) return;
  }

  const { per, via } = priceFor(d, giftId);
  if (!loggedGifts.has(giftId)) {
    loggedGifts.add(giftId);
    log(`[gift] ${giftName} id=${giftId} 価値=${per} 取得元=${via} type=${type}`);
  }
  broadcast({
    type: "gift",
    id: crypto.randomUUID(),
    test: false,
    user: nick,
    giftName,
    giftId,
    coins: per,
    count,
    priceKnown: !via.startsWith("unknown"),
  });
}

process.on("unhandledRejection", (e) => log("[unhandledRejection]", shortErr(e)));
process.on("uncaughtException", (e) => log("[uncaughtException]", shortErr(e)));

server.listen(PORT, "0.0.0.0", () => {
  log(`中継サーバー起動 port=${PORT} 合言葉設定=${Boolean(ADMIN_TOKEN)} 署名キー設定=${Boolean(EULER_API_KEY)}`);
  const n = cleanName(DEFAULT_USER);
  if (n && VALID_NAME.test(n)) startTikTok(n);
});
