/* ================= Configraft Dashboard · 前端逻辑 ================= */
"use strict";

const state = {
  nodes: [],        // [{ host, port }]
  leader: null,     // { host, port }
  lastRev: 0,
  onlineCount: 0,
};

const $ = (id) => document.getElementById(id);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- 节点发现：从当前节点 /healthz 拿 peers 与 leader ----------
// peers/leader_id 格式形如 "127.0.0.1:8001:0"（host:port:raft_index），去掉末尾 :0。
function parseEndpoint(s) {
  if (!s) return null;
  const m = String(s).match(/^([^:]+):(\d+)/);
  return m ? { host: m[1], port: m[2] } : null;
}
function nodeUrl(n, path) { return `http://${n.host}:${n.port}${path}`; }
function leaderUrl(path) {
  return state.leader ? nodeUrl(state.leader, path) : null;
}
async function fetchJson(url, opts) {
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

async function discover() {
  try {
    // 用相对路径（页面必须通过 http://<host>:<port>/dashboard 访问；
    // 直接双击 index.html 是 file:// 协议，fetch 会失败并走到下方提示）。
    const h = await fetchJson("/healthz");
    state.leader = parseEndpoint(h.leader_id) || state.leader;
    const peers = (h.peers || []).map(parseEndpoint).filter(Boolean);
    if (peers.length) state.nodes = peers;
  } catch (e) {
    const wrap = $("nodes");
    wrap.innerHTML =
      `<div class="event-empty">无法连接 /healthz（${escapeHtml(e.message)}）。<br>` +
      `请确认已运行 <code>bash scripts/run_cluster.sh</code>，并<b>通过 ` +
      `<code>http://127.0.0.1:800X/dashboard</code> 访问</b>（不要直接双击打开 index.html 文件）。</div>`;
    return;
  }
  renderNodes(state.nodes.map(() => null));
}

// ---------- 集群状态轮询 ----------
async function pollHealth() {
  if (!state.nodes.length) {
    await discover();
    return;
  }
  const snapshot = [];
  let online = 0;
  for (const n of state.nodes) {
    try {
      const h = await fetchJson(nodeUrl(n, "/healthz"));
      snapshot.push({ node: n, ok: true, ...h });
      if (h.role === "leader") state.leader = { host: n.host, port: n.port };
      online++;
    } catch (e) {
      snapshot.push({ node: n, ok: false });
    }
  }
  state.onlineCount = online;
  renderNodes(snapshot);
}
setInterval(pollHealth, 2000);

// ---------- 渲染节点卡片 ----------
function renderNodes(snapshot) {
  const wrap = $("nodes");
  if (!snapshot.length) {
    wrap.innerHTML = '<div class="event-empty">集群信息获取中…</div>';
    return;
  }
  const badge = $("clusterBadge");
  const allOnline = state.onlineCount === snapshot.length;
  badge.classList.toggle("offline", !allOnline);
  $("clusterLabel").textContent = allOnline ? "集群在线" : "部分节点离线";
  $("clusterCount").textContent = `${state.onlineCount}/${snapshot.length}`;
  $("leaderHint").textContent = state.leader ? `→ ${state.leader.host}:${state.leader.port}` : "";

  wrap.innerHTML = snapshot.map((s) => {
    if (!s || !s.ok) {
      const p = s ? s.node.port : "?";
      return `
        <div class="node-card offline">
          <div class="node-head">
            <span class="node-name">:${p}</span>
            <span class="node-role offline">OFFLINE</span>
          </div>
          <div class="node-metrics">连接失败</div>
        </div>`;
    }
    const roleCls = s.role === "leader" ? "leader"
      : s.role === "candidate" ? "candidate"
      : "follower";
    const peersTxt = (s.peers || []).map((p) => parseEndpoint(p)).filter(Boolean)
      .map((p) => `${p.host}:${p.port}`).join(", ");
    return `
      <div class="node-card ${s.role === "leader" ? "leader" : ""}">
        <div class="node-head">
          <span class="node-name">${s.leader_id ? "" : ""}${s.node ? ":" + s.node.port : ""}</span>
          <span class="node-role ${roleCls}">${(s.role || "?").toUpperCase()}</span>
        </div>
        <div class="node-metrics">
          <span>term <b>${s.term ?? "-"}</b></span>
          <span>commit <b>${s.commit_index ?? "-"}</b></span>
          <span>applied <b>${s.applied_index ?? "-"}</b></span>
        </div>
        <div class="node-peers">${peersTxt}</div>
      </div>`;
  }).join("");
}

// ---------- 配置 / KV 操作 ----------
async function doOp(kind) {
  const key = $("keyInput").value.trim();
  if (!key) { showResult("请输入 Key", true); return; }
  if (!state.leader) { showResult("未发现 Leader，请稍候或检查集群", true); return; }

  const value = $("valueInput").value;
  const expect = $("expectInput").value;
  const ver = parseInt($("verInput").value, 10) || 0;
  const enc = encodeURIComponent(key);
  let url, method = "POST", body = null;

  switch (kind) {
    case "put":     url = `/v1/kv/${enc}`;              body = { value }; break;
    case "get":     url = `/v1/kv/${enc}`;              method = "GET"; break;
    case "delete":  url = `/v1/kv/${enc}`;              method = "DELETE"; break;
    case "cas":     url = `/v1/kv/${enc}/cas`;          body = { expect, value }; break;
    case "publish": url = `/v1/config/${enc}`;          body = { value }; break;
    case "cfgget":  url = `/v1/config/${enc}`;          method = "GET"; break;
    case "rollback": url = `/v1/config/${enc}/rollback`; body = { target_version: ver }; break;
    default: return;
  }

  try {
    const j = await fetchJson(leaderUrl(url), {
      method,
      headers: { "Content-Type": "application/json" },
      body: body ? JSON.stringify(body) : undefined,
    });
    renderResult(j, kind);
    if (kind === "cfgget") renderHistory(j);
  } catch (e) {
    showResult(`请求失败：${e.message}（Leader 可能已变更，稍后自动恢复）`, true);
  }
}

function showResult(text, isErr) {
  $("opResult").innerHTML =
    `<div class="${isErr ? "result-err" : "result-ok"}">${escapeHtml(text)}</div>`;
}

function renderResult(j, kind) {
  const box = $("opResult");
  if (!j) { box.innerHTML = '<div class="result-err">无响应</div>'; return; }
  const lines = [];
  if (j.code !== undefined && j.code !== 0) {
    lines.push(`<div class="result-err">✗ code=${j.code} ${escapeHtml(j.message || "")}</div>`);
  }
  if (j.kv) {
    const kv = j.kv;
    lines.push(`<div class="kv-line"><b>key</b>     ${escapeHtml(kv.key)}</div>`);
    lines.push(`<div class="kv-line"><b>value</b>   <span class="val">${escapeHtml(kv.value)}</span></div>`);
    lines.push(`<div class="kv-line"><b>version</b> ${kv.version}</div>`);
    lines.push(`<div class="kv-line"><b>revision</b> ${kv.revision}</div>`);
  } else if (j.code === 0 || j.code === undefined) {
    lines.push(`<div class="result-ok">✓ ${kind.toUpperCase()} 成功</div>`);
  }
  box.innerHTML = lines.join("\n");
}

function renderHistory(j) {
  const box = $("historyBox");
  const hist = j.history || [];
  if (!hist.length) { box.innerHTML = ""; return; }
  box.innerHTML =
    '<div class="history-title">配置版本链（cfg/{key}/{ver} 索引）</div>' +
    hist.map((kv) => `
      <div class="history-item">
        <span class="history-ver">v${kv.version}</span>
        <span class="history-rev">rev ${kv.revision}</span>
        <span class="history-val">${escapeHtml(kv.value)}</span>
      </div>`).join("");
}

// ---------- Watch 实时事件流（长轮询，监听全部 key） ----------
async function watchLoop() {
  while (true) {
    if (!state.leader) { await sleep(1500); continue; }
    try {
      const url = leaderUrl(`/v1/watch?from_revision=${state.lastRev}&timeout_ms=30000`);
      const j = await fetchJson(url);
      if (j && j.events && j.events.length) {
        renderEvents(j.events);
        state.lastRev = j.current_revision || state.lastRev;
      } else if (j && j.current_revision) {
        state.lastRev = j.current_revision;  // 无事件也推进续传锚点
      }
      // 无论结果如何都让出 200ms，避免 /v1/watch 快速返回时忙循环
      // 饿死 setInterval(pollHealth)（否则节点状态会停止刷新）。
      await sleep(200);
    } catch (e) {
      // Leader 变更或断线：重定位后自动重连
      state.leader = null;
      await discover();
      await sleep(1200);
    }
  }
}

function renderEvents(events) {
  const stream = $("watchEvents");
  const first = stream.querySelector(".event-empty");
  if (first) first.remove();
  for (const ev of events) {
    const el = document.createElement("div");
    el.className = "event";
    el.innerHTML = `
      <div class="event-top">
        <span class="event-rev">#${ev.revision}</span>
        <span class="event-type ${ev.type}">${ev.type}</span>
        <span class="event-key">${escapeHtml(ev.key)}</span>
      </div>
      <div>value: <span class="event-val">${escapeHtml(ev.value)}</span>
        <span class="event-rev"> · v${ev.version}</span>
      </div>`;
    stream.appendChild(el);
  }
  // 位于底部附近时自动滚动
  const nearBottom = stream.scrollHeight - stream.scrollTop - stream.clientHeight < 60;
  if (nearBottom) stream.scrollTop = stream.scrollHeight;
}

// ---------- 工具 ----------
function escapeHtml(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// ---------- 初始化 ----------
document.querySelectorAll("button[data-op]").forEach((btn) => {
  btn.addEventListener("click", () => doOp(btn.dataset.op));
});
$("keyInput").addEventListener("keydown", (e) => { if (e.key === "Enter") doOp("put"); });
$("valueInput").addEventListener("keydown", (e) => { if (e.key === "Enter") doOp("put"); });

(async function init() {
  await discover();
  pollHealth();
  watchLoop();
})();
