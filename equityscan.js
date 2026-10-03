#!/usr/bin/env node
"use strict";
/* EquityScan - single-file full stack (Express backend + Liquid Glass frontend).
   npm install && npm start   |   env: PORT, GROQ_API_KEY, GROQ_MODEL, DAILY_CALL_BUDGET */
const fs = require("fs"), path = require("path"), express = require("express"), cors = require("cors");
const { NseIndia } = require("stock-nse-india");

/* ---------- CONFIG ---------- */
const env = (k, d) => { const v = process.env[k]; const n = Number(v); return v && Number.isFinite(n) ? n : d; };
const C = {
  port: env("PORT", 3000), ttl: env("QUOTE_CACHE_TTL", 300000), conc: env("MAX_CONCURRENCY", 4),
  refresh: env("REFRESH_INTERVAL_MS", 900000), groqKey: process.env.GROQ_API_KEY || null,
  model: process.env.GROQ_MODEL || "openai/gpt-oss-120b",
  U: ["TCS","RELIANCE","HDFCBANK","INFY","ICICIBANK","BHARTIARTL","SBIN","ITC","LT","KOTAKBANK","HINDUNILVR","AXISBANK","BAJFINANCE","MARUTI","ASIANPAINT","WIPRO","TITAN","SUNPHARMA","NTPC","ADANIENT","ULTRACEMCO","POWERGRID","NESTLEIND","TATAMOTORS","JSWSTEEL"],
};
const fail = (code, msg, status) => Object.assign(new Error(msg), { code, status: status || 500 });

/* ---------- DAILY BUDGETS (persisted, reset 00:00 IST) ---------- */
const ist = () => new Date(Date.now() + 19800000).toISOString().slice(0, 10);
function Budget(file, limit) {
  this.f = path.join(__dirname, file); this.l = limit; this.d = ist(); this.u = 0;
  try { const p = JSON.parse(fs.readFileSync(this.f, "utf8")); if (p.date === ist()) { this.u = p.used; } } catch (e) {}
}
Budget.prototype.can = function () { if (this.d !== ist()) { this.d = ist(); this.u = 0; } return this.u < this.l; };
Budget.prototype.spend = function () {
  this.u++; try { fs.writeFileSync(this.f, JSON.stringify({ date: this.d, used: this.u })); } catch (e) {}
};
const nseBudget = new Budget(".budget-state.json", env("DAILY_CALL_BUDGET", 50));
const aiBudget = new Budget(".groq-budget-state.json", env("GROQ_DAILY_BUDGET", 500));

/* ---------- CACHE (TTL + in-flight dedupe + stale reads) ---------- */
const store = new Map(), inflight = new Map();
const cget = (k) => { const e = store.get(k); return e && Date.now() < e.e ? e.v : undefined; };
const stale = (k) => { const e = store.get(k); return e ? e.v : undefined; };
const cset = (k, v, ttl) => { store.set(k, { v, e: Date.now() + ttl }); return v; };
const dedupe = (k, fn) => {
  if (inflight.has(k)) return inflight.get(k);
  const p = Promise.resolve().then(fn).finally(() => inflight.delete(k));
  inflight.set(k, p); return p;
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const tmo = (p, ms) => { let t; return Promise.race([p, new Promise((_, rj) => { t = setTimeout(() => rj(fail("TIMEOUT", "Upstream timed out.", 504)), ms); })]).finally(() => clearTimeout(t)); };
async function pool(arr, n, fn) {
  const out = []; let i = 0;
  await Promise.all(Array.from({ length: Math.min(n, arr.length) }, async () => { while (i < arr.length) { const j = i++; out[j] = await fn(arr[j]); } }));
  return out;
}

/* ---------- NSE PROVIDER + NORMALIZE (never fabricate: missing = null) ---------- */
const nse = new NseIndia();
const num = (v) => { const x = Number(v); return Number.isFinite(x) ? x : null; };
function norm(symbol, raw) {
  if (!raw || typeof raw !== "object") return null;
  const info = raw.info || {}, pi = raw.priceInfo || {}, wh = pi.weekHighLow || {}, id = pi.intraDayHighLow || {};
  const price = num(pi.lastPrice), hi = num(wh.max), lo = num(wh.min);
  const have = [price, hi, lo].filter((v) => v !== null).length;
  return {
    symbol, companyName: info.companyName || null, sector: info.industry || null, currentPrice: price,
    previousClose: num(pi.previousClose), change: num(pi.change), percentChange: num(pi.pChange),
    open: num(pi.open), dayHigh: num(id.max), dayLow: num(id.min), week52High: hi, week52Low: lo,
    volume: num(pi.totalTradedVolume), lastUpdated: pi.lastUpdateTime || null,
    highPercent: price !== null && hi ? Number(((price / hi) * 100).toFixed(2)) : null,
    dataStatus: have === 3 ? "COMPLETE" : have === 0 ? "UNAVAILABLE" : "PARTIAL",
  };
}
async function getStock(sym) {
  const k = "s:" + sym, f = cget(k); if (f) return { ...f, source: "cache" };
  return dedupe(k, async () => {
    const f2 = cget(k); if (f2) return { ...f2, source: "cache" };
    const old = stale(k);
    if (!nseBudget.can()) { if (old) return { ...old, source: "stale" }; throw fail("RATE_LIMITED", "Daily NSE call budget used up and nothing cached yet.", 429); }
    let raw, err;
    for (let i = 0; i <= 2 && !raw; i++) {
      try { raw = await tmo(nse.getEquityDetails(sym), 10000); }
      catch (e) { err = e; if (e && e.response && e.response.status === 403) break; await sleep(250 * 2 ** i); }
    }
    if (!raw) { if (old) return { ...old, source: "stale" }; throw fail("PROVIDER_UNAVAILABLE", "NSE unavailable: " + (err && err.message), 503); }
    nseBudget.spend();
    const v = norm(sym, raw); if (!v) throw fail("INVALID_PROVIDER_RESPONSE", "Unexpected NSE response.", 502);
    cset(k, v, C.ttl); return { ...v, source: "provider" };
  });
}
const allStocks = async () => (await pool(C.U, C.conc, (s) => getStock(s).catch(() => null))).filter(Boolean);

async function marketStatus() {
  const k = "market", f = cget(k); if (f) return f;
  try {
    const raw = await tmo(nse.getMarketStatus(), 10000);
    const m = Array.isArray(raw && raw.marketState) ? raw.marketState.find((x) => x.market === "Capital Market") || raw.marketState[0] : null;
    return cset(k, { status: m && m.marketStatus ? (/open/i.test(m.marketStatus) ? "OPEN" : "CLOSED") : "UNKNOWN", tradeDate: (m && m.tradeDate) || null }, 60000);
  } catch (e) { return stale(k) || { status: "UNKNOWN", tradeDate: null }; }
}

/* ---------- GROQ CLIENT ---------- */
async function groq(messages, maxTokens) {
  if (!C.groqKey) throw fail("AI_NOT_CONFIGURED", "GROQ_API_KEY is not set, so AI features are off.", 503);
  if (!aiBudget.can()) throw fail("RATE_LIMITED", "Daily AI budget used up. Try after reset.", 429);
  const ac = new AbortController(), t = setTimeout(() => ac.abort(), 20000);
  let res;
  try {
    res = await fetch("https://api.groq.com/openai/v1/chat/completions", {
      method: "POST", signal: ac.signal,
      headers: { "Content-Type": "application/json", Authorization: "Bearer " + C.groqKey },
      body: JSON.stringify({ model: C.model, messages, max_tokens: maxTokens, temperature: 0 }),
    });
  } catch (e) { throw fail("PROVIDER_UNAVAILABLE", e.name === "AbortError" ? "AI timed out." : e.message, 503); }
  finally { clearTimeout(t); }
  if (res.status === 401) throw fail("PROVIDER_UNAVAILABLE", "Groq rejected the API key.", 503);
  if (res.status === 429) throw fail("RATE_LIMITED", "Groq rate limit hit.", 429);
  if (!res.ok) throw fail("PROVIDER_UNAVAILABLE", "Groq error " + res.status, 503);
  aiBudget.spend();
  const j = await res.json(), c = j && j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.content;
  if (!c) throw fail("INVALID_PROVIDER_RESPONSE", "Empty AI reply.", 502);
  return c.trim();
}

/* ---------- NATURAL-LANGUAGE QUERY ---------- */
const FIELDS = { currentPrice: "price in INR", percentChange: "today's % change", volume: "shares traded today", week52High: "52-week high", week52Low: "52-week low", highPercent: "price as % of 52-week high (0-100)" };
const OPS = ["<", ">", "<=", ">="];
const QPROMPT = 'Convert a stock search into JSON only (no prose, no code fences): {"sector":string|null,"filters":[{"field":string,"op":"<"|">"|"<="|">=","value":number}],"sortBy":string|null,"sortDir":"asc"|"desc","limit":number|null,"unsupported":[string]}. Allowed fields: ' +
  Object.entries(FIELDS).map(([k, v]) => k + " (" + v + ")").join("; ") +
  '. "sector" is one short keyword (bank, software, pharma, steel...). Anything requested that is not an allowed field (P/E, market cap, dividend...) goes in "unsupported"; never invent filters for it.';
async function runQuery(text) {
  const reply = await groq([{ role: "system", content: QPROMPT }, { role: "user", content: text }], 900);
  let s;
  try { const a = reply.indexOf("{"), b = reply.lastIndexOf("}"); s = JSON.parse(reply.slice(a, b + 1)); }
  catch (e) { throw fail("INVALID_PROVIDER_RESPONSE", "Could not understand the AI reply. Try rephrasing.", 502); }
  const spec = {
    sector: typeof s.sector === "string" && s.sector.trim() ? s.sector.trim().toLowerCase() : null,
    filters: (Array.isArray(s.filters) ? s.filters : []).filter((f) => f && FIELDS[f.field] && OPS.includes(f.op) && Number.isFinite(Number(f.value))).map((f) => ({ field: f.field, op: f.op, value: Number(f.value) })),
    sortBy: FIELDS[s.sortBy] ? s.sortBy : null, sortDir: s.sortDir === "asc" ? "asc" : "desc",
    limit: Number(s.limit) > 0 ? Math.min(Number(s.limit), 50) : null,
    unsupported: Array.isArray(s.unsupported) ? s.unsupported.filter((x) => typeof x === "string").slice(0, 5) : [],
  };
  const ok = (r) => (!spec.sector || ((r.sector || "") + " " + (r.companyName || "")).toLowerCase().includes(spec.sector)) &&
    spec.filters.every((f) => { const v = r[f.field]; if (v === null || v === undefined) return false; return f.op === "<" ? v < f.value : f.op === ">" ? v > f.value : f.op === "<=" ? v <= f.value : v >= f.value; });
  let rows = (await allStocks()).filter(ok);
  if (spec.sortBy) rows.sort((a, b) => ((a[spec.sortBy] || 0) - (b[spec.sortBy] || 0)) * (spec.sortDir === "asc" ? 1 : -1));
  if (spec.limit) rows = rows.slice(0, spec.limit);
  return { interpreted: spec, matches: rows.length, results: rows };
}

/* ---------- ROUTES ---------- */
const app = express();
app.use(cors()); app.use(express.json({ limit: "50kb" }));
const h = (fn) => async (q, r) => {
  try { r.json({ success: true, data: await fn(q) }); }
  catch (e) { r.status(e.status || 500).json({ success: false, error: { code: e.code || "INTERNAL_ERROR", message: e.message || "Internal error." } }); }
};
app.get("/api/health", h(async () => ({ ok: true, ai: !!C.groqKey, nseBudgetUsed: nseBudget.u, aiBudgetUsed: aiBudget.u })));
app.get("/api/market-status", h(marketStatus));
app.get("/api/stocks", h(async () => ({ rows: await allStocks() })));
app.get("/api/stock/:symbol", h(async (q) => {
  const sym = String(q.params.symbol || "").trim().toUpperCase();
  if (!/^[A-Z0-9&-]{1,20}$/.test(sym)) throw fail("INVALID_SYMBOL", '"' + sym + '" is not a valid NSE symbol.', 400);
  return getStock(sym);
}));
app.post("/api/query", h(async (q) => {
  const t = typeof q.body.query === "string" ? q.body.query.trim() : "";
  if (!t || t.length > 300) throw fail("INVALID_PARAMETERS", '"query" must be 1-300 characters.', 400);
  return dedupe("q:" + t.toLowerCase(), () => runQuery(t));
}));
app.post("/api/chat", h(async (q) => {
  const m = typeof q.body.message === "string" ? q.body.message.trim() : "";
  if (!m || m.length > 400) throw fail("INVALID_PARAMETERS", '"message" must be 1-400 characters.', 400);
  const ctx = JSON.stringify(q.body.context || {}).slice(0, 6000), k = "c:" + m.toLowerCase() + ctx.length;
  const hit = cget(k); if (hit) return hit;
  const sys = "You are the EquityScan assistant. Answer ONLY from the CONTEXT JSON (the stocks, watchlist and stock currently shown in the app). If the answer is not in it, say you don't have that. Be brief (max 3 sentences). NEVER give buy/sell/hold advice or price predictions; if asked, politely decline.\nCONTEXT: " + ctx;
  return cset(k, { reply: await groq([{ role: "system", content: sys }, { role: "user", content: m }], 700) }, 60000);
}));
app.get("/", (q, r) => r.type("html").send(HTML));

/* ---------- FRONTEND (Liquid Glass) ---------- */
const HTML = String.raw`<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>EquityScan</title>
<style>
:root{--g:#2ee6a6;--r:#ff6b81;--t:#eaf6ff;--m:#9db8cc}
*{box-sizing:border-box;margin:0}
body{font:15px system-ui,-apple-system,sans-serif;color:var(--t);background:linear-gradient(160deg,#061a2e,#0b3b4f 60%,#0a5560);background-attachment:fixed;min-height:100vh;padding-bottom:110px;overflow-x:hidden}
.blob{position:fixed;border-radius:50%;filter:blur(70px);opacity:.4;z-index:0;animation:fl 16s ease-in-out infinite alternate}
@keyframes fl{to{transform:translate(70px,-90px) scale(1.25)}}
#fx{position:fixed;inset:0;z-index:1;pointer-events:none}
#app{position:relative;z-index:2;max-width:760px;margin:0 auto;padding:env(safe-area-inset-top) 14px 0}
.glass{background:rgba(255,255,255,.09);-webkit-backdrop-filter:blur(18px);backdrop-filter:blur(18px);border:1px solid rgba(255,255,255,.18);border-radius:22px;box-shadow:inset 0 1px 0 rgba(255,255,255,.25),0 8px 30px rgba(0,0,0,.25)}
header{display:flex;justify-content:space-between;align-items:center;padding:12px 16px;margin:12px 0;position:sticky;top:8px;z-index:5}
header b{font-size:18px}header small{display:block;color:var(--m);font-size:10px;letter-spacing:2px}
#mk{font-size:12px;padding:5px 10px;border-radius:99px;background:rgba(255,255,255,.12)}
.up{color:var(--g)}.dn{color:var(--r)}.mut{color:var(--m);font-size:12px}
.row{display:grid;grid-template-columns:1fr auto;gap:2px 10px;padding:12px 16px;margin-bottom:10px;cursor:pointer;transition:transform .2s cubic-bezier(.3,1.6,.5,1)}
.row:active{transform:scale(.96) rotate(-.5deg)}.row small{color:var(--m);font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.row .p{text-align:right}
h3{margin:16px 4px 8px;font-size:14px;color:var(--m);font-weight:600}
input{width:100%;padding:14px 16px;border-radius:16px;border:1px solid rgba(255,255,255,.2);background:rgba(255,255,255,.08);color:var(--t);font-size:15px;outline:none}
button{font:inherit;color:var(--t);border:1px solid rgba(255,255,255,.25);background:rgba(255,255,255,.14);border-radius:14px;padding:10px 16px;cursor:pointer}
.chip{display:inline-block;padding:4px 10px;margin:3px 4px 3px 0;border-radius:99px;background:rgba(46,230,166,.15);font-size:12px}
#tabs{position:fixed;left:50%;transform:translateX(-50%);bottom:calc(12px + env(safe-area-inset-bottom));display:flex;gap:4px;padding:6px;z-index:6;width:min(92vw,420px)}
#tabs button{flex:1;border:0;background:none;padding:10px 4px;border-radius:16px;font-size:13px}
#tabs button.on{background:rgba(255,255,255,.2)}
.sk{height:58px;margin-bottom:10px;background:linear-gradient(90deg,rgba(255,255,255,.05),rgba(255,255,255,.15),rgba(255,255,255,.05));background-size:200% 100%;animation:sh 1.2s infinite;border-radius:18px}
@keyframes sh{to{background-position:-200% 0}}
#modal,#chat{position:fixed;z-index:9;display:none}
#modal{inset:0;background:rgba(0,0,0,.45);align-items:flex-end;justify-content:center}
#modal .box{width:min(560px,100%);padding:20px 20px calc(24px + env(safe-area-inset-bottom));border-radius:28px 28px 0 0;animation:up .3s ease}
@keyframes up{from{transform:translateY(60px);opacity:0}}
.bar{height:8px;border-radius:9px;background:rgba(255,255,255,.15);margin:8px 0;position:relative}.bar i{position:absolute;top:-4px;width:16px;height:16px;border-radius:50%;background:var(--g);transform:translateX(-50%);box-shadow:0 0 12px var(--g)}
#cb{position:fixed;right:16px;bottom:calc(84px + env(safe-area-inset-bottom));z-index:7;width:54px;height:54px;border-radius:50%;font-size:22px}
#chat{right:12px;left:12px;bottom:calc(150px + env(safe-area-inset-bottom));max-width:420px;margin-left:auto;padding:14px;max-height:55vh;flex-direction:column}
#log{overflow:auto;flex:1;margin-bottom:10px;font-size:14px}#log p{margin:6px 0}#log .u{color:var(--g)}
.err{padding:16px;text-align:center}
@media(prefers-reduced-motion:reduce){.blob{animation:none}}
</style></head><body>
<div class="blob" style="width:320px;height:320px;background:#1fb6c9;top:-60px;left:-80px"></div>
<div class="blob" style="width:280px;height:280px;background:#2ee6a6;bottom:5%;right:-90px;animation-delay:-6s"></div>
<canvas id="fx"></canvas>
<div id="app">
<header class="glass"><div><b>EquityScan</b><small>MARKET INTELLIGENCE</small></div><span id="mk">Checking…</span></header>
<main id="view"></main>
<p class="mut" style="text-align:center;margin:20px 0">Data from NSE, may be delayed. Not investment advice.</p>
</div>
<nav id="tabs" class="glass"></nav>
<button id="cb" class="glass">✦</button>
<div id="chat" class="glass"><div id="log"><p class="mut">Ask about the stocks loaded in the app. I won't give buy/sell advice.</p></div><input id="ci" placeholder="Ask something…"></div>
<div id="modal"><div class="box glass" id="mb"></div></div>
<script>
var $=function(s){return document.querySelector(s)};
var TABS=[['dash','Dashboard'],['scr','Screener'],['mkt','Markets'],['wl','Watchlist']];
var tab='dash',ctl=null,rows=[],viewing=null,wl=[];
try{wl=JSON.parse(localStorage.getItem('wl')||'[]')}catch(e){}
function saveWl(){try{localStorage.setItem('wl',JSON.stringify(wl))}catch(e){}}
function n(x,d){return x==null?'–':Number(x).toLocaleString('en-IN',{maximumFractionDigits:d==null?2:d})}
function esc(s){return String(s==null?'':s).replace(/[&<>"]/g,function(c){return{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]})}
function newSignal(){if(ctl)ctl.abort();ctl=new AbortController();return ctl.signal}
function api(p,body,sig){
  var o={signal:sig};if(body){o.method='POST';o.headers={'Content-Type':'application/json'};o.body=JSON.stringify(body)}
  return fetch(p,o).then(function(r){return r.json()}).then(function(j){if(!j.success)throw new Error(j.error&&j.error.message||'Request failed');return j.data});
}
function skel(){return '<div class="sk"></div><div class="sk"></div><div class="sk"></div><div class="sk"></div>'}
function errBox(e,retry){return '<div class="err glass"><p>'+esc(e.message)+'</p><br><button onclick="'+retry+'()">Retry</button></div>'}
function row(s){
  var c=(s.percentChange||0)>=0?'up':'dn',sg=(s.percentChange||0)>=0?'+':'';
  return '<div class="row glass" data-s="'+esc(s.symbol)+'"><b>'+esc(s.symbol)+'</b><span class="p">₹'+n(s.currentPrice)+'</span><small>'+esc(s.companyName||s.sector||'')+(s.source&&s.source!=='provider'?' · '+esc(s.source):'')+'</small><span class="p '+c+'">'+sg+n(s.percentChange)+'%</span></div>';
}
function setView(h){var v=$('#view');if(v)v.innerHTML=h}
function loadRows(sig){return api('/api/stocks',null,sig).then(function(d){rows=d.rows;return rows})}
function isAbort(e){return e&&e.name==='AbortError'}

function dash(){
  setView(skel());
  loadRows(newSignal()).then(function(r){
    if(tab!=='dash')return;
    var by=function(k,dir){return r.slice().sort(function(a,b){return((a[k]||0)-(b[k]||0))*dir}).slice(0,4).map(row).join('')};
    setView('<h3>Top gainers</h3>'+by('percentChange',-1)+'<h3>Top losers</h3>'+by('percentChange',1)+'<h3>Most active</h3>'+by('volume',-1));
  }).catch(function(e){if(!isAbort(e))setView(errBox(e,'dash'))});
}
function mkt(){
  setView('<input id="f" placeholder="Search symbol or company"><div id="list" style="margin-top:12px">'+skel()+'</div>');
  var draw=function(){var f=$('#f'),l=$('#list');if(!f||!l)return;var q=f.value.toLowerCase();
    l.innerHTML=rows.filter(function(s){return(s.symbol+' '+(s.companyName||'')).toLowerCase().indexOf(q)>-1}).map(row).join('')||'<p class="mut">No matches.</p>'};
  var f=$('#f');if(f)f.oninput=draw;
  loadRows(newSignal()).then(function(){if(tab==='mkt')draw()}).catch(function(e){var l=$('#list');if(l&&!isAbort(e))l.innerHTML=errBox(e,'mkt')});
}
function wlv(){
  if(!wl.length){setView('<div class="err glass"><p>Your watchlist is empty.</p><p class="mut">Open any stock and tap “Add to watchlist”.</p></div>');return}
  setView(skel());
  loadRows(newSignal()).then(function(){if(tab!=='wl')return;
    var m=rows.filter(function(s){return wl.indexOf(s.symbol)>-1});
    setView(m.map(row).join('')||'<p class="mut">No data for your symbols yet.</p>');
  }).catch(function(e){if(!isAbort(e))setView(errBox(e,'wlv'))});
}
function scr(){
  setView('<input id="q" placeholder="Try: banks under 1500 rupees near 52-week high"><div style="margin:10px 0"><button id="go">Search</button></div><div id="out"></div>');
  var go=$('#go'),q=$('#q');
  var run=function(){
    var out=$('#out');if(!q||!out||!q.value.trim())return;
    out.innerHTML=skel();
    api('/api/query',{query:q.value.trim()},newSignal()).then(function(d){
      var o=$('#out');if(!o||tab!=='scr')return;var i=d.interpreted,chips='';
      if(i.sector)chips+='<span class="chip">sector: '+esc(i.sector)+'</span>';
      i.filters.forEach(function(f){chips+='<span class="chip">'+esc(f.field)+' '+esc(f.op)+' '+n(f.value)+'</span>'});
      if(i.sortBy)chips+='<span class="chip">sort: '+esc(i.sortBy)+' '+i.sortDir+'</span>';
      var un=i.unsupported.length?'<p class="mut" style="margin:6px 0">Not available in this app: '+esc(i.unsupported.join(', '))+'</p>':'';
      o.innerHTML='<div>'+chips+'</div>'+un+'<h3>'+d.matches+' match(es)</h3>'+(d.results.map(row).join('')||'<p class="mut">Nothing matched.</p>');
      rows=d.results.length?rows:rows;
    }).catch(function(e){var o=$('#out');if(o&&!isAbort(e))o.innerHTML='<div class="err glass">'+esc(e.message)+'</div>'});
  };
  if(go)go.onclick=run;if(q)q.onkeydown=function(e){if(e.key==='Enter')run()};
}
var VIEWS={dash:dash,scr:scr,mkt:mkt,wl:wlv};
function go(t){tab=t;if(ctl)ctl.abort();
  var nav=$('#tabs');if(nav)nav.querySelectorAll('button').forEach(function(b){b.className=b.dataset.t===t?'on':''});
  VIEWS[t]()}
function drawTabs(){var nav=$('#tabs');if(!nav)return;nav.innerHTML=TABS.map(function(t){return'<button data-t="'+t[0]+'">'+t[1]+'</button>'}).join('');
  nav.onclick=function(e){var b=e.target.closest('button');if(b)go(b.dataset.t)}}

/* detail */
function openDetail(sym){
  var m=$('#modal'),b=$('#mb');if(!m||!b)return;
  m.style.display='flex';b.innerHTML='<div class="sk"></div><div class="sk"></div>';
  api('/api/stock/'+encodeURIComponent(sym)).then(function(s){
    if(m.style.display!=='flex')return;viewing=s;
    var pos=s.week52High&&s.week52Low&&s.week52High>s.week52Low?Math.max(0,Math.min(100,(s.currentPrice-s.week52Low)/(s.week52High-s.week52Low)*100)):50;
    var c=(s.change||0)>=0?'up':'dn',on=wl.indexOf(s.symbol)>-1;
    b.innerHTML='<div style="display:flex;justify-content:space-between"><div><b style="font-size:20px">'+esc(s.symbol)+'</b><div class="mut">'+esc(s.companyName||'')+'</div></div><button id="x">✕</button></div>'+
      '<div style="font-size:32px;margin:12px 0">₹'+n(s.currentPrice)+' <span class="'+c+'" style="font-size:16px">'+n(s.change)+' ('+n(s.percentChange)+'%)</span></div>'+
      '<div class="mut">Day range: ₹'+n(s.dayLow)+' – ₹'+n(s.dayHigh)+'</div>'+
      '<div class="mut" style="margin-top:10px">52-week range</div><div class="bar"><i style="left:'+pos+'%"></i></div><div class="mut" style="display:flex;justify-content:space-between"><span>₹'+n(s.week52Low)+'</span><span>₹'+n(s.week52High)+'</span></div>'+
      '<p class="mut" style="margin-top:10px">Volume: '+n(s.volume,0)+' · Updated: '+esc(s.lastUpdated||'–')+' · Data: '+esc(s.dataStatus)+(s.source&&s.source!=='provider'?' · '+esc(s.source):'')+'</p>'+
      '<button id="w" style="margin-top:14px;width:100%">'+(on?'Remove from watchlist':'Add to watchlist')+'</button>';
    var x=$('#x'),w=$('#w');
    if(x)x.onclick=closeDetail;
    if(w)w.onclick=function(){var i=wl.indexOf(s.symbol);if(i>-1)wl.splice(i,1);else wl.push(s.symbol);saveWl();closeDetail();if(tab==='wl')wlv()};
  }).catch(function(e){var bb=$('#mb');if(bb)bb.innerHTML='<div class="err">'+esc(e.message)+'<br><br><button onclick="closeDetail()">Close</button></div>'});
}
function closeDetail(){var m=$('#modal');if(m)m.style.display='none';viewing=null}

/* chat */
function chatInit(){
  var cb=$('#cb'),ch=$('#chat'),ci=$('#ci'),lg=$('#log');if(!cb||!ch||!ci||!lg)return;
  cb.onclick=function(){ch.style.display=ch.style.display==='flex'?'none':'flex'};
  ci.onkeydown=function(e){
    if(e.key!=='Enter'||!ci.value.trim())return;var m=ci.value.trim();ci.value='';
    lg.insertAdjacentHTML('beforeend','<p class="u">'+esc(m)+'</p>');
    var ctx={viewing:viewing,watchlist:wl,loaded:rows.slice(0,25).map(function(s){return{symbol:s.symbol,price:s.currentPrice,pct:s.percentChange,sector:s.sector}})};
    api('/api/chat',{message:m,context:ctx}).then(function(d){lg.insertAdjacentHTML('beforeend','<p>'+esc(d.reply)+'</p>');lg.scrollTop=lg.scrollHeight})
      .catch(function(er){lg.insertAdjacentHTML('beforeend','<p class="dn">'+esc(er.message)+'</p>')});
  };
}

/* water ripples */
var cv=$('#fx'),cx=cv&&cv.getContext('2d'),rp=[],last=0,still=matchMedia('(prefers-reduced-motion:reduce)').matches;
function sz(){if(cv){cv.width=innerWidth;cv.height=innerHeight}}sz();addEventListener('resize',sz);
function add(x,y,a){rp.push({x:x,y:y,r:4,a:a})}
if(!still){
  addEventListener('pointermove',function(e){var t=Date.now();if(t-last>60){last=t;add(e.clientX,e.clientY,.35)}});
  addEventListener('pointerdown',function(e){add(e.clientX,e.clientY,.65)});
  (function loop(){
    if(cx&&!document.hidden){cx.clearRect(0,0,cv.width,cv.height);rp=rp.filter(function(p){return p.a>.015});
      rp.forEach(function(p){p.r+=2.4;p.a*=.955;cx.lineWidth=2;cx.strokeStyle='rgba(180,240,255,'+p.a+')';cx.beginPath();cx.arc(p.x,p.y,p.r,0,6.283);cx.stroke();
        cx.strokeStyle='rgba(255,255,255,'+p.a/2+')';cx.beginPath();cx.arc(p.x,p.y,p.r*.65,0,6.283);cx.stroke()})}
    requestAnimationFrame(loop)})();
}

/* boot */
document.addEventListener('click',function(e){var r=e.target.closest&&e.target.closest('.row');if(r&&r.dataset.s)openDetail(r.dataset.s)});
var mdl=$('#modal');if(mdl)mdl.onclick=function(e){if(e.target===mdl)closeDetail()};
function market(){api('/api/market-status').then(function(d){var m=$('#mk');if(m){m.textContent='Market '+(d.status==='OPEN'?'Open':d.status==='CLOSED'?'Closed':'—');m.className=d.status==='OPEN'?'up':''}}).catch(function(){var m=$('#mk');if(m)m.textContent='Market —'})}
drawTabs();chatInit();market();setInterval(market,60000);go('dash');
</script></body></html>`;

/* ---------- START + BACKGROUND PRE-WARMER ---------- */
app.listen(C.port, () => {
  console.log("[EquityScan] http://localhost:" + C.port + (C.groqKey ? " (AI on)" : " (AI off: set GROQ_API_KEY)"));
  const warm = () => allStocks().catch(() => {});
  warm(); setInterval(warm, C.refresh);
});
