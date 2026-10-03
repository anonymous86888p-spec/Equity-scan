#!/usr/bin/env node
"use strict";
/* EquityScan - single-file full stack (Express backend + Liquid Glass frontend).
   npm install && npm start   |   env: PORT, GROQ_API_KEY, GROQ_MODEL, DAILY_CALL_BUDGET */
const fs = require("fs"), path = require("path"), express = require("express"), cors = require("cors");
const { NseIndia } = require("stock-nse-india");

/* ---------- CONFIG ---------- */
const env = (k, d) => { const v = process.env[k]; const n = Number(v); return v && Number.isFinite(n) ? n : d; };
const C = {
  port: env("PORT", 3000), ttl: env("QUOTE_CACHE_TTL", 240000), conc: env("MAX_CONCURRENCY", 5), timeout: env("NSE_TIMEOUT", 15000), index: process.env.INDEX_NAME || "NIFTY 500",
  refresh: env("REFRESH_INTERVAL_MS", 240000), groqKey: process.env.GROQ_API_KEY || null,
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
const nseBudget = new Budget(".budget-state.json", env("DAILY_CALL_BUDGET", 2500));
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
let lastNseError = null;
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
    volume: num(pi.totalTradedVolume) ?? num(raw.marketDeptOrderBook && raw.marketDeptOrderBook.tradeInfo && raw.marketDeptOrderBook.tradeInfo.totalTradedVolume) ?? num(raw.securityInfo && raw.securityInfo.totalTradedVolume), lastUpdated: pi.lastUpdateTime || null,
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
      try {
        const [det, ti] = await Promise.all([
          tmo(nse.getEquityDetails(sym), C.timeout),
          typeof nse.getEquityTradeInfo === "function" ? tmo(nse.getEquityTradeInfo(sym), C.timeout).catch(() => null) : null,
        ]);
        raw = det;
        if (ti && ti.marketDeptOrderBook && raw && typeof raw === "object" && !raw.marketDeptOrderBook) raw.marketDeptOrderBook = ti.marketDeptOrderBook;
      }
      catch (e) {
        err = e;
        lastNseError = { at: new Date().toISOString(), symbol: sym, status: (e && e.response && e.response.status) || null, message: String((e && e.message) || e).slice(0, 200) };
        console.warn("[EquityScan] NSE fail", sym, lastNseError.status, lastNseError.message);
        if (e && e.response && e.response.status === 403) break;
        await sleep(250 * 2 ** i);
      }
    }
    if (!raw) { if (old) return { ...old, source: "stale" }; throw fail("PROVIDER_UNAVAILABLE", "NSE unavailable: " + (err && err.message), 503); }
    nseBudget.spend();
    const v = norm(sym, raw); if (!v) throw fail("INVALID_PROVIDER_RESPONSE", "Unexpected NSE response.", 502);
    cset(k, v, C.ttl); return { ...v, source: "provider" };
  });
}
/* ---------- BULK UNIVERSE: one NSE call returns every stock in an index ---------- */
let universe = C.U.slice();
let loadedIndex = null, indexError = null;
async function loadIndex(name) {
  if (!nseBudget.can()) throw fail("RATE_LIMITED", "Daily NSE call budget used up.", 429);
  const attempts = [];
  if (typeof nse.getEquityStockIndices === "function") attempts.push(() => nse.getEquityStockIndices(name));
  attempts.push(() => nse.getDataByEndpoint("/api/equity-stockIndices?index=" + encodeURIComponent(name)));
  let raw = null, arr = null, lastE = null;
  for (const run of attempts) {
    try { raw = await tmo(run(), Math.max(C.timeout, 25000)); arr = raw && Array.isArray(raw.data) && raw.data.length > 1 ? raw.data : null; if (arr) break; }
    catch (e) { lastE = e; }
  }
  if (!arr) {
    if (lastE && !raw) throw lastE;
    throw fail("INVALID_PROVIDER_RESPONSE", "Index had no stocks (rows: " + ((raw && Array.isArray(raw.data) && raw.data.length) || 0) + ", keys: " + Object.keys(raw || {}).slice(0, 6).join(",") + ").", 502);
  }
  const syms = [];
  for (const it of arr) {
    const meta = (it && it.meta) || {}, sym = it && (it.symbol || meta.symbol);
    if (!sym || sym === raw.name || sym === name || (it.priority === 1 && !meta.companyName)) continue;
    it.symbol = sym;
    const price = num(it.lastPrice), hi = num(it.yearHigh), lo = num(it.yearLow);
    const k = "s:" + it.symbol, old = stale(k) || {};
    const got = [price, hi, lo].filter((x) => x !== null).length;
    cset(k, {
      symbol: it.symbol, companyName: meta.companyName || old.companyName || null, sector: meta.industry || old.sector || null,
      currentPrice: price, previousClose: num(it.previousClose), change: num(it.change), percentChange: num(it.pChange),
      open: num(it.open), dayHigh: num(it.dayHigh), dayLow: num(it.dayLow), week52High: hi, week52Low: lo,
      volume: num(it.totalTradedVolume), lastUpdated: it.lastUpdateTime || raw.timestamp || null,
      highPercent: price !== null && hi ? Number(((price / hi) * 100).toFixed(2)) : null,
      dataStatus: got === 3 ? "COMPLETE" : got === 0 ? "UNAVAILABLE" : "PARTIAL",
    }, C.ttl);
    syms.push(it.symbol);
  }
  if (!syms.length) throw fail("INVALID_PROVIDER_RESPONSE", "Index had no stocks (rows: " + arr.length + ", keys: " + Object.keys(arr[1] || arr[0] || {}).slice(0, 8).join(",") + ").", 502);
  universe = syms; nseBudget.spend();
  return syms.length;
}

async function loadIndexLadder() {
  let lastE;
  for (const name of [...new Set([C.index, "NIFTY 100", "NIFTY 50"])]) {
    try { const c = await loadIndex(name); loadedIndex = name; return c; }
    catch (e) { lastE = e; indexError = name + ": " + String((e && e.message) || e).slice(0, 120); console.warn("[EquityScan] index", name, "failed:", indexError); }
  }
  throw lastE;
}

/* ---------- YAHOO FINANCE (free, no key) - merged with NSE ---------- */
const YF_LIST = ["ADANIENT","ADANIPORTS","APOLLOHOSP","ASIANPAINT","AXISBANK","BAJAJ-AUTO","BAJFINANCE","BAJAJFINSV","BEL","BHARTIARTL","CIPLA","COALINDIA","DRREDDY","EICHERMOT","ETERNAL","GRASIM","HCLTECH","HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDUNILVR","ICICIBANK","INDUSINDBK","INFY","ITC","JIOFIN","JSWSTEEL","KOTAKBANK","LT","M&M","MARUTI","NESTLEIND","NTPC","ONGC","POWERGRID","RELIANCE","SBILIFE","SBIN","SHRIRAMFIN","SUNPHARMA","TATACONSUM","TATAMOTORS","TATASTEEL","TCS","TECHM","TITAN","TRENT","ULTRACEMCO","WIPRO",
  "ABB","ADANIGREEN","ADANIPOWER","AMBUJACEM","BANKBARODA","BOSCHLTD","CANBK","CHOLAFIN","COLPAL","DABUR","DLF","DIVISLAB","GAIL","GODREJCP","HAVELLS","HAL","ICICIGI","ICICIPRULI","INDIGO","IOC","IRCTC","IRFC","JINDALSTEL","LICI","LODHA","LTIM","MARICO","MUTHOOTFIN","NAUKRI","PFC","PIDILITIND","PNB","RECLTD","SHREECEM","SIEMENS","SRF","TVSMOTOR","TORNTPHARM","UNIONBANK","UNITDSPR","VBL","VEDL","ZYDUSLIFE","BPCL","BERGEPAINT","CGPOWER","MAXHEALTH","POLYCAB","PERSISTENT"];
let yfClient = null, yahooError = null, yahooRun = null, yahooAt = 0, yahooTry = 0;
const yahooSyms = new Set();
async function yahooClient() {
  if (yfClient) return yfClient;
  const mod = await import("yahoo-finance2");
  const Y = mod.default || mod;
  yfClient = typeof Y === "function" ? new Y({ suppressNotices: ["yahooSurvey"] }) : Y;
  return yfClient;
}
let yfQuoteFailAt = 0;
async function loadYahooQuotes() {
  const yf = await yahooClient();
  const syms = [...new Set([...YF_LIST, ...universe])].slice(0, 600);
  let ok = 0;
  for (let i = 0; i < syms.length; i += 100) {
    const chunk = syms.slice(i, i + 100);
    const res = await tmo(yf.quote(chunk.map((x) => x + ".NS"), {}, { validateResult: false }), 25000);
    for (const q of Array.isArray(res) ? res : res ? [res] : []) {
      if (!q || !q.symbol || num(q.regularMarketPrice) === null) continue;
      const sym = String(q.symbol).replace(/\.NS$/, "");
      const price = num(q.regularMarketPrice), hi = num(q.fiftyTwoWeekHigh), lo = num(q.fiftyTwoWeekLow);
      const got = [price, hi, lo].filter((x) => x !== null).length;
      const t = q.regularMarketTime instanceof Date ? q.regularMarketTime.toISOString() : q.regularMarketTime ? new Date(Number(q.regularMarketTime) * 1000).toISOString() : null;
      cset("y:" + sym, {
        symbol: sym, companyName: q.longName || q.shortName || null, sector: null, currentPrice: price,
        previousClose: num(q.regularMarketPreviousClose), change: num(q.regularMarketChange), percentChange: num(q.regularMarketChangePercent),
        open: num(q.regularMarketOpen), dayHigh: num(q.regularMarketDayHigh), dayLow: num(q.regularMarketDayLow), week52High: hi, week52Low: lo,
        volume: num(q.regularMarketVolume), lastUpdated: t, marketCap: num(q.marketCap), pe: num(q.trailingPE),
        highPercent: price !== null && hi ? Number(((price / hi) * 100).toFixed(2)) : null,
        dataStatus: got === 3 ? "COMPLETE" : got === 0 ? "UNAVAILABLE" : "PARTIAL",
      }, C.ttl);
      yahooSyms.add(sym); ok++;
    }
  }
  if (!ok) throw fail("PROVIDER_UNAVAILABLE", "Yahoo returned no quotes.", 503);
  return ok;
}
const YH = { "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Mobile Safari/537.36", Accept: "application/json" };
async function yahooChartRaw(sym, range, interval) {
  const ac = new AbortController(), t = setTimeout(() => ac.abort(), 15000);
  try {
    const r = await fetch("https://query1.finance.yahoo.com/v8/finance/chart/" + encodeURIComponent(sym) + ".NS?range=" + range + "&interval=" + interval, { headers: YH, signal: ac.signal });
    if (!r.ok) throw fail("PROVIDER_UNAVAILABLE", "Yahoo returned " + r.status, 503);
    const res = (((await r.json()) || {}).chart || {}).result;
    if (!res || !res[0]) throw fail("PROVIDER_UNAVAILABLE", "Yahoo had no data for " + sym, 503);
    return res[0];
  } catch (e) { throw e && e.code ? e : fail("PROVIDER_UNAVAILABLE", "Yahoo: " + String((e && e.message) || e).slice(0, 100), 503); }
  finally { clearTimeout(t); }
}
async function loadYahooMeta() {
  const syms = [...new Set(YF_LIST)];
  let ok = 0, lastErr = null;
  await pool(syms, 6, async (sym) => {
    try {
      const m = (await yahooChartRaw(sym, "1d", "1d")).meta || {};
      const price = num(m.regularMarketPrice), prev = num(m.chartPreviousClose ?? m.previousClose), hi = num(m.fiftyTwoWeekHigh), lo = num(m.fiftyTwoWeekLow);
      if (price === null) return;
      const got = [price, hi, lo].filter((x) => x !== null).length, ch = prev ? price - prev : null;
      cset("y:" + sym, {
        symbol: sym, companyName: m.longName || m.shortName || null, sector: null, currentPrice: price, previousClose: prev,
        change: ch === null ? null : Number(ch.toFixed(2)), percentChange: ch === null ? null : Number(((ch / prev) * 100).toFixed(2)),
        open: null, dayHigh: num(m.regularMarketDayHigh), dayLow: num(m.regularMarketDayLow), week52High: hi, week52Low: lo,
        volume: num(m.regularMarketVolume), lastUpdated: m.regularMarketTime ? new Date(Number(m.regularMarketTime) * 1000).toISOString() : null,
        marketCap: null, pe: null, highPercent: hi ? Number(((price / hi) * 100).toFixed(2)) : null,
        dataStatus: got === 3 ? "COMPLETE" : got === 0 ? "UNAVAILABLE" : "PARTIAL",
      }, C.ttl);
      yahooSyms.add(sym); ok++;
    } catch (e) { lastErr = e; }
  });
  if (!ok) throw fail("PROVIDER_UNAVAILABLE", (lastErr && lastErr.message) || "Yahoo returned no quotes.", 503);
  return ok;
}
async function loadYahoo() {
  if (Date.now() - yfQuoteFailAt > 30 * 60000) {
    try { return await loadYahooQuotes(); }
    catch (e) { yfQuoteFailAt = Date.now(); console.warn("[EquityScan] Yahoo batch (crumb) failed, using chart fallback:", String((e && e.message) || e).slice(0, 100)); }
  }
  return loadYahooMeta();
}
function refreshYahoo() {
  if (yahooRun || Date.now() - yahooTry < 30000 || Date.now() - yahooAt < C.ttl) return yahooRun;
  yahooTry = Date.now();
  yahooRun = loadYahoo().then(() => { yahooAt = Date.now(); yahooError = null; })
    .catch((e) => { yahooError = String((e && e.message) || e).slice(0, 140); console.warn("[EquityScan] Yahoo failed:", yahooError); })
    .finally(() => { yahooRun = null; });
  return yahooRun;
}
async function yahooChart(sym, range) {
  const cfg = { "1d": ["1d", "5m"], "1m": ["1mo", "1d"], "6m": ["6mo", "1d"], "1y": ["1y", "1d"] }[range];
  const res = await yahooChartRaw(sym, cfg[0], cfg[1]);
  const ts = res.timestamp || [], q = (res.indicators && res.indicators.quote && res.indicators.quote[0]) || {};
  const pts = [];
  ts.forEach((t, i) => {
    const c = q.close && q.close[i];
    if (c == null) return;
    pts.push(range === "1d" ? [t * 1000 + 19800000, Number(c)] : [t * 1000, Number(c), Number((q.volume && q.volume[i]) || 0)]);
  });
  if (!pts.length) throw fail("PROVIDER_UNAVAILABLE", "No Yahoo chart data.", 503);
  return pts;
}

/* Stale-while-revalidate: always answer instantly from cache; refresh in the background. */
let bg = null, lastBg = 0;
function refreshStale() {
  if (bg || Date.now() - lastBg < 8000) return bg;
  if (universe.every((x) => cget("s:" + x))) return null;
  lastBg = Date.now();
  bg = loadIndexLadder().then(() => { indexError = null; }).catch(async (e) => {
    lastNseError = { at: new Date().toISOString(), symbol: "INDEX", status: (e && e.response && e.response.status) || null, message: String((e && e.message) || e).slice(0, 200) };
    console.warn("[EquityScan] index load failed, falling back to per-stock:", lastNseError.message);
    const need = C.U.filter((x) => !cget("s:" + x));
    await pool(need, C.conc, (x) => getStock(x).catch(() => null));
  }).finally(() => { bg = null; });
  return bg;
}
const have = () => {
  const list = [];
  for (const x of new Set([...universe, ...yahooSyms])) {
    const f = cget("s:" + x), nv = f || stale("s:" + x), yf = cget("y:" + x), yv = yf || stale("y:" + x), base = nv || yv;
    if (!base) continue;
    list.push({ ...base, marketCap: yv ? yv.marketCap : null, pe: yv ? yv.pe : null, via: nv ? "NSE" : "Yahoo", source: nv ? (f ? "cache" : "stale") : (yf ? "cache" : "stale") });
  }
  const vols = list.map((r) => r.volume).filter((v) => v > 0).sort((a, b) => a - b);
  const med = vols.length ? vols[Math.floor(vols.length / 2)] : null;
  return list.map((r) => ({ ...r, relVolume: med && r.volume ? Number((r.volume / med).toFixed(2)) : null }));
};
async function allStocks() {
  let rows = have();
  const ps = [refreshStale(), refreshYahoo()].filter(Boolean);
  if (ps.length && rows.length < universe.length / 2) { await Promise.race([Promise.all(ps), sleep(10000)]); rows = have(); }
  return rows;
}

async function marketStatus() {
  const k = "market", f = cget(k); if (f) return f;
  try {
    const raw = await tmo(nse.getMarketStatus(), 10000);
    const m = Array.isArray(raw && raw.marketState) ? raw.marketState.find((x) => x.market === "Capital Market") || raw.marketState[0] : null;
    return cset(k, { status: m && m.marketStatus ? (/open/i.test(m.marketStatus) ? "OPEN" : "CLOSED") : "UNKNOWN", tradeDate: (m && m.tradeDate) || null }, 60000);
  } catch (e) { return stale(k) || { status: "UNKNOWN", tradeDate: null }; }
}

/* ---------- GROQ MODEL AUTO-PICK (Groq renames/retires models often) ---------- */
let pickedModel = null;
async function pickModel() {
  if (pickedModel) return pickedModel;
  const prefs = [process.env.GROQ_MODEL, "openai/gpt-oss-120b", "openai/gpt-oss-20b", "llama-3.3-70b-versatile", "llama-3.1-8b-instant"].filter(Boolean);
  try {
    const r = await fetch("https://api.groq.com/openai/v1/models", { headers: { Authorization: "Bearer " + C.groqKey } });
    if (r.ok) {
      const ids = (((await r.json()) || {}).data || []).map((m) => m.id);
      const found = prefs.find((x) => ids.includes(x)) || ids.find((id) => !/whisper|orpheus|guard|compound|tts|allam/i.test(id));
      if (found) { console.log("[EquityScan] Groq model:", found); return (pickedModel = found); }
    }
  } catch (e) {}
  return prefs[0];
}

/* ---------- GROQ CLIENT ---------- */
async function groq(messages, maxTokens, retry = true) {
  if (!C.groqKey) throw fail("AI_NOT_CONFIGURED", "GROQ_API_KEY is not set, so AI features are off.", 503);
  if (!aiBudget.can()) throw fail("RATE_LIMITED", "Daily AI budget used up. Try after reset.", 429);
  const ac = new AbortController(), t = setTimeout(() => ac.abort(), 20000);
  let res;
  try {
    res = await fetch("https://api.groq.com/openai/v1/chat/completions", {
      method: "POST", signal: ac.signal,
      headers: { "Content-Type": "application/json", Authorization: "Bearer " + C.groqKey },
      body: JSON.stringify({ model: await pickModel(), messages, max_tokens: maxTokens, temperature: 0 }),
    });
  } catch (e) { throw fail("PROVIDER_UNAVAILABLE", e.name === "AbortError" ? "AI timed out." : e.message, 503); }
  finally { clearTimeout(t); }
  if (res.status === 401) throw fail("PROVIDER_UNAVAILABLE", "Groq rejected the API key.", 503);
  if (res.status === 429) throw fail("RATE_LIMITED", "Groq rate limit hit.", 429);
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    if (retry && (res.status === 404 || (res.status === 400 && /model/i.test(body)))) { pickedModel = null; return groq(messages, maxTokens, false); }
    throw fail("PROVIDER_UNAVAILABLE", "Groq error " + res.status + ": " + body.slice(0, 150), 503);
  }
  aiBudget.spend();
  const j = await res.json(), c = j && j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.content;
  if (!c) throw fail("INVALID_PROVIDER_RESPONSE", "Empty AI reply.", 502);
  return c.trim();
}

/* ---------- NATURAL-LANGUAGE QUERY ---------- */
const FIELDS = { currentPrice: "price in INR", percentChange: "today's % change", volume: "shares traded today", week52High: "52-week high", week52Low: "52-week low", highPercent: "price as % of 52-week high (0-100)", relVolume: "volume relative to the typical (median) stock in the app; 1 = typical, 2 = twice as busy", marketCap: "market capitalisation in INR (1 crore = 10000000, 1 lakh crore = 1000000000000)", pe: "trailing price/earnings ratio" };
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
  const total = rows.length;
  rows = rows.slice(0, spec.limit || 100);
  return { interpreted: spec, matches: total, results: rows };
}

/* ---------- ROUTES ---------- */
const app = express();
app.use(cors()); app.use(express.json({ limit: "50kb" }));
const h = (fn) => async (q, r) => {
  try { r.json({ success: true, data: await fn(q) }); }
  catch (e) { r.status(e.status || 500).json({ success: false, error: { code: e.code || "INTERNAL_ERROR", message: e.message || "Internal error." } }); }
};
app.get("/api/health", h(async () => ({ ok: true, ai: !!C.groqKey, nseBudgetUsed: nseBudget.u, aiBudgetUsed: aiBudget.u, lastNseError })));
app.get("/api/debug/nse", h(async (q) => {
  const sym = String(q.query.symbol || "TCS").toUpperCase().replace(/[^A-Z0-9&-]/g, "").slice(0, 20) || "TCS";
  const out = { symbol: sym, lib: null };
  try { out.lib = require("stock-nse-india/package.json").version; } catch (e) {}
  const fmt = (e) => ({ ok: false, status: (e && e.response && e.response.status) || null, code: (e && e.code) || null, message: String((e && e.message) || e).slice(0, 200) });
  let t = Date.now();
  try {
    const raw = await tmo(nse.getEquityDetails(sym), 12000);
    out.equity = { ok: true, ms: Date.now() - t, topKeys: Object.keys(raw || {}), priceInfoKeys: Object.keys((raw && raw.priceInfo) || {}), lastPrice: raw && raw.priceInfo ? raw.priceInfo.lastPrice : null };
  } catch (e) { out.equity = { ...fmt(e), ms: Date.now() - t }; }
  t = Date.now();
  try {
    const m = await tmo(nse.getMarketStatus(), 12000);
    out.market = { ok: true, ms: Date.now() - t, keys: Object.keys(m || {}) };
  } catch (e) { out.market = { ...fmt(e), ms: Date.now() - t }; }
  t = Date.now();
  try {
    const r = typeof nse.getEquityStockIndices === "function" ? await tmo(nse.getEquityStockIndices(C.index), 25000) : await tmo(nse.getDataByEndpoint("/api/equity-stockIndices?index=" + encodeURIComponent(C.index)), 25000);
    out.index = { ok: true, name: C.index, ms: Date.now() - t, count: Array.isArray(r && r.data) ? r.data.length : null, sampleKeys: r && r.data && r.data[1] ? Object.keys(r.data[1]) : null, hasGetter: typeof nse.getEquityStockIndices === "function" };
  } catch (e) { out.index = { ...fmt(e), ms: Date.now() - t, name: C.index }; }
  t = Date.now();
  try { const m = (await yahooChartRaw(sym, "1d", "1d")).meta || {}; out.yahooChart = { ok: true, ms: Date.now() - t, price: m.regularMarketPrice, volume: m.regularMarketVolume }; }
  catch (e) { out.yahooChart = { ...fmt(e), ms: Date.now() - t }; }
  t = Date.now();
  try { const yq = await tmo((await yahooClient()).quote([sym + ".NS"], {}, { validateResult: false }), 20000); const q0 = Array.isArray(yq) ? yq[0] : yq; out.yahooQuote = { ok: !!q0, ms: Date.now() - t, marketCap: q0 ? q0.marketCap : null, pe: q0 ? q0.trailingPE : null }; }
  catch (e) { out.yahooQuote = { ...fmt(e), ms: Date.now() - t }; }
  out.yahooSymbols = yahooSyms.size; out.yahooError = yahooError;
  out.universeSize = universe.length;
  out.lastNseError = lastNseError; out.nseBudgetUsed = nseBudget.u;
  return out;
}));
app.get("/api/debug/raw", h(async (q) => {
  const pth = String(q.query.path || "");
  if (!pth.startsWith("/api/") || pth.length > 200) throw fail("INVALID_PARAMETERS", 'Use ?path=/api/... (NSE endpoint path).', 400);
  const t = Date.now();
  try {
    const r = await tmo(nse.getDataByEndpoint(pth), 25000);
    const txt = JSON.stringify(r) || "";
    return { ok: true, ms: Date.now() - t, type: Array.isArray(r) ? "array" : typeof r, keys: r && typeof r === "object" ? Object.keys(r).slice(0, 15) : null, dataRows: r && Array.isArray(r.data) ? r.data.length : null, bytes: txt.length, preview: txt.slice(0, 500) };
  } catch (e) { return { ok: false, ms: Date.now() - t, status: (e && e.response && e.response.status) || null, message: String((e && e.message) || e).slice(0, 200) }; }
}));
app.get("/api/market-status", h(marketStatus));
app.get("/api/stocks", h(async () => {
  refreshStale(); refreshYahoo();
  const rows = have(), mix = { NSE: 0, Yahoo: 0 };
  rows.forEach((r) => { mix[r.via]++; });
  return {
    rows, loading: Math.max(0, universe.length - rows.length), total: Math.max(universe.length, rows.length), mix,
    index: loadedIndex && universe.length > C.U.length ? loadedIndex : null, indexError,
    yahooError: yahooSyms.size ? null : yahooError, error: rows.length ? null : lastNseError,
  };
}));

app.get("/api/stock/:symbol", h(async (q) => {
  const sym = String(q.params.symbol || "").trim().toUpperCase();
  if (!/^[A-Z0-9&-]{1,20}$/.test(sym)) throw fail("INVALID_SYMBOL", '"' + sym + '" is not a valid NSE symbol.', 400);
  const k = "s:" + sym, f = cget(k), st = f || stale(k);
  if (st) { if (!f) getStock(sym).catch(() => {}); return { ...st, source: f ? "cache" : "stale", via: "NSE" }; }
  const y = cget("y:" + sym) || stale("y:" + sym);
  if (y) { getStock(sym).catch(() => {}); return { ...y, source: "cache", via: "Yahoo" }; }
  return getStock(sym);
}));
app.get("/api/chart/:symbol", h(async (q) => {
  const sym = String(q.params.symbol || "").trim().toUpperCase();
  if (!/^[A-Z0-9&-]{1,20}$/.test(sym)) throw fail("INVALID_SYMBOL", '"' + sym + '" is not a valid NSE symbol.', 400);
  const range = ["1d", "1m", "6m", "1y"].includes(q.query.range) ? q.query.range : "1d";
  const k = "c:" + sym + ":" + range, hit = cget(k);
  if (hit) return hit;
  return dedupe(k, async () => {
    if (!nseBudget.can()) { const old = stale(k); if (old) return old; throw fail("RATE_LIMITED", "Daily NSE call budget used up.", 429); }
    let pts;
    try {
      if (range === "1d") {
        const raw = await tmo(nse.getEquityIntradayData(sym), C.timeout);
        const g = (raw && (raw.grapthData || raw.graphData)) || [];
        pts = g.map((r) => [Number(r[0]), Number(r[1])]).filter((r) => Number.isFinite(r[0]) && Number.isFinite(r[1]));
      } else {
        const days = { "1m": 31, "6m": 183, "1y": 366 }[range];
        const raw = await tmo(nse.getEquityHistoricalData(sym, { start: new Date(Date.now() - days * 864e5), end: new Date() }), Math.max(C.timeout, 25000));
        const pages = Array.isArray(raw) ? raw : [raw];
        const rows = [].concat(...pages.map((pg) => (pg && pg.data) || []));
        pts = rows.map((r) => [Date.parse(r.CH_TIMESTAMP || r.mTIMESTAMP || r.date), Number(r.CH_CLOSING_PRICE ?? r.close), Number(r.CH_TOT_TRADED_QTY ?? r.volume)])
          .filter((r) => Number.isFinite(r[0]) && Number.isFinite(r[1])).sort((a, b) => a[0] - b[0])
          .map((r) => (Number.isFinite(r[2]) ? r : [r[0], r[1]]));
      }
    } catch (e) {
      const old = stale(k); if (old) return old;
      try { pts = await yahooChart(sym, range); }
      catch (e2) { throw e && e.code ? e : fail("PROVIDER_UNAVAILABLE", "Chart data unavailable: " + String((e && e.message) || e).slice(0, 120), 503); }
    }
    if (!pts.length) { try { pts = await yahooChart(sym, range); } catch (e3) {} }
    if (pts.length > 120) {
      const step = Math.ceil(pts.length / 120), out = [];
      for (let i = 0; i < pts.length; i += step) {
        const ch = pts.slice(i, i + step), last = ch[ch.length - 1];
        out.push(last.length > 2 ? [last[0], last[1], ch.reduce((a, r) => a + (r[2] || 0), 0)] : [last[0], last[1]]);
      }
      pts = out;
    }
    nseBudget.spend();
    return cset(k, { symbol: sym, range, points: pts }, range === "1d" ? 120000 : 6 * 3600000);
  });
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
html{background:#061a2e}
*{box-sizing:border-box;margin:0}
body{font:15px system-ui,-apple-system,sans-serif;color:var(--t);background:linear-gradient(160deg,#061a2e,#0b3b4f 60%,#0a5560);min-height:100vh;padding-bottom:110px;overflow-x:hidden}
#app{position:relative;z-index:2;max-width:760px;margin:0 auto;padding:env(safe-area-inset-top) 14px 0}
.glass{background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.14);border-radius:22px;box-shadow:inset 0 1px 0 rgba(255,255,255,.18)}
header.glass,#tabs,#chat{-webkit-backdrop-filter:blur(14px);backdrop-filter:blur(14px);background:rgba(10,40,60,.72)}
#mb{background:rgba(10,40,60,.96)}
header{display:flex;justify-content:space-between;align-items:center;padding:12px 16px;margin:12px 0;position:sticky;top:8px;z-index:5}
header b{font-size:18px}header small{display:block;color:var(--m);font-size:10px;letter-spacing:2px}
#mk{font-size:12px;padding:5px 10px;border-radius:99px;background:rgba(255,255,255,.12)}
.up{color:var(--g)}.dn{color:var(--r)}.mut{color:var(--m);font-size:12px}
.row{display:grid;grid-template-columns:1fr auto;gap:2px 10px;padding:12px 16px;margin-bottom:10px;cursor:pointer;transition:transform .15s ease;-webkit-tap-highlight-color:transparent;content-visibility:auto;contain-intrinsic-size:64px}
.row:active{transform:scale(.97)}.row small{color:var(--m);font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.row .p{text-align:right}
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
body{font-variant-numeric:tabular-nums}
.row{display:grid;grid-template-columns:1fr auto;gap:6px 12px;align-items:center}
.row .l b{font-size:16px}.row .l small{display:block;margin-top:2px}
.row .r{text-align:right;display:flex;flex-direction:column;align-items:flex-end;gap:4px}
.pr{font-size:16px;font-weight:600}
.pill{font-size:12px;font-weight:600;padding:2px 8px;border-radius:99px;background:rgba(255,255,255,.1)}
.pill.up{background:rgba(46,230,166,.16)}.pill.dn{background:rgba(255,107,129,.16)}
.vol{grid-column:1/-1;position:relative;height:18px;border-radius:9px;background:rgba(255,255,255,.07);overflow:hidden}
.vol i{position:absolute;left:0;top:0;bottom:0;border-radius:9px;background:linear-gradient(90deg,rgba(31,182,201,.55),rgba(46,230,166,.55))}
.vol.hot i{background:linear-gradient(90deg,rgba(255,184,77,.7),rgba(255,107,129,.7))}
.vol em{position:relative;font-style:normal;font-size:11px;line-height:18px;padding-left:8px}
.vol.big{height:10px;margin:6px 0}
.sum{display:flex;justify-content:space-around;text-align:center;padding:14px 8px;margin-bottom:8px}
.sum small{display:block;color:var(--m);font-size:11px}.sum b{font-size:20px}
.split{height:6px;border-radius:9px;background:var(--r);margin:0 4px 10px;overflow:hidden}.split i{display:block;height:100%;background:var(--g)}
.chips{display:flex;gap:8px;overflow-x:auto;padding:4px 0 10px;scrollbar-width:none}.chips::-webkit-scrollbar{display:none}
.chips button{white-space:nowrap;padding:7px 12px;font-size:13px;border-radius:99px}
.chips button.on{background:rgba(46,230,166,.22);border-color:rgba(46,230,166,.5)}
#rg{padding-bottom:6px}#rg button{padding:5px 12px;font-size:12px}
#tabs button{display:flex;flex-direction:column;align-items:center;gap:2px;font-size:11px}#tabs button span{font-size:17px;line-height:1}
</style></head><body>
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
var TABS=[['dash','Dashboard','▦'],['scr','Screener','⌕'],['mkt','Markets','≋'],['wl','Watchlist','★']];
var maxVol=1,msort='volume',mlim=50;
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
function mcf(v){if(v==null)return '–';if(v>=1e12)return (v/1e12).toFixed(2)+' L Cr';return (v/1e7).toLocaleString('en-IN',{maximumFractionDigits:0})+' Cr'}
function vf(v){if(v==null)return '–';if(v>=1e7)return (v/1e7).toFixed(2)+' Cr';if(v>=1e5)return (v/1e5).toFixed(2)+' L';if(v>=1e3)return (v/1e3).toFixed(1)+' K';return String(v)}
function calcMax(){maxVol=Math.max.apply(null,rows.map(function(v){return v.volume||0}).concat([1]))}
function row(s){
  var up=(s.percentChange||0)>=0,rel=s.relVolume,w=Math.min(100,(s.volume||0)/maxVol*100);
  return '<div class="row glass" data-s="'+esc(s.symbol)+'"><div class="l"><b>'+esc(s.symbol)+'</b><small>'+esc(s.companyName||s.sector||'')+(s.via==='Yahoo'?' · Yahoo':'')+(s.source==='stale'?' · updating':'')+'</small></div>'+
    '<div class="r"><span class="pr">₹'+n(s.currentPrice)+'</span><span class="pill '+(up?'up':'dn')+'">'+(up?'+':'')+n(s.percentChange)+'%</span></div>'+
    '<div class="vol'+(rel>=2?' hot':'')+'"><i style="width:'+w+'%"></i><em>Vol '+vf(s.volume)+(rel>=1.5?' · '+rel.toFixed(1)+'× busy':'')+'</em></div></div>';
}
function setView(h){var v=$('#view');if(v)v.innerHTML=h}
function loadRows(sig){return api('/api/stocks',null,sig).then(function(d){rows=d.rows;return rows})}
function isAbort(e){return e&&e.name==='AbortError'}

function poll(sig,render,tries){
  return api('/api/stocks',null,sig).then(function(d){
    rows=d.rows;calcMax();render(d);
    if(d.loading>0&&(tries||0)<24&&!sig.aborted)return new Promise(function(r){setTimeout(r,2500)}).then(function(){if(!sig.aborted)return poll(sig,render,(tries||0)+1)});
  });
}
function more(d){return d&&d.loading>0&&d.rows.length?'<p class="mut" style="text-align:center">Loading '+d.loading+' more…</p>':''}
function waiting(d,retry){return d.error?errBox(new Error('NSE is not responding ('+(d.error.status||d.error.message)+'). Retrying…'),retry):skel()}
function dash(){
  setView(skel());var sig=newSignal();
  poll(sig,function(d){
    if(tab!=='dash')return;var r=d.rows;
    if(!r.length){setView(waiting(d,'dash'));return}
    var adv=0,dec=0,tot=0;r.forEach(function(x){if(x.percentChange>0)adv++;else if(x.percentChange<0)dec++;tot+=x.volume||0});
    var pct=adv+dec?adv/(adv+dec)*100:50;
    var by=function(k,dir,min){return r.filter(function(x){return min==null||(x[k]||0)>=min}).sort(function(a,b){return((a[k]||0)-(b[k]||0))*dir}).slice(0,4).map(row).join('')};
    var surge=by('relVolume',-1,1.5);
    setView('<div class="sum glass"><div><small>Advancing</small><b class="up">'+adv+'</b></div><div><small>Declining</small><b class="dn">'+dec+'</b></div><div><small>Total volume</small><b>'+vf(tot)+'</b></div></div><div class="split"><i style="width:'+pct+'%"></i></div>'+
      '<p class="mut" style="text-align:center;margin:-4px 0 6px">Tracking '+r.length+' stocks'+(d.index?' · '+esc(d.index):'')+(d.mix&&d.mix.Yahoo?' · NSE '+d.mix.NSE+' + Yahoo '+d.mix.Yahoo:'')+'</p>'+(d.yahooError?'<p class="mut" style="text-align:center;margin:0 0 8px">Yahoo unavailable ('+esc(d.yahooError)+')</p>':'')+(d.indexError?'<p class="mut" style="text-align:center;margin:0 0 8px">Full list unavailable ('+esc(d.indexError)+')</p>':'')+'<h3>Top gainers</h3>'+by('percentChange',-1)+'<h3>Top losers</h3>'+by('percentChange',1)+'<h3>Most active by volume</h3>'+by('volume',-1)+(surge?'<h3>Volume surge (busier than usual)</h3>'+surge:'')+more(d));
  }).catch(function(e){if(!isAbort(e))setView(errBox(e,'dash'))});
}
function mkt(){
  var SORTS=[['volume','Volume'],['percentChange','% Change'],['currentPrice','Price'],['symbol','A–Z']];
  setView('<input id="f" placeholder="Search symbol or company"><div class="chips" id="sc" style="margin-top:10px"></div><div id="list">'+skel()+'</div>');
  var lastD=null;
  var draw=function(d){if(d)lastD=d;var f=$('#f'),l=$('#list'),sc=$('#sc');if(!f||!l||!sc)return;var q=f.value.toLowerCase();
    sc.innerHTML=SORTS.map(function(x){return'<button data-k="'+x[0]+'" class="'+(msort===x[0]?'on':'')+'">'+x[1]+'</button>'}).join('');
    var list=rows.filter(function(x){return(x.symbol+' '+(x.companyName||'')).toLowerCase().indexOf(q)>-1});
    list.sort(function(a,b){return msort==='symbol'?a.symbol.localeCompare(b.symbol):(b[msort]||0)-(a[msort]||0)});
    var cap=q?100:mlim,h=list.slice(0,cap).map(row).join('');
    var btn=list.length>cap?'<button id="mo" style="width:100%;margin:4px 0 12px">Show more ('+(list.length-cap)+' left)</button>':'';
    l.innerHTML=(h?'<p class="mut" style="margin:0 4px 8px">'+list.length+' stocks</p>':'')+(h||(!rows.length&&lastD?waiting(lastD,'mkt'):'<p class="mut">No matches.</p>'))+btn+more(lastD)};
  var f=$('#f'),sc=$('#sc');if(f)f.oninput=function(){draw()};
  if(sc)sc.onclick=function(e){var b=e.target.closest('button');if(b){msort=b.dataset.k;mlim=50;draw()}};
  var lst=$('#list');if(lst)lst.onclick=function(e){if(e.target.closest&&e.target.closest('#mo')){mlim+=50;draw()}};
  poll(newSignal(),function(d){if(tab==='mkt')draw(d)}).catch(function(e){var l=$('#list');if(l&&!isAbort(e))l.innerHTML=errBox(e,'mkt')});
}
function wlv(){
  if(!wl.length){setView('<div class="err glass"><p>Your watchlist is empty.</p><p class="mut">Open any stock and tap “Add to watchlist”.</p></div>');return}
  setView(skel());
  poll(newSignal(),function(d){if(tab!=='wl')return;
    var m=rows.filter(function(s){return wl.indexOf(s.symbol)>-1});
    setView(m.length?m.map(row).join('')+more(d):waiting(d,'wlv'));
  }).catch(function(e){if(!isAbort(e))setView(errBox(e,'wlv'))});
}
function scr(){
  setView('<input id="q" placeholder="Try: busy banks under 1500 rupees"><div style="margin:10px 0"><button id="go">Search</button></div><div class="chips" id="eg"></div><div id="out"></div>');
  var EX=['High volume stocks','Top gainers above 2%','Busy banks','Near 52-week high','Quiet stocks under 1000 rupees'];
  var go=$('#go'),q=$('#q');
  var run=function(){
    var out=$('#out');if(!q||!out||!q.value.trim())return;
    out.innerHTML=skel();
    api('/api/query',{query:q.value.trim()},newSignal()).then(function(d){
      var o=$('#out');if(!o||tab!=='scr')return;var i=d.interpreted,chips='';if(d.results.length)maxVol=Math.max.apply(null,d.results.map(function(v){return v.volume||0}).concat([1]));
      if(i.sector)chips+='<span class="chip">sector: '+esc(i.sector)+'</span>';
      i.filters.forEach(function(f){chips+='<span class="chip">'+esc(f.field)+' '+esc(f.op)+' '+n(f.value)+'</span>'});
      if(i.sortBy)chips+='<span class="chip">sort: '+esc(i.sortBy)+' '+i.sortDir+'</span>';
      var un=i.unsupported.length?'<p class="mut" style="margin:6px 0">Not available in this app: '+esc(i.unsupported.join(', '))+'</p>':'';
      o.innerHTML='<div>'+chips+'</div>'+un+'<h3>'+d.matches+' match(es)'+(d.matches>d.results.length?' · showing top '+d.results.length:'')+'</h3>'+(d.results.map(row).join('')||'<p class="mut">Nothing matched.</p>');
      rows=d.results.length?rows:rows;
    }).catch(function(e){var o=$('#out');if(o&&!isAbort(e))o.innerHTML='<div class="err glass">'+esc(e.message)+'</div>'});
  };
  if(go)go.onclick=run;if(q)q.onkeydown=function(e){if(e.key==='Enter')run()};
  var eg=$('#eg');if(eg){eg.innerHTML=EX.map(function(x){return'<button>'+x+'</button>'}).join('');eg.onclick=function(e){var b=e.target.closest('button');if(b&&q){q.value=b.textContent;run()}}}
}
var VIEWS={dash:dash,scr:scr,mkt:mkt,wl:wlv};
function go(t){tab=t;if(ctl)ctl.abort();
  var nav=$('#tabs');if(nav)nav.querySelectorAll('button').forEach(function(b){b.className=b.dataset.t===t?'on':''});
  VIEWS[t]()}
function drawTabs(){var nav=$('#tabs');if(!nav)return;nav.innerHTML=TABS.map(function(t){return'<button data-t="'+t[0]+'"><span>'+t[2]+'</span>'+t[1]+'</button>'}).join('');
  nav.onclick=function(e){var b=e.target.closest('button');if(b)go(b.dataset.t)}}

var crange='1d',cctl=null;
function chartSvg(pts,up){
  var W=320,H=130,P=6,vals=pts.map(function(p){return p[1]}),mn=Math.min.apply(null,vals),mx=Math.max.apply(null,vals),rg=mx-mn||1;
  var X=function(i){return P+i*(W-2*P)/Math.max(1,pts.length-1)},Y=function(v){return P+(mx-v)*(H-2*P-26)/rg};
  var d=pts.map(function(p,i){return(i?'L':'M')+X(i).toFixed(1)+' '+Y(p[1]).toFixed(1)}).join('');
  var col=up?'#2ee6a6':'#ff6b81',mv=Math.max.apply(null,pts.map(function(p){return p[2]||0}).concat([1]));
  var bars=pts[0].length>2?pts.map(function(p,i){var h=(p[2]||0)/mv*22;return'<rect x="'+(X(i)-1).toFixed(1)+'" y="'+(H-h).toFixed(1)+'" width="2" height="'+h.toFixed(1)+'" fill="rgba(255,255,255,.22)"/>'}).join(''):'';
  return '<svg id="cs" viewBox="0 0 '+W+' '+H+'" width="100%" style="touch-action:none;display:block"><defs><linearGradient id="gf" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="'+col+'" stop-opacity=".35"/><stop offset="1" stop-color="'+col+'" stop-opacity="0"/></linearGradient></defs><path d="'+d+'L'+X(pts.length-1).toFixed(1)+' '+(H-26)+'L'+X(0).toFixed(1)+' '+(H-26)+'Z" fill="url(#gf)"/><path d="'+d+'" fill="none" stroke="'+col+'" stroke-width="2" stroke-linejoin="round"/>'+bars+'<line id="cl" y1="0" y2="'+H+'" stroke="rgba(255,255,255,.5)" stroke-dasharray="3 3" style="display:none"/></svg>';
}
function fmtT(t,range){return new Date(t).toLocaleString('en-IN',range==='1d'?{hour:'2-digit',minute:'2-digit',timeZone:'UTC'}:{day:'numeric',month:'short',year:'2-digit'})}
function bindChart(pts,range){
  var sv=$('#cs'),cl=$('#cl'),lb=$('#cv');if(!sv||!cl||!lb)return;
  var W=320,P=6,cnt=pts.length;
  var mv=function(e){var r=sv.getBoundingClientRect(),x=(e.clientX-r.left)/r.width*W,i=Math.max(0,Math.min(cnt-1,Math.round((x-P)/(W-2*P)*(cnt-1))));
    var p=pts[i],xx=P+i*(W-2*P)/Math.max(1,cnt-1);cl.setAttribute('x1',xx);cl.setAttribute('x2',xx);cl.style.display='';
    lb.textContent='₹'+n(p[1])+' · '+fmtT(p[0],range)+(p[2]?' · Vol '+vf(p[2]):'')};
  sv.onpointermove=mv;sv.onpointerdown=mv;
}
function loadChart(sym){
  var rg=$('#rg'),ch=$('#ch'),cv=$('#cv');if(!rg||!ch)return;
  var R=[['1d','1D'],['1m','1M'],['6m','6M'],['1y','1Y']];
  rg.innerHTML=R.map(function(x){return'<button data-r="'+x[0]+'" class="'+(crange===x[0]?'on':'')+'">'+x[1]+'</button>'}).join('');
  rg.onclick=function(e){var b=e.target.closest('button');if(b){crange=b.dataset.r;loadChart(sym)}};
  ch.innerHTML='<div class="sk" style="height:130px"></div>';if(cv)cv.textContent='';
  if(cctl)cctl.abort();cctl=new AbortController();
  api('/api/chart/'+encodeURIComponent(sym)+'?range='+crange,null,cctl.signal).then(function(d){
    var c2=$('#ch'),v2=$('#cv');if(!c2)return;
    if(!d.points.length){c2.innerHTML='<p class="mut">No chart data for this range.</p>';return}
    var a=d.points[0][1],b=d.points[d.points.length-1][1];
    c2.innerHTML=chartSvg(d.points,b>=a);bindChart(d.points,crange);
    if(v2)v2.textContent='Range change: '+(b>=a?'+':'')+((b-a)/a*100).toFixed(2)+'% · touch the chart to inspect';
  }).catch(function(e){var c3=$('#ch');if(c3&&!isAbort(e))c3.innerHTML='<p class="mut">Chart unavailable: '+esc(e.message)+'</p>'});
}
/* detail */
function openDetail(sym){
  var m=$('#modal'),b=$('#mb');if(!m||!b)return;
  m.style.display='flex';b.innerHTML='<div class="sk"></div><div class="sk"></div>';
  api('/api/stock/'+encodeURIComponent(sym)).then(function(s){
    if(m.style.display!=='flex')return;viewing=s;
    var pos=s.week52High&&s.week52Low&&s.week52High>s.week52Low?Math.max(0,Math.min(100,(s.currentPrice-s.week52Low)/(s.week52High-s.week52Low)*100)):50;
    var c=(s.change||0)>=0?'up':'dn',on=wl.indexOf(s.symbol)>-1;var rr=rows.filter(function(x){return x.symbol===s.symbol})[0],rel=rr&&rr.relVolume,vw=Math.min(100,(s.volume||0)/maxVol*100);
    b.innerHTML='<div style="display:flex;justify-content:space-between"><div><b style="font-size:20px">'+esc(s.symbol)+'</b><div class="mut">'+esc(s.companyName||'')+'</div></div><button id="x">✕</button></div>'+
      '<div style="font-size:32px;margin:12px 0">₹'+n(s.currentPrice)+' <span class="'+c+'" style="font-size:16px">'+n(s.change)+' ('+n(s.percentChange)+'%)</span></div>'+
      '<div class="mut">Day range: ₹'+n(s.dayLow)+' – ₹'+n(s.dayHigh)+'</div>'+
      '<div class="chips" id="rg" style="margin-top:12px"></div><div id="ch" style="min-height:130px"></div><div id="cv" class="mut" style="font-size:12px;min-height:16px;margin-bottom:4px"></div><div class="mut" style="margin-top:10px">52-week range</div><div class="bar"><i style="left:'+pos+'%"></i></div><div class="mut" style="display:flex;justify-content:space-between"><span>₹'+n(s.week52Low)+'</span><span>₹'+n(s.week52High)+'</span></div>'+
      (rr&&(rr.marketCap||rr.pe)?'<p class="mut" style="margin-top:10px">'+(rr.marketCap?'Market cap ₹'+mcf(rr.marketCap):'')+(rr.marketCap&&rr.pe?' · ':'')+(rr.pe?'P/E '+n(rr.pe):'')+'</p>':'')+'<div class="mut" style="margin-top:12px">Volume today</div><div style="display:flex;justify-content:space-between;align-items:baseline"><b style="font-size:20px">'+vf(s.volume)+'</b><span class="mut">'+(rel?rel.toFixed(1)+'× typical':'')+'</span></div><div class="vol big'+(rel>=2?' hot':'')+'"><i style="width:'+vw+'%"></i></div><p class="mut" style="margin-top:10px">Updated: '+esc(s.lastUpdated||'–')+' · Data: '+esc(s.dataStatus)+(s.source==='stale'?' · updating':'')+'</p>'+
      '<button id="w" style="margin-top:14px;width:100%">'+(on?'Remove from watchlist':'Add to watchlist')+'</button>';
    loadChart(s.symbol);
    var x=$('#x'),w=$('#w');
    if(x)x.onclick=closeDetail;
    if(w)w.onclick=function(){var i=wl.indexOf(s.symbol);if(i>-1)wl.splice(i,1);else wl.push(s.symbol);saveWl();closeDetail();if(tab==='wl')wlv()};
  }).catch(function(e){var bb=$('#mb');if(bb)bb.innerHTML='<div class="err">'+esc(e.message)+'<br><br><button onclick="closeDetail()">Close</button></div>'});
}
function closeDetail(){if(cctl)cctl.abort();var m=$('#modal');if(m)m.style.display='none';viewing=null}

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

/* boot */
document.addEventListener('click',function(e){var r=e.target.closest&&e.target.closest('.row');if(r&&r.dataset.s)openDetail(r.dataset.s)});
var mdl=$('#modal');if(mdl)mdl.onclick=function(e){if(e.target===mdl)closeDetail()};
function market(){api('/api/market-status').then(function(d){var m=$('#mk');if(m){m.textContent='Market '+(d.status==='OPEN'?'Open':d.status==='CLOSED'?'Closed':'—');m.className=d.status==='OPEN'?'up':''}}).catch(function(){var m=$('#mk');if(m)m.textContent='Market —'})}
drawTabs();chatInit();market();setInterval(market,60000);go('dash');
</script></body></html>`;

/* ---------- START + BACKGROUND PRE-WARMER ---------- */
app.listen(C.port, () => {
  console.log("[EquityScan] http://localhost:" + C.port + (C.groqKey ? " (AI on)" : " (AI off: set GROQ_API_KEY)"));
  const warm = async () => {
    try {
      const m = await marketStatus();
      if (m.status !== "CLOSED" || !have().length) { refreshStale(); refreshYahoo(); }
    } catch (e) {}
  };
  warm(); setInterval(warm, C.refresh);
});
