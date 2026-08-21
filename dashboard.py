"""
Веб-дашборд статуса бота — тот самый "сайт", через который следишь за ботом,
не заходя на ПК. Одна страница: режим, депозит/PnL, график эквити, открытые
позиции, история сделок, лог событий, и кнопка аварийной остановки (kill switch).

Отдаёт также /api/status (JSON) для программного доступа и
/api/kill, /api/resume для управления kill switch.
"""
import logging
import time
from collections import deque

from aiohttp import web

log = logging.getLogger("dashboard")

MAX_EVENTS = 200

# Новое калибровочное логирование (WALL_CANDIDATE/WALL_OUTCOME на каждый тик
# стакана, latency на каждый сигнал) - полезно в логах Render для анализа, но
# на дашборде это просто мусор, который топит реально важные события (сделки,
# ошибки, kill switch). Фильтруем на уровне перехвата, а не на клиенте, чтобы
# не гонять лишний JSON туда-обратно каждые 3 секунды.
_NOISY_SUBSTRINGS = ("WALL_CANDIDATE", "WALL_OUTCOME", "latency signal_age_ms")


class EventLog:
    def __init__(self):
        self.events = deque(maxlen=MAX_EVENTS)

    def add(self, level: str, message: str):
        self.events.append({"ts": time.time(), "level": level, "message": message})


class DashboardLogHandler(logging.Handler):
    """Перехватывает логи бота в кольцевой буфер для показа на дашборде
    (кроме калибровочного шума - см. _NOISY_SUBSTRINGS)."""

    def __init__(self, event_log: EventLog):
        super().__init__()
        self.event_log = event_log

    def emit(self, record):
        try:
            msg = self.format(record)
            if any(s in msg for s in _NOISY_SUBSTRINGS):
                return
            self.event_log.add(record.levelname, msg)
        except Exception:
            pass


class Dashboard:
    def __init__(self, risk, order_manager, trade_log, kill_switch, port: int, mode: str, symbols):
        self.risk = risk
        self.orders = order_manager
        self.trade_log = trade_log
        self.kill_switch = kill_switch
        self.port = port
        self.mode = mode
        self.symbols = symbols
        self.events = EventLog()
        self.started_at = time.time()

        handler = DashboardLogHandler(self.events)
        handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
        logging.getLogger().addHandler(handler)

        self.app = web.Application()
        self.app.router.add_get("/", self.handle_index)
        self.app.router.add_get("/api/status", self.handle_status)
        self.app.router.add_post("/api/kill", self.handle_kill)
        self.app.router.add_post("/api/resume", self.handle_resume)
        self.app.router.add_post("/api/reset_account", self.handle_reset_account)

    def _status_dict(self):
        positions = []
        for sym, pos in self.orders.positions.items():
            positions.append({
                "symbol": sym, "side": pos.side, "size": round(pos.filled_size, 6),
                "avg_entry": round(pos.avg_entry, 4), "sl": round(pos.current_sl_price, 4),
                "reduced": pos.reduced_once, "edge_reverse_streak": pos.edge_reverse_streak,
                "opened_at": pos.opened_at, "signal_type": pos.signal_type,
            })
        return {
            "mode": self.mode,
            "symbols": self.symbols,
            "uptime_sec": round(time.time() - self.started_at),
            "equity_usd": round(self.risk.equity, 2),
            "day_start_equity_usd": round(self.risk.day_start_equity, 2),
            "consecutive_losses": self.risk.consecutive_losses,
            "kill_switch_active": self.kill_switch.active,
            "kill_switch_reason": self.kill_switch.reason,
            "positions": positions,
            "stats": self.trade_log.stats(),
            "recent_trades": [t.__dict__ for t in self.trade_log.recent(20)],
            "equity_curve": self.trade_log.equity_curve(self.risk.day_start_equity, self.started_at),
            "events": list(self.events.events)[-60:][::-1],
            "server_ts": time.time(),
        }

    async def handle_status(self, request):
        return web.json_response(self._status_dict())

    async def handle_kill(self, request):
        await self.kill_switch.trigger("manual (dashboard)")
        return web.json_response({"ok": True})

    async def handle_resume(self, request):
        self.kill_switch.reset()
        return web.json_response({"ok": True})

    async def handle_reset_account(self, request):
        """Сброс счёта бота (баланс/позиции/история сделок) с дашборда - см.
        OrderManager.reset_account. Только paper - в live своя реальная биржа,
        "сбросить баланс" там не бывает, и кнопка на дашборде для live не
        показывается вовсе (см. INDEX_HTML), но проверяем и на сервере -
        нельзя полагаться только на то, что скрыто в UI."""
        if self.mode != "paper":
            return web.json_response({"ok": False, "error": "доступно только в paper-режиме"}, status=400)
        await self.orders.reset_account("manual (dashboard)")
        return web.json_response({"ok": True})

    async def handle_index(self, request):
        return web.Response(text=INDEX_HTML, content_type="text/html")

    async def start(self):
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.port)
        await site.start()
        log.info("Дашборд запущен на порту %d", self.port)
        return runner


INDEX_HTML = """<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Lighter DOM-Bot</title>
<style>
  :root {
    color-scheme: dark;
    --bg: #0a0c12;
    --panel: #12151f;
    --panel-2: #171b28;
    --border: #232838;
    --text: #e8eaf1;
    --muted: #7d859c;
    --accent: #5b8cff;
    --green: #34d399;
    --red: #f2596b;
    --amber: #f0b656;
  }
  * { box-sizing: border-box; }
  html, body { margin:0; padding:0; background:var(--bg); color:var(--text);
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; }
  body { padding: 20px 20px 60px; max-width: 1080px; margin: 0 auto; }

  .topbar { display:flex; align-items:center; justify-content:space-between; margin-bottom:18px;
    flex-wrap:wrap; gap:10px; }
  .brand { display:flex; align-items:center; gap:10px; }
  .brand h1 { font-size:17px; margin:0; font-weight:650; letter-spacing:-.01em; }
  .dot { width:9px; height:9px; border-radius:50%; background:var(--green); flex:none;
    box-shadow:0 0 0 3px rgba(52,211,153,.15); }
  .dot.stale { background:var(--amber); box-shadow:0 0 0 3px rgba(240,182,86,.15); }
  .dot.dead { background:var(--red); box-shadow:0 0 0 3px rgba(242,89,107,.15); }
  .meta { color:var(--muted); font-size:12.5px; }
  .meta b { color:var(--text); font-weight:600; }

  .kill-banner { background:linear-gradient(180deg,#2a1219,#20101a); border:1px solid #4a1f2a;
    color:#ffb3bf; padding:12px 16px; border-radius:12px; margin-bottom:18px; font-size:13.5px;
    display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap; }
  .toolbar { display:flex; justify-content:flex-end; gap:10px; margin-bottom:18px; }

  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:10px; margin-bottom:16px; }
  .card { background:var(--panel); border:1px solid var(--border); border-radius:12px; padding:14px 16px; }
  .card .label { color:var(--muted); font-size:11.5px; text-transform:uppercase; letter-spacing:.05em; }
  .card .value { font-size:21px; font-weight:650; margin-top:6px; letter-spacing:-.01em; }
  .card .sub { font-size:11.5px; color:var(--muted); margin-top:3px; }
  .pos-val { color:var(--green); } .neg-val { color:var(--red); }

  section { background:var(--panel); border:1px solid var(--border); border-radius:12px;
    padding:16px 18px; margin-bottom:14px; }
  section .head { display:flex; align-items:center; justify-content:space-between; margin-bottom:12px; }
  section h2 { font-size:13px; margin:0; color:#c7cbdb; font-weight:600;
    text-transform:uppercase; letter-spacing:.04em; }
  section .head .hint { font-size:11.5px; color:var(--muted); }

  #chartWrap { position:relative; }
  #chartSvg { width:100%; height:120px; display:block; }
  #chartEmpty { color:var(--muted); font-size:13px; padding:30px 0; text-align:center; }

  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { text-align:left; padding:8px 10px; border-bottom:1px solid var(--border); white-space:nowrap; }
  th { color:var(--muted); font-weight:500; font-size:11.5px; text-transform:uppercase; letter-spacing:.03em; }
  tr:last-child td { border-bottom:none; }
  tbody tr:hover { background:rgba(255,255,255,.02); }

  .badge { display:inline-block; padding:2px 9px; border-radius:20px; font-size:11.5px; font-weight:650;
    letter-spacing:.02em; }
  .badge.long { background:rgba(52,211,153,.13); color:var(--green); }
  .badge.short { background:rgba(242,89,107,.13); color:var(--red); }

  button { background:var(--red); color:#fff; border:none; padding:9px 16px; border-radius:8px;
    font-weight:600; cursor:pointer; font-size:13px; transition:opacity .15s; }
  button.resume { background:var(--green); color:#08130d; }
  button.ghost { background:transparent; border:1px solid var(--border); color:var(--muted); }
  button.ghost.reset { border-color:var(--amber); color:var(--amber); }
  button:hover { opacity:.85; }

  .event-line { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px;
    padding:4px 0; color:#a7adc0; border-bottom:1px solid rgba(255,255,255,.03); }
  .event-line .t { color:var(--muted); margin-right:8px; }
  .event-WARNING { color:var(--amber); } .event-ERROR { color:var(--red); }
  #eventsBox { max-height:280px; overflow-y:auto; }
  #eventsBox::-webkit-scrollbar { width:6px; }
  #eventsBox::-webkit-scrollbar-thumb { background:var(--border); border-radius:3px; }

  .empty { color:var(--muted); font-size:13px; padding:14px 0; text-align:center; }
  .footer-note { color:var(--muted); font-size:11.5px; text-align:center; margin-top:20px; }
</style>
</head>
<body>
  <div class="topbar">
    <div class="brand">
      <span class="dot" id="liveDot"></span>
      <h1>Lighter DOM-Bot</h1>
    </div>
    <div class="meta" id="subheader">загрузка...</div>
  </div>

  <div id="killBanner"></div>

  <div class="grid" id="statCards"></div>

  <section>
    <div class="head"><h2>Эквити</h2><div class="hint" id="chartHint"></div></div>
    <div id="chartWrap"><svg id="chartSvg" viewBox="0 0 600 120" preserveAspectRatio="none"></svg>
      <div id="chartEmpty" style="display:none;">пока нет закрытых сделок</div>
    </div>
  </section>

  <section>
    <div class="head"><h2>Открытые позиции</h2></div>
    <div id="positionsBox"><div class="empty">нет открытых позиций</div></div>
  </section>

  <section>
    <div class="head"><h2>Последние сделки</h2></div>
    <div id="tradesBox"><div class="empty">сделок пока нет</div></div>
  </section>

  <section>
    <div class="head"><h2>Лог событий</h2><div class="hint">только сделки/предупреждения/ошибки</div></div>
    <div id="eventsBox"></div>
  </section>

  <div class="toolbar" id="toolbar"></div>
  <div class="footer-note">обновляется каждые 3с</div>

<script>
function fmt(n, d=2) { return (typeof n === 'number') ? n.toFixed(d) : n; }
function timeAgo(sec) {
  const h = Math.floor(sec/3600), m = Math.floor((sec%3600)/60), s = Math.floor(sec%60);
  return (h>0?h+'ч ':'') + (m>0?m+'м ':'') + s+'с';
}
function tsToTime(ts) {
  if (!ts) return '-';
  return new Date(ts*1000).toLocaleString('ru-RU', {hour:'2-digit', minute:'2-digit', second:'2-digit', day:'2-digit', month:'2-digit'});
}
function tsToShortTime(ts) {
  if (!ts) return '-';
  return new Date(ts*1000).toLocaleString('ru-RU', {hour:'2-digit', minute:'2-digit', second:'2-digit'});
}

let lastStatusAt = 0;

async function killSwitch() {
  if (!confirm('Закрыть все позиции и остановить бота?')) return;
  await fetch('/api/kill', {method:'POST'});
  refresh();
}
async function resumeBot() {
  await fetch('/api/resume', {method:'POST'});
  refresh();
}
async function resetAccount() {
  if (!confirm('Сбросить счёт бота?\\n\\nБаланс вернётся к стартовому, все открытые позиции будут закрыты, а ВСЯ история сделок удалится безвозвратно. Это нельзя отменить.')) return;
  const r = await fetch('/api/reset_account', {method:'POST'});
  const j = await r.json().catch(() => ({}));
  if (!r.ok || j.ok === false) { alert('Не удалось сбросить счёт' + (j.error ? ': ' + j.error : '')); }
  refresh();
}

function renderChart(curve) {
  const svg = document.getElementById('chartSvg');
  const empty = document.getElementById('chartEmpty');
  const hint = document.getElementById('chartHint');
  if (!curve || curve.length < 2) {
    svg.style.display = 'none'; empty.style.display = 'block'; hint.textContent = '';
    return;
  }
  svg.style.display = 'block'; empty.style.display = 'none';

  const W = 600, H = 120, PAD = 6;
  const values = curve.map(p => p.equity);
  let min = Math.min(...values), max = Math.max(...values);
  if (min === max) { min -= 1; max += 1; }
  const n = curve.length;
  const x = i => PAD + (i/(n-1)) * (W - PAD*2);
  const y = v => H - PAD - ((v-min)/(max-min)) * (H - PAD*2);

  const first = values[0], last = values[values.length-1];
  const up = last >= first;
  const stroke = up ? 'var(--green)' : 'var(--red)';
  const fillId = up ? 'gGreen' : 'gRed';

  let path = 'M ' + x(0) + ' ' + y(values[0]);
  for (let i=1;i<n;i++) path += ' L ' + x(i) + ' ' + y(values[i]);
  let areaPath = path + ' L ' + x(n-1) + ' ' + H + ' L ' + x(0) + ' ' + H + ' Z';

  svg.innerHTML = `
    <defs>
      <linearGradient id="gGreen" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="#34d399" stop-opacity="0.25"/>
        <stop offset="100%" stop-color="#34d399" stop-opacity="0"/>
      </linearGradient>
      <linearGradient id="gRed" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="#f2596b" stop-opacity="0.25"/>
        <stop offset="100%" stop-color="#f2596b" stop-opacity="0"/>
      </linearGradient>
    </defs>
    <path d="${areaPath}" fill="url(#${fillId})" stroke="none"></path>
    <path d="${path}" fill="none" stroke="${stroke}" stroke-width="1.75" vector-effect="non-scaling-stroke"></path>
  `;
  const deltaPct = first !== 0 ? ((last-first)/Math.abs(first)*100) : 0;
  hint.textContent = `${up?'+':''}${fmt(last-first)}$ (${up?'+':''}${fmt(deltaPct,2)}%) за период`;
  hint.style.color = up ? 'var(--green)' : 'var(--red)';
}

async function refresh() {
  let s;
  try {
    s = await (await fetch('/api/status')).json();
  } catch (e) {
    document.getElementById('liveDot').className = 'dot dead';
    return;
  }
  lastStatusAt = Date.now()/1000;
  document.getElementById('liveDot').className = 'dot';

  document.getElementById('subheader').innerHTML =
    `режим: <b>${s.mode}</b> &nbsp;·&nbsp; ${s.symbols.join(', ')} &nbsp;·&nbsp; аптайм ${timeAgo(s.uptime_sec)}`;

  const killBanner = document.getElementById('killBanner');
  const toolbar = document.getElementById('toolbar');
  let toolbarHtml = '';
  if (s.kill_switch_active) {
    killBanner.innerHTML = `<div class="kill-banner">
      <span>⛔ <b>Kill switch активен</b> (${s.kill_switch_reason}) — новые входы заблокированы</span>
      <button class="resume" onclick="resumeBot()">Возобновить торговлю</button></div>`;
  } else {
    killBanner.innerHTML = '';
    toolbarHtml += `<button class="ghost" onclick="killSwitch()">⛔ Аварийная остановка</button>`;
  }
  // Сброс счёта - только paper (в live своя реальная биржа, сервер тоже это
  // проверяет и отклонит запрос, см. Dashboard.handle_reset_account).
  if (s.mode === 'paper') {
    toolbarHtml += `<button class="ghost reset" onclick="resetAccount()">🔄 Сбросить счёт</button>`;
  }
  toolbar.innerHTML = toolbarHtml;

  const pnlClass = s.equity_usd >= s.day_start_equity_usd ? 'pos-val' : 'neg-val';
  document.getElementById('statCards').innerHTML = `
    <div class="card"><div class="label">Equity</div><div class="value ${pnlClass}">$${fmt(s.equity_usd)}</div>
      <div class="sub">старт дня $${fmt(s.day_start_equity_usd)}</div></div>
    <div class="card"><div class="label">Суммарный PnL</div><div class="value ${s.stats.total_pnl>=0?'pos-val':'neg-val'}">$${fmt(s.stats.total_pnl)}</div>
      <div class="sub">${s.stats.total_trades} сделок</div></div>
    <div class="card"><div class="label">Win-rate</div><div class="value">${fmt(s.stats.win_rate,1)}%</div>
      <div class="sub">avg win $${fmt(s.stats.avg_win)} / avg loss $${fmt(s.stats.avg_loss)}</div></div>
    <div class="card"><div class="label">Убытков подряд</div><div class="value">${s.consecutive_losses}</div>
      <div class="sub">${s.kill_switch_active ? 'пауза активна' : 'ок'}</div></div>
  `;

  renderChart(s.equity_curve);

  const pb = document.getElementById('positionsBox');
  pb.innerHTML = s.positions.length === 0 ? '<div class="empty">нет открытых позиций</div>' :
    `<table><tr><th>Символ</th><th>Сторона</th><th>Размер</th><th>Вход</th><th>SL (бэкстоп)</th><th>REDUCE</th><th>Разворот EDGE</th><th>Открыта</th></tr>` +
    s.positions.map(p => `<tr>
      <td>${p.symbol}</td>
      <td><span class="badge ${p.side}">${p.side.toUpperCase()}</span></td>
      <td>${fmt(p.size,6)}</td><td>${fmt(p.avg_entry,4)}</td><td>${fmt(p.sl,4)}</td>
      <td>${p.reduced ? '✅' : '—'}</td><td>${p.edge_reverse_streak > 0 ? p.edge_reverse_streak : '—'}</td>
      <td>${p.opened_at ? timeAgo(Math.max(0, s.server_ts - p.opened_at)) + ' назад' : '—'}</td>
    </tr>`).join('') + `</table>`;

  const tb = document.getElementById('tradesBox');
  tb.innerHTML = s.recent_trades.length === 0 ? '<div class="empty">сделок пока нет</div>' :
    `<table><tr><th>Время</th><th>Символ</th><th>Сторона</th><th>Вход</th><th>Выход</th><th>PnL</th><th>Причина</th></tr>` +
    s.recent_trades.map(t => `<tr>
      <td>${tsToShortTime(t.closed_at || t.opened_at)}</td><td>${t.symbol}</td>
      <td><span class="badge ${t.side}">${t.side.toUpperCase()}</span></td>
      <td>${fmt(t.entry_price,4)}</td><td>${t.exit_price ? fmt(t.exit_price,4) : '—'}</td>
      <td class="${(t.pnl_usd||0)>=0?'pos-val':'neg-val'}">${t.pnl_usd!=null ? '$'+fmt(t.pnl_usd) : 'открыта'}</td>
      <td>${t.close_reason || '—'}</td>
    </tr>`).join('') + `</table>`;

  const eb = document.getElementById('eventsBox');
  eb.innerHTML = s.events.map(e =>
    `<div class="event-line event-${e.level}"><span class="t">${tsToShortTime(e.ts)}</span>[${e.level}] ${e.message}</div>`
  ).join('') || '<div class="empty">пока пусто</div>';
}

refresh();
setInterval(refresh, 3000);
setInterval(() => {
  if (Date.now()/1000 - lastStatusAt > 12) {
    document.getElementById('liveDot').className = 'dot stale';
  }
}, 2000);
</script>
</body>
</html>"""
