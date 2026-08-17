"""
Веб-дашборд статуса бота — тот самый "сайт", через который следишь за ботом,
не заходя на ПК. Одна страница: режим, депозит/PnL, открытые позиции,
история сделок, лог событий, и кнопка аварийной остановки (kill switch).

Отдаёт также /api/status (JSON) для программного доступа и
/api/kill, /api/resume для управления kill switch.
"""
import logging
import time
from collections import deque

from aiohttp import web

log = logging.getLogger("dashboard")

MAX_EVENTS = 200


class EventLog:
    def __init__(self):
        self.events = deque(maxlen=MAX_EVENTS)

    def add(self, level: str, message: str):
        self.events.append({"ts": time.time(), "level": level, "message": message})


class DashboardLogHandler(logging.Handler):
    """Перехватывает все логи бота в кольцевой буфер для показа на дашборде."""

    def __init__(self, event_log: EventLog):
        super().__init__()
        self.event_log = event_log

    def emit(self, record):
        try:
            self.event_log.add(record.levelname, self.format(record))
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

    def _status_dict(self):
        positions = []
        for sym, pos in self.orders.positions.items():
            positions.append({
                "symbol": sym, "side": pos.side, "size": round(pos.filled_size, 6),
                "avg_entry": round(pos.avg_entry, 4), "sl": round(pos.current_sl_price, 4),
                "tp1_done": pos.tp1_done, "trailing": pos.trailing_active,
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
            "equity_curve": self.trade_log.equity_curve(self.risk.day_start_equity),
            "events": list(self.events.events)[-60:][::-1],
        }

    async def handle_status(self, request):
        return web.json_response(self._status_dict())

    async def handle_kill(self, request):
        await self.kill_switch.trigger("manual (dashboard)")
        return web.json_response({"ok": True})

    async def handle_resume(self, request):
        self.kill_switch.reset()
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
<title>Lighter Bot — статус</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin:0; padding:24px; background:#0b0e14; color:#e6e8ee;
         font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; }
  h1 { font-size:20px; margin:0 0 4px; }
  .sub { color:#8b93a7; font-size:13px; margin-bottom:20px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(190px,1fr)); gap:12px; margin-bottom:20px; }
  .card { background:#141824; border:1px solid #232838; border-radius:10px; padding:14px 16px; }
  .card .label { color:#8b93a7; font-size:12px; text-transform:uppercase; letter-spacing:.04em; }
  .card .value { font-size:22px; font-weight:600; margin-top:4px; }
  .pos-val { color:#3ddc84; } .neg-val { color:#ff5c72; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { text-align:left; padding:6px 10px; border-bottom:1px solid #1d2230; white-space:nowrap; }
  th { color:#8b93a7; font-weight:500; }
  section { background:#141824; border:1px solid #232838; border-radius:10px; padding:16px; margin-bottom:16px; }
  section h2 { font-size:14px; margin:0 0 12px; color:#c7cbdb; }
  .badge { display:inline-block; padding:2px 8px; border-radius:20px; font-size:12px; font-weight:600; }
  .badge.long { background:#123d2a; color:#3ddc84; }
  .badge.short { background:#3d1420; color:#ff5c72; }
  .kill-banner { background:#3d1420; border:1px solid #ff5c72; color:#ff9aa8; padding:10px 14px;
                 border-radius:8px; margin-bottom:16px; font-size:13px; }
  button { background:#ff5c72; color:#fff; border:none; padding:8px 16px; border-radius:6px;
           font-weight:600; cursor:pointer; font-size:13px; }
  button.resume { background:#3ddc84; color:#08130d; }
  button:hover { opacity:.85; }
  .event-line { font-family:ui-monospace,Menlo,monospace; font-size:12px; padding:3px 0; color:#aab0c2; }
  .event-WARNING { color:#f5c66e; } .event-ERROR { color:#ff5c72; }
  #eventsBox { max-height:260px; overflow-y:auto; }
  .row-actions { display:flex; gap:8px; align-items:center; }
  .empty { color:#8b93a7; font-size:13px; padding:10px 0; }
</style>
</head>
<body>
  <h1>Lighter DOM-Bot</h1>
  <div class="sub" id="subheader">загрузка...</div>

  <div id="killBanner"></div>

  <div class="grid" id="statCards"></div>

  <section>
    <h2>Открытые позиции</h2>
    <div id="positionsBox"><div class="empty">нет открытых позиций</div></div>
  </section>

  <section>
    <h2>Последние сделки</h2>
    <div id="tradesBox"><div class="empty">сделок пока нет</div></div>
  </section>

  <section>
    <h2>Лог событий</h2>
    <div id="eventsBox"></div>
  </section>

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

async function killSwitch() {
  if (!confirm('Закрыть все позиции и остановить бота?')) return;
  await fetch('/api/kill', {method:'POST'});
  refresh();
}
async function resumeBot() {
  await fetch('/api/resume', {method:'POST'});
  refresh();
}

async function refresh() {
  let s;
  try {
    s = await (await fetch('/api/status')).json();
  } catch (e) { return; }

  document.getElementById('subheader').textContent =
    `режим: ${s.mode} | символы: ${s.symbols.join(', ')} | аптайм: ${timeAgo(s.uptime_sec)}`;

  const killBanner = document.getElementById('killBanner');
  if (s.kill_switch_active) {
    killBanner.innerHTML = `<div class="kill-banner">⛔ KILL SWITCH АКТИВЕН (${s.kill_switch_reason}) — новые входы заблокированы.
      <button class="resume" onclick="resumeBot()" style="margin-left:10px;">Возобновить торговлю</button></div>`;
  } else {
    killBanner.innerHTML = `<div class="row-actions" style="margin-bottom:16px;">
      <button onclick="killSwitch()">⛔ Аварийная остановка</button></div>`;
  }

  const pnlClass = s.equity_usd >= s.day_start_equity_usd ? 'pos-val' : 'neg-val';
  document.getElementById('statCards').innerHTML = `
    <div class="card"><div class="label">Equity</div><div class="value ${pnlClass}">$${fmt(s.equity_usd)}</div></div>
    <div class="card"><div class="label">Депозит на начало дня</div><div class="value">$${fmt(s.day_start_equity_usd)}</div></div>
    <div class="card"><div class="label">Всего сделок</div><div class="value">${s.stats.total_trades}</div></div>
    <div class="card"><div class="label">Win-rate</div><div class="value">${fmt(s.stats.win_rate,1)}%</div></div>
    <div class="card"><div class="label">Суммарный PnL</div><div class="value ${s.stats.total_pnl>=0?'pos-val':'neg-val'}">$${fmt(s.stats.total_pnl)}</div></div>
    <div class="card"><div class="label">Убытков подряд</div><div class="value">${s.consecutive_losses}</div></div>
  `;

  const pb = document.getElementById('positionsBox');
  pb.innerHTML = s.positions.length === 0 ? '<div class="empty">нет открытых позиций</div>' :
    `<table><tr><th>Символ</th><th>Сторона</th><th>Размер</th><th>Вход</th><th>Стоп</th><th>TP1</th><th>Трейлинг</th></tr>` +
    s.positions.map(p => `<tr>
      <td>${p.symbol}</td>
      <td><span class="badge ${p.side}">${p.side.toUpperCase()}</span></td>
      <td>${fmt(p.size,6)}</td><td>${fmt(p.avg_entry,4)}</td><td>${fmt(p.sl,4)}</td>
      <td>${p.tp1_done ? '✅' : '—'}</td><td>${p.trailing ? '🔄' : '—'}</td>
    </tr>`).join('') + `</table>`;

  const tb = document.getElementById('tradesBox');
  tb.innerHTML = s.recent_trades.length === 0 ? '<div class="empty">сделок пока нет</div>' :
    `<table><tr><th>Время</th><th>Символ</th><th>Сторона</th><th>Вход</th><th>Выход</th><th>PnL</th><th>Причина</th></tr>` +
    s.recent_trades.map(t => `<tr>
      <td>${tsToTime(t.closed_at || t.opened_at)}</td><td>${t.symbol}</td>
      <td><span class="badge ${t.side}">${t.side.toUpperCase()}</span></td>
      <td>${fmt(t.entry_price,4)}</td><td>${t.exit_price ? fmt(t.exit_price,4) : '—'}</td>
      <td class="${(t.pnl_usd||0)>=0?'pos-val':'neg-val'}">${t.pnl_usd!=null ? '$'+fmt(t.pnl_usd) : 'открыта'}</td>
      <td>${t.close_reason || '—'}</td>
    </tr>`).join('') + `</table>`;

  const eb = document.getElementById('eventsBox');
  eb.innerHTML = s.events.map(e =>
    `<div class="event-line event-${e.level}">${tsToTime(e.ts)} [${e.level}] ${e.message}</div>`
  ).join('') || '<div class="empty">пока пусто</div>';
}

refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>"""
