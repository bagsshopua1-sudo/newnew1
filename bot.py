"""
Точка входа бота.

MODE=collect -> запускает только сборщик стакана (см. collector.py), торговли нет.
MODE=paper   -> полный цикл (сигналы + вход лимитками с chase + SL/TP + трейлинг +
                трендовый фильтр + kill switch + журнал сделок + веб-дашборд),
                но ордера виртуальные, через lighter.PaperClient на реальном стакане.
MODE=live    -> то же самое, но реальными деньгами через lighter.SignerClient.

Запуск:
    python bot.py
Настройки — в .env (см. .env.example). В paper/live режиме дашборд статуса
доступен на http://<host>:<DASHBOARD_PORT> (или PORT, если задан платформой).
"""
import asyncio
import logging

from config import CFG
from dashboard import Dashboard
from exchange_client import ExchangeClient
from kill_switch import KillSwitch
from market_data import MarketData
from order_manager import OrderManager
from risk import RiskManager
from signals import SignalEngine
from trade_log import TradeLog
from trend_filter import TrendFilter

log = logging.getLogger("bot")


async def run_trading():
    exchange = ExchangeClient()
    markets = await exchange.resolve_markets()

    trade_log = TradeLog()
    kill_switch = KillSwitch()
    risk = RiskManager(on_breach=kill_switch.trigger)

    md = MarketData(exchange, markets, on_prolonged_outage=kill_switch.trigger)
    orders = OrderManager(exchange, md, risk, trade_log=trade_log, kill_switch=kill_switch)
    kill_switch.bind(orders)

    trend_filters = {sym: TrendFilter(CFG.trend_ema_fast_sec, CFG.trend_ema_slow_sec,
                                       CFG.vol_lookback, CFG.vol_spike_mult) for sym in markets}
    engines = {sym: SignalEngine(sym, trend_filter=trend_filters[sym]) for sym in markets}

    dashboard = Dashboard(risk, orders, trade_log, kill_switch, CFG.dashboard_port, CFG.mode, list(markets.keys()))
    await dashboard.start()

    await md.start()
    log.info("Бот запущен в режиме MODE=%s | символы: %s | депозит=%.2f USD | риск/сделка=%.1f%% | дашборд на порту %d",
              CFG.mode, ", ".join(markets.keys()), CFG.account_equity_usd, CFG.risk_per_trade_pct, CFG.dashboard_port)

    try:
        while True:
            snap = await md.events.get()
            orders.note_snapshot(snap)

            if kill_switch.active:
                continue  # аварийная остановка: сигналы не обрабатываем, но дашборд продолжает работать

            engine = engines[snap.symbol]
            signal = engine.on_snapshot(snap)
            if signal is None:
                continue

            log.info("[%s] СИГНАЛ %s (%s) confidence=%.2f у цены %.2f",
                      signal.symbol, signal.side.upper(), signal.signal_type, signal.confidence, signal.reference_price)

            if signal.confidence < 0.5:
                continue  # слабый сигнал без подтверждения имбалансом/трендом - пропускаем

            market = markets[signal.symbol]
            asyncio.create_task(orders.handle_signal(market, signal))
    except asyncio.CancelledError:
        pass
    finally:
        await orders.shutdown()
        await md.stop()
        await exchange.close()
        trade_log.close()


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if CFG.mode == "collect":
        from collector import run_collector
        await run_collector()
    else:
        await run_trading()


if __name__ == "__main__":
    asyncio.run(main())
