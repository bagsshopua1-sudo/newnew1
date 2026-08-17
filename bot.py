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
from typing import Dict

from binance_feed import BinanceFeed
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

    binance = BinanceFeed(markets, on_prolonged_outage=kill_switch.trigger) if CFG.use_binance_signals else None
    latest_lighter_snap: Dict[str, "BookSnapshot"] = {}

    dashboard = Dashboard(risk, orders, trade_log, kill_switch, CFG.dashboard_port, CFG.mode, list(markets.keys()))
    await dashboard.start()

    await md.start()
    if binance:
        await binance.start()
    log.info("Бот запущен в режиме MODE=%s | символы: %s | депозит=%.2f USD | риск/сделка=%.1f%% | "
              "источник сигнала=%s | дашборд на порту %d",
              CFG.mode, ", ".join(markets.keys()), CFG.account_equity_usd, CFG.risk_per_trade_pct,
              "Binance" if binance else "Lighter", CFG.dashboard_port)

    async def lighter_consumer():
        """Стакан Lighter: только для цены исполнения (touch price) и basis-проверки.
        Если Binance-сигналы выключены - это же и единственный источник сигналов."""
        while True:
            snap = await md.events.get()
            orders.note_snapshot(snap)
            latest_lighter_snap[snap.symbol] = snap

            if binance:
                continue  # сигналы строим по Binance, тут только исполнение/basis

            await _maybe_signal(snap, snap)

    async def binance_consumer():
        while True:
            snap = await binance.events.get()
            lighter_snap = latest_lighter_snap.get(snap.symbol)
            if lighter_snap is None:
                continue  # ещё нет цены Lighter для исполнения - подождём
            await _maybe_signal(snap, lighter_snap)

    async def _maybe_signal(signal_snap, lighter_snap):
        """signal_snap - откуда берём сигнал (Binance или Lighter), lighter_snap -
        актуальная цена Lighter, по которой реально будем входить/считать SL/TP."""
        if kill_switch.active:
            return  # аварийная остановка: сигналы не обрабатываем, дашборд продолжает работать

        engine = engines[signal_snap.symbol]
        signal = engine.on_snapshot(signal_snap)
        if signal is None:
            return

        log.info("[%s] СИГНАЛ %s (%s) confidence=%.2f у цены %.2f (источник: %s)",
                  signal.symbol, signal.side.upper(), signal.signal_type, signal.confidence,
                  signal.reference_price, "Binance" if binance else "Lighter")

        if signal.confidence < 0.5:
            return  # слабый сигнал без подтверждения имбалансом/трендом - пропускаем

        if binance:
            # basis-проверка: сигнал построен по Binance, но торгуем на Lighter -
            # если цены разошлись сильнее порога, вход по этому сигналу не оправдан.
            basis_pct = abs(lighter_snap.mid - signal_snap.mid) / lighter_snap.mid * 100
            if basis_pct > CFG.basis_max_divergence_pct:
                log.warning("[%s] сигнал пропущен: базис Lighter/Binance %.3f%% > порога %.3f%%",
                            signal.symbol, basis_pct, CFG.basis_max_divergence_pct)
                return
            # цену/стоп/тейк считаем от РЕАЛЬНОЙ цены исполнения (Lighter), не от Binance
            signal.mid = lighter_snap.mid

        market = markets[signal.symbol]
        asyncio.create_task(orders.handle_signal(market, signal))

    try:
        tasks = [asyncio.create_task(lighter_consumer())]
        if binance:
            tasks.append(asyncio.create_task(binance_consumer()))
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        await orders.shutdown()
        await md.stop()
        if binance:
            await binance.stop()
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
