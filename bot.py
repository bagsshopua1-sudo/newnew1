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
import signal
import time
from typing import Dict

from binance_feed import BinanceFeed
from binance_trades import BinanceTradeFeed
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
from wall_event_log import WallEventLog

log = logging.getLogger("bot")


async def run_trading():
    exchange = ExchangeClient()
    markets = await exchange.resolve_markets()

    trade_log = TradeLog()
    # Персистентное хранилище WALL_CANDIDATE/WALL_OUTCOME/shadow_evals (аудит
    # стратегии 18.08, этап 1.3) - см. wall_event_log.py.
    wall_events = WallEventLog()
    kill_switch = KillSwitch()
    risk = RiskManager(on_breach=kill_switch.trigger)

    md = MarketData(exchange, markets, on_prolonged_outage=kill_switch.trigger)

    binance = BinanceFeed(markets, on_prolonged_outage=kill_switch.trigger) if CFG.use_binance_signals else None
    # Поток реальных исполненных сделок (aggTrade) - отдельно от снепшотов
    # стакана, нужен для "сколько реально прошло объёма у этой стенки", а не
    # только "какой displayed size сейчас видно" (см. binance_trades.py).
    # Теперь передаётся и в OrderManager (см. ниже) - opposite-flow exit
    # (19.08, финальный этап рестройки) смотрит на тот же реальный тейп уже
    # ПОСЛЕ входа, чтобы поймать разворот потока против позиции раньше, чем
    # он успеет сломать формальную структуру сделки.
    binance_trades = BinanceTradeFeed(markets, on_prolonged_outage=kill_switch.trigger) \
        if CFG.use_binance_signals else None

    orders = OrderManager(exchange, md, risk, trade_log=trade_log, kill_switch=kill_switch,
                           trade_feed=binance_trades)
    kill_switch.bind(orders)

    trend_filters = {sym: TrendFilter(CFG.trend_ema_fast_sec, CFG.trend_ema_slow_sec,
                                       CFG.vol_lookback, CFG.vol_spike_mult,
                                       CFG.dead_range_lookback_sec, CFG.dead_range_min_pct,
                                       CFG.dead_range_min_coverage_sec) for sym in markets}
    engines = {sym: SignalEngine(sym, trend_filter=trend_filters[sym], trade_feed=binance_trades,
                                  event_log=wall_events)
               for sym in markets}

    latest_lighter_snap: Dict[str, "BookSnapshot"] = {}
    # См. CFG.startup_grace_sec - первые N секунд после запуска процесса сигналы
    # не обрабатываем вообще (ни на вход, ни на разворот) - стакан/wall-tracking
    # ещё не устаканились после свежего WS-коннекта, особенно после free-tier
    # рестарта Render (см. комментарий у startup_grace_sec в config.py).
    process_start_ts = time.time()
    _startup_grace_logged = False

    dashboard = Dashboard(risk, orders, trade_log, kill_switch, CFG.dashboard_port, CFG.mode, list(markets.keys()))
    await dashboard.start()

    await md.start()
    if binance:
        await binance.start()
    if binance_trades:
        await binance_trades.start()
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
            # Сохраняем последний снепшот стакана Binance - это структура (стенки/
            # дисбаланс), по которой order_manager._thesis_invalidated проверяет,
            # жив ли ещё тезис сделки, пока позиция открыта (см. note_signal_snapshot).
            orders.note_signal_snapshot(snap)
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

        # См. CFG.startup_grace_sec - первые секунды после запуска процесса
        # сигналы НЕ отбрасываем молча в on_snapshot (там wall-tracking должен
        # продолжать копиться и прогреваться нормально), а глушим именно тут,
        # на выходе - чтобы не открывать сделки на ещё не устаканившемся стакане
        # сразу после (пере)запуска процесса. on_snapshot() выше всё равно
        # выполняется каждый тик ради прогрева self.tracked - иначе к моменту
        # окончания грейс-периода стенки пришлось бы отслеживать заново с нуля.
        nonlocal _startup_grace_logged
        since_start = time.time() - process_start_ts
        if since_start < CFG.startup_grace_sec:
            if not _startup_grace_logged:
                log.info("[%s] сигнал %s пропущен: стартовый грейс-период (%.0fs из %.0fs после запуска "
                          "процесса) - стакан ещё не устаканился", signal.symbol, signal.side.upper(),
                          since_start, CFG.startup_grace_sec)
                _startup_grace_logged = True
            return

        log.info("[%s] СИГНАЛ %s (%s) confidence=%.2f у цены %.2f (источник: %s)",
                  signal.symbol, signal.side.upper(), signal.signal_type, signal.confidence,
                  signal.reference_price, "Binance" if binance else "Lighter")

        if signal.confidence < 0.5:
            return  # слабый сигнал без подтверждения имбалансом/трендом - пропускаем

        # Латентность: сколько времени прошло между событием стакана, которое
        # породило сигнал, и тем, что бот его обработал - важно на free tier
        # (Frankfurt), где сеть/CPU не гарантированы. Если сигналу уже условно
        # 300+ мс, к моменту реального входа он может успеть "протухнуть".
        signal_age_ms = (time.time() - signal_snap.ts) * 1000
        basis_pct = abs(lighter_snap.mid - signal_snap.mid) / lighter_snap.mid * 100 if binance else 0.0
        log.info("[%s] latency signal_age_ms=%.0f basis_pct=%.4f", signal.symbol, signal_age_ms, basis_pct)

        # НОВОЕ 19.08 (финальный этап рестройки стратегии) - прямой пункт из
        # запроса пользователя: "если сигнал устарел к моменту исполнения на
        # Lighter - отменить вход". Раньше signal_age_ms только логировался и
        # ни на что не влиял (см. CFG.max_signal_age_ms в config.py за полное
        # обоснование порога) - структура (стенка/уровень), на которой строился
        # сигнал, могла успеть измениться за время задержки между снепшотом
        # Binance и обработкой ботом, особенно на free tier (Frankfurt, не
        # гарантированы CPU/сеть).
        if signal_age_ms > CFG.max_signal_age_ms:
            log.info("[%s] сигнал %s (%s) ОТМЕНЁН: устарел к моменту исполнения "
                      "(signal_age_ms=%.0f > %.0f)", signal.symbol, signal.side.upper(),
                      signal.signal_type, signal_age_ms, CFG.max_signal_age_ms)
            return

        if binance:
            # Порог на разрыв цен Lighter/Binance (BASIS_MAX_DIVERGENCE_PCT) убран
            # по просьбе пользователя 19.08 - раньше он резал сигналы просто за то,
            # что биржи разошлись в моменте, хотя расчёт стопа теперь сам поправлен
            # на этот базис (см. Signal.exchange_basis / risk.build_plan) и уже не
            # тянет туда межбиржевой шум как ложную "структуру". basis_pct всё ещё
            # считается и логируется выше - просто больше не блокирует вход.
            # цену/стоп/тейк считаем от РЕАЛЬНОЙ цены исполнения (Lighter), не от Binance
            signal.mid = lighter_snap.mid
            # См. Signal.exchange_basis в signals.py - risk.build_plan скорректирует
            # reference_price (стенка, Binance) на этот базис перед расчётом
            # дистанции стопа, чтобы межбиржевой разброс цен не попадал в стоп
            # как будто это расстояние до реального уровня в стакане.
            signal.exchange_basis = lighter_snap.mid - signal_snap.mid

        market = markets[signal.symbol]
        asyncio.create_task(orders.handle_signal(market, signal))

    # Грейсфул шатдаун: хостинг (Render и любой другой контейнерный PaaS) перед
    # рестартом/редеплоем шлёт SIGTERM с коротким грейс-периодом, а не сразу
    # SIGKILL. Без обработчика Python на SIGTERM завершается МГНОВЕННО, минуя
    # весь finally ниже - открытая на этот момент позиция просто пропадает:
    # в paper-режиме lighter.PaperClient целиком живёт в памяти процесса (см.
    # exchange_client.py - создаётся заново на каждом старте с тем же
    # initial_collateral_usdc), новый процесс стартует с чистого листа, а в
    # trade_log остаётся запись сделки, у которой никогда не проставится exit.
    # На Render free tier это особенно важно - процесс перезапускается сам по
    # себе (без нового деплоя) в среднем каждые несколько минут. Ловим сигнал
    # и закрываем позиции штатно (flatten_all), чтобы PnL хотя бы попал в
    # журнал, прежде чем процесс всё равно умрёт при следующем рестарте.
    shutdown_event = asyncio.Event()

    def _on_shutdown_signal(sig_name: str):
        if not shutdown_event.is_set():
            log.warning("Получен сигнал %s - грейсфул шатдаун (закрываю открытые позиции перед выходом)",
                        sig_name)
            shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_shutdown_signal, sig.name)
        except (NotImplementedError, RuntimeError):
            pass  # платформы без поддержки add_signal_handler - не актуально на Render/Linux

    try:
        tasks = [asyncio.create_task(lighter_consumer())]
        if binance:
            tasks.append(asyncio.create_task(binance_consumer()))
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        done, pending = await asyncio.wait([*tasks, shutdown_task], return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        # если завершилось не из-за сигнала (например consumer упал с исключением) -
        # даём исключению всплыть наружу, как раньше делал asyncio.gather(*tasks)
        for t in done:
            if t is not shutdown_task:
                t.result()
    except asyncio.CancelledError:
        pass
    finally:
        if orders.positions:
            log.warning("Грейсфул шатдаун: закрываю %d открытых позиций перед выходом", len(orders.positions))
            try:
                await orders.flatten_all("graceful_shutdown")
            except Exception as e:
                log.error("Не удалось закрыть позиции при грейсфул шатдауне: %s", e)
        await orders.shutdown()
        await md.stop()
        if binance:
            await binance.stop()
        if binance_trades:
            await binance_trades.stop()
        await exchange.close()
        trade_log.close()
        wall_events.close()


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if CFG.mode == "collect":
        from collector import run_collector
        await run_collector()
    else:
        await run_trading()


if __name__ == "__main__":
    asyncio.run(main())
