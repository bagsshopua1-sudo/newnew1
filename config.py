"""
Загрузка и валидация конфигурации бота из .env / переменных окружения.
"""
import os
from dataclasses import dataclass, field
from typing import List

from dotenv import load_dotenv

load_dotenv()


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v not in (None, "") else default


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v not in (None, "") else default


def _s(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default


@dataclass
class Config:
    mode: str = field(default_factory=lambda: _s("MODE", "collect").lower())
    network: str = field(default_factory=lambda: _s("NETWORK", "mainnet").lower())
    symbols: List[str] = field(default_factory=lambda: [
        s.strip() for s in _s("SYMBOLS", "ETH-USDC,BTC-USDC").split(",") if s.strip()
    ])

    collect_minutes: float = field(default_factory=lambda: _f("COLLECT_MINUTES", 60))

    account_index: int = field(default_factory=lambda: _i("ACCOUNT_INDEX", -1))
    api_private_key: str = field(default_factory=lambda: _s("API_PRIVATE_KEY", ""))
    api_key_index: int = field(default_factory=lambda: _i("API_KEY_INDEX", 0))

    wall_min_usd: float = field(default_factory=lambda: _f("WALL_MIN_USD", 150_000))
    wall_max_distance_pct: float = field(default_factory=lambda: _f("WALL_MAX_DISTANCE_PCT", 0.3))
    imbalance_threshold: float = field(default_factory=lambda: _f("IMBALANCE_THRESHOLD", 0.65))

    account_equity_usd: float = field(default_factory=lambda: _f("ACCOUNT_EQUITY_USD", 1000))
    risk_per_trade_pct: float = field(default_factory=lambda: _f("RISK_PER_TRADE_PCT", 1.0))
    # Запасной вариант для build_plan(), если по какой-то причине нет уровня-опоры
    # (сигнальной стенки) - в норме не используется, стоп считается от структуры.
    stop_loss_pct: float = field(default_factory=lambda: _f("STOP_LOSS_PCT", 0.6))
    take_profit_1_pct: float = field(default_factory=lambda: _f("TAKE_PROFIT_1_PCT", 0.6))
    # === "Умный" стоп - от структуры (стенки/уровня сигнала), а не одинаковый % ===
    # Стоп = |entry - reference_price сигнала| + буфер, зажатый в [MIN_STOP_PCT, MAX_STOP_PCT]
    # от цены входа. Так расстояние до стопа определяется реальным уровнем, который
    # породил сигнал (и текущей волатильностью через сам уровень), а не произвольным
    # одинаковым числом для каждой сделки. Клампы нужны, чтобы не словить ни аномально
    # узкий стоп (шум выбивает), ни аномально широкий (непропорциональный риск).
    min_stop_pct: float = field(default_factory=lambda: _f("MIN_STOP_PCT", 0.15))
    max_stop_pct: float = field(default_factory=lambda: _f("MAX_STOP_PCT", 1.5))
    stop_buffer_pct: float = field(default_factory=lambda: _f("STOP_BUFFER_PCT", 0.05))
    # TP1 = стоп-дистанция * это отношение (risk:reward), а не отдельный фиксированный %.
    rr_target_1: float = field(default_factory=lambda: _f("RR_TARGET_1", 1.5))
    take_profit_1_size: float = field(default_factory=lambda: _f("TAKE_PROFIT_1_SIZE", 0.5))
    trailing_stop_pct: float = field(default_factory=lambda: _f("TRAILING_STOP_PCT", 0.4))
    max_leverage: float = field(default_factory=lambda: _f("MAX_LEVERAGE", 3))
    daily_loss_limit_pct: float = field(default_factory=lambda: _f("DAILY_LOSS_LIMIT_PCT", 3.0))
    max_consecutive_losses: int = field(default_factory=lambda: _i("MAX_CONSECUTIVE_LOSSES", 3))
    cooldown_minutes: float = field(default_factory=lambda: _f("COOLDOWN_MINUTES", 120))

    order_fill_timeout_sec: float = field(default_factory=lambda: _f("ORDER_FILL_TIMEOUT_SEC", 8))
    max_reprice_attempts: int = field(default_factory=lambda: _i("MAX_REPRICE_ATTEMPTS", 3))
    # Как часто (сек) проверять открытую позицию: срабатывание стопа/тейка по цене
    # и "умный" выход по развалу структуры сделки (см. order_manager._thesis_invalidated).
    position_check_interval_sec: float = field(default_factory=lambda: _f("POSITION_CHECK_INTERVAL_SEC", 1.0))
    # Сколько секунд после входа не проверять развал структуры - без этого шум
    # сразу после входа (цена на секунду качнулась к стенке) может закрыть
    # сделку мгновенно, прежде чем тезис вообще успел подтвердиться или нет.
    thesis_grace_period_sec: float = field(default_factory=lambda: _f("THESIS_GRACE_PERIOD_SEC", 5.0))
    # Сколько проверок подряд (с интервалом POSITION_CHECK_INTERVAL_SEC) условие
    # развала тезиса должно оставаться истинным, прежде чем реально закрыть
    # сделку. Раньше закрывали по первому же срабатыванию - в проде это привело
    # к тому, что почти все сделки закрывались за 8-40 секунд от шума в стакане,
    # ни разу не дойдя ни до TP1, ни до стопа (винрейт ушёл к ~35-40% при
    # одинаковом размере плюсовых/минусовых сделок вместо задуманного risk:reward).
    invalidation_confirm_ticks: int = field(default_factory=lambda: _i("INVALIDATION_CONFIRM_TICKS", 2))
    # Встречная стенка (_opposing_wall_exit) фиксирует прибыль немедленно, БЕЗ
    # confirm-tick debounce (см. order_manager._watch_position) - раз уж
    # сработало, ждать нельзя, иначе цена успевает откатить обратно за вход
    # (именно так и было: оба первых opposing_wall закрытия в проде дали
    # убыток, а не прибыль). Но раньше условие срабатывания было "mid чуть
    # выше entry" - то есть буквально ЛЮБОЙ тик в плюс на 0.001% при наличии
    # крупной стенки на Binance (а она почти всегда есть при глубоком стакане
    # и пороге BINANCE_WALL_MIN_USD) - после round-trip проскальзывания на
    # входе/выходе (2x PAPER_CROSS_BUFFER_PCT) это гарантированно убыток. Этот
    # порог требует, чтобы позиция была в плюсе минимум на столько % от цены
    # входа, прежде чем встречная стенка вообще рассматривается как повод
    # зафиксировать прибыль.
    opposing_wall_min_profit_pct: float = field(default_factory=lambda: _f("OPPOSING_WALL_MIN_PROFIT_PCT", 0.15))
    # Порог фиксации прибыли по встречной стенке не должен быть одним и тем же
    # числом при спокойном рынке и при резком движении. Реальный требуемый порог =
    # max(OPPOSING_WALL_MIN_PROFIT_PCT, волатильность_на_входе * этот множитель).
    opposing_wall_vol_multiplier: float = field(default_factory=lambda: _f("OPPOSING_WALL_VOL_MULTIPLIER", 1.0))
    # Если встречная стенка стоит ПРЯМО у цены (в пределах этого % от mid), а не
    # где-то на 0.2-0.3% дальше - это более срочный повод отреагировать: цена
    # физически упёрлась в неё прямо сейчас, а не когда-то может дойти. Для
    # такой "стенки в упор" используем пониженный порог прибыли (см. ниже) вместо
    # обычного OPPOSING_WALL_MIN_PROFIT_PCT.
    opposing_wall_close_distance_pct: float = field(
        default_factory=lambda: _f("OPPOSING_WALL_CLOSE_DISTANCE_PCT", 0.08))
    # Пониженный порог прибыли для стенки "в упор" (см. opposing_wall_close_distance_pct).
    # ВАЖНО: не может быть ниже round-trip издержек на проскальзывание
    # (2 x PAPER_CROSS_BUFFER_PCT = 0.10% при дефолте) - иначе закрытие снова
    # гарантированно уходит в минус после проскальзывания на входе И выходе,
    # ровно та регрессия, из-за которой изначально появился OPPOSING_WALL_MIN_PROFIT_PCT
    # (в проде первые два opposing_wall закрытия дали -0.47 и -0.35 именно по этой причине).
    # 0.12% - на 0.02% выше break-even издержек, т.е. закрытие фиксирует
    # небольшую, но реальную прибыль, а не просто отдаёт её проскальзыванию.
    opposing_wall_close_min_profit_pct: float = field(
        default_factory=lambda: _f("OPPOSING_WALL_CLOSE_MIN_PROFIT_PCT", 0.12))

    # === Сравнение силы стенок (НЕ завязано на % прибыли) ===
    # Жалоба пользователя: "если лонг идёт хорошо вверх и упирается в стенку
    # 1кк, а сейчас держит цену стенка всего 200к - надо выходить, так как 1кк
    # сильнее 200к". Т.е. решение не о том, сколько % прибыли уже набежало, а
    # о том, что стакан ПРЯМО СЕЙЧАС говорит: сторона, которая держит текущее
    # движение, слабее стороны, которая его блокирует - значит держать дальше
    # бессмысленно, пробьёт. См. _thesis_invalidated в order_manager.py - там
    # ближайшая "блокирующая" стенка (против движения) сравнивается с ближайшей
    # "держащей" (по ходу движения); если блокирующая сильнее в это число раз -
    # тезис считается развалившимся. Работает через тот же
    # INVALIDATION_CONFIRM_TICKS-дебаунс и THESIS_GRACE_PERIOD_SEC, что и
    # остальные проверки в _thesis_invalidated - защита от секундного шума в
    # стакане, тот же принцип, что и у остальных "умных" выходов.
    wall_dominance_ratio: float = field(default_factory=lambda: _f("WALL_DOMINANCE_RATIO", 1.0))

    # === Проверка "подложки" за стенкой (бот сам отсеивает слабые сигналы) ===
    # Крупная стенка на входе - ещё не гарантия, что она удержит цену: если
    # сразу ЗА ней (глубже в стакане, в ту же сторону) объём резко тает, это
    # одиночная заявка без реальной поддержки за ней, и цена может пройти её
    # насквозь. Пример от пользователя: стенка на 1.5кк, а под ней всего 1кк -
    # такое считается слабой структурой и сигнал ABSORPTION на такой стенке
    # пропускается (см. signals.py).
    # Диапазон (% от цены стенки), в котором считается "подложка" за ней.
    # ВАЖНО: у Binance partial-depth стрима всего 20 уровней снепшота
    # (BINANCE_WS_DEPTH_LEVELS, максимум для этого типа потока) - на диапазоне
    # 0.3% почти всегда упирались в конец снепшота, подложка получалась
    # искусственно маленькой (наблюдалось в проде: реальная подложка $60-400К
    # при стенке $1.5-3М, то есть 3-15%, а не заявленные изначально 70%) и
    # фильтр резал 100% сигналов. 1.0% даёт больше видимых уровней позади стенки.
    wall_backup_range_pct: float = field(default_factory=lambda: _f("WALL_BACKUP_RANGE_PCT", 1.0))
    # Подложка должна быть не меньше этой доли от объёма самой стенки.
    # Откалибровано по реальным данным прода (см. коммент выше) - 0.7 отсекал
    # вообще все сигналы, 0.15 фильтрует только совсем пустые "стенки-одиночки".
    wall_backup_min_ratio: float = field(default_factory=lambda: _f("WALL_BACKUP_MIN_RATIO", 0.15))
    # lighter.PaperClient не умеет держать "висящую" лимитку в очереди - IOC либо
    # исполняется сразу против противоположной стороны стакана, либо отменяется
    # целиком. Чтобы paper-режим вообще мог исполнять сделки, вход ставится с
    # небольшим пересечением спреда (в % от цены) - это НЕ используется в live,
    # там ордер честно висит в стакане post-only.
    paper_cross_buffer_pct: float = field(default_factory=lambda: _f("PAPER_CROSS_BUFFER_PCT", 0.05))

    # Веб-дашборд. Render и другие облачные платформы сами прокидывают порт через PORT -
    # если он задан, он в приоритете над DASHBOARD_PORT.
    dashboard_port: int = field(default_factory=lambda: _i("PORT", _i("DASHBOARD_PORT", 8080)))

    # Трендовый/волатильностный фильтр (см. trend_filter.py)
    trend_ema_fast_sec: float = field(default_factory=lambda: _f("TREND_EMA_FAST_SEC", 15))
    trend_ema_slow_sec: float = field(default_factory=lambda: _f("TREND_EMA_SLOW_SEC", 90))
    vol_lookback: int = field(default_factory=lambda: _i("VOL_LOOKBACK", 60))
    vol_spike_mult: float = field(default_factory=lambda: _f("VOL_SPIKE_MULT", 3.0))

    # === Источник сигнала - стакан Binance Futures (публичный REST, без ключей) ===
    # У Lighter стакан тоньше, чем у Binance - крупные заявки на Binance надёжнее
    # отражают реальный интерес. Исполнение всё равно на Lighter (0 комиссий).
    use_binance_signals: bool = field(default_factory=lambda: _s("USE_BINANCE_SIGNALS", "true").lower() == "true")
    # WebSocket partial book depth: сколько уровней (5/10/20) и с какой скоростью
    # обновления (100/250/500 мс). REST-поллинг не используется - Binance банит IP
    # за превышение веса запросов (см. binance_feed.py), WS для market-data так не тарифицируется.
    binance_ws_depth_levels: int = field(default_factory=lambda: _i("BINANCE_WS_DEPTH_LEVELS", 20))
    binance_ws_speed_ms: int = field(default_factory=lambda: _i("BINANCE_WS_SPEED_MS", 500))
    # Стакан Binance на порядки глубже Lighter - порог WALL_MIN_USD, откалиброванный
    # под тонкий Lighter, на Binance будет ловить мусорные "стенки" почти на каждом
    # тике. Нужен отдельный, заметно более высокий порог - точное значение требует
    # эмпирической калибровки (как и было задумано MODE=collect), это стартовое
    # приближение, не проверенное вживую.
    binance_wall_min_usd: float = field(default_factory=lambda: _f("BINANCE_WALL_MIN_USD", 2_000_000))
    # Если цена Lighter разошлась с Binance больше чем на столько % - сигнал с
    # Binance пропускаем: базис слишком большой, вход по нему не оправдан.
    basis_max_divergence_pct: float = field(default_factory=lambda: _f("BASIS_MAX_DIVERGENCE_PCT", 0.15))

    # === Задержка подтверждения сигнала (было жёстко зашито в signals.py) ===
    # Стенка должна простоять минимум столько секунд, прежде чем считаться
    # "реальной" (и для ABSORPTION, и для BREAKOUT) - фильтр против спуфинга
    # (мгновенно снятых заявок). Раньше было 3.0 без возможности настройки.
    # Жалоба пользователя: "заходит через 5 секунд" - к моменту, когда сигнал
    # наконец подтверждался (age>=3s И 3 стакана подряд "топчется"), цена уже
    # часто успевала пройти мимо выгодной точки входа. 2.0 - разумный компромисс:
    # всё ещё фильтрует однотиковый шум (обновления Binance WS раз в 0.5с, то
    # есть 2с - это минимум 4 снепшота), но не тянет с подтверждением так долго.
    min_wall_age_sec: float = field(default_factory=lambda: _f("MIN_WALL_AGE_SEC", 2.0))
    # Сколько снепшотов подряд цена должна "топтаться" у стенки для ABSORPTION.
    # Было жёстко зашито 3 - при обновлениях раз в 0.5с это ещё +1-1.5с сверху
    # min_wall_age_sec. 2 подряд - всё ещё требует реального "затыка" цены у
    # уровня, а не одного случайного тика, но не удваивает и без того длинную
    # задержку до входа.
    absorption_stall_ticks: int = field(default_factory=lambda: _i("ABSORPTION_STALL_TICKS", 2))
    # Минимальный интервал (сек) между повторными ABSORPTION-сигналами по ОДНОЙ
    # И ТОЙ ЖЕ стенке. Найдено в проде: снепшоты Binance WS иногда приходят
    # "пачкой" (несколько сообщений в пределах миллисекунд, а не штатным
    # интервалом BINANCE_WS_SPEED_MS) - внутри такой пачки mid почти не
    # меняется, поэтому stall_count успевает повторно дойти до
    # ABSORPTION_STALL_TICKS ещё до конца пачки, и один и тот же сигнал
    # выдавался по 10-40 раз за миллисекунды (лог-спам + лишние параллельные
    # asyncio-задачи на один и тот же сигнал). НЕ влияет на скорость первого
    # входа - см. signals.py.
    absorption_min_refire_sec: float = field(default_factory=lambda: _f("ABSORPTION_MIN_REFIRE_SEC", 1.0))

    def validate(self):
        assert self.mode in ("collect", "paper", "live"), f"Неизвестный MODE={self.mode}"
        assert self.network in ("mainnet", "testnet"), f"Неизвестная NETWORK={self.network}"
        if self.mode == "live":
            if self.account_index < 0 or not self.api_private_key:
                raise SystemExit(
                    "MODE=live требует ACCOUNT_INDEX и API_PRIVATE_KEY в .env. "
                    "Сначала протестируй стратегию в MODE=paper."
                )
        return self


CFG = Config().validate()
