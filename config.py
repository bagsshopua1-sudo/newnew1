"""
Загрузка и валидация конфигурации бота из .env / переменных окружения.

ПЕРЕСТРОЕНО 21.08 по прямому и явному запросу пользователя - вся торговая
логика заменена на одну простую механику:

    ORDER BOOK -> EDGE -> ENTRY -> REPRICE/CANCEL -> HOLD/REDUCE -> EXIT

Смотрим крупнейшую отдельную заявку (стенку) на каждой стороне глубокого
стакана Binance рядом с ценой. Если одна сторона решительно (в
WALL_ADVANTAGE_RATIO_MIN раз) крупнее другой И крупная сторона не меньше
BINANCE_WALL_MIN_USD - это EDGE. Без решительного перевеса - NO TRADE, ждём
следующего снепшота, а не подгоняем сделку под каждый тик книги.

Старая логика (REAL_WALL/ABSORPTION/BREAKOUT/SPOOF классификация, executed-
volume тейп с Binance, microprice, трендовый фильтр, детектор мёртвого
рынка, opposing-wall/opposite-flow/thesis-weakening выходы, dynamic
trailing, TIME_EXIT) была специально построена под другую механику ("не
размер стенки, а динамика вокруг неё") и по прямой просьбе пользователя
убрана целиком - десятки взаимозависимых порогов конфликтовали бы с новой,
явно более простой механикой, которую пользователь прямо попросил не
усложнять. Все параметры ниже, оставшиеся от старой логики, удалены; файлы
signals.py/trend_filter.py на диске могут содержать старый код, но больше
никем не импортируются.
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

    # === Источник сигнала - глубокий стакан Binance Futures (публичный WS,
    # без ключей) - см. binance_feed.py. Исполнение всё равно на Lighter. ===
    use_binance_signals: bool = field(default_factory=lambda: _s("USE_BINANCE_SIGNALS", "true").lower() == "true")
    # Partial book depth: сколько уровней и с какой скоростью обновления.
    # Пользователь прямо просил мониторить стакан "каждые 300-500 мс" - 400мс
    # взято серединой этого диапазона.
    binance_ws_depth_levels: int = field(default_factory=lambda: _i("BINANCE_WS_DEPTH_LEVELS", 20))
    binance_ws_speed_ms: int = field(default_factory=lambda: _i("BINANCE_WS_SPEED_MS", 400))

    # === EDGE: крупнейшая отдельная заявка на каждой стороне книги рядом с
    # ценой (см. signals.py::_top_wall_usd) ===
    # Порог "крупная стенка" на Lighter-стакане (только если USE_BINANCE_SIGNALS=false,
    # у Lighter стакан на порядки тоньше Binance - другой порядок величины).
    wall_min_usd: float = field(default_factory=lambda: _f("WALL_MIN_USD", 150_000))
    # Порог "крупная стенка" на Binance-стакане - прямой пример пользователя:
    # "BID = $1.2M -> сильный LONG bias". Это порог для СТОРОНЫ, которая
    # претендует на edge, а не для обеих сторон сразу - встречная сторона
    # может (и почти всегда будет) быть значительно меньше этого числа, см.
    # WALL_ADVANTAGE_RATIO_MIN ниже.
    binance_wall_min_usd: float = field(default_factory=lambda: _f("BINANCE_WALL_MIN_USD", 1_200_000))
    # Как далеко от mid (% от цены) вообще смотрим на заявки при поиске
    # крупнейшей на каждой стороне - слишком далёкая крупная заявка не
    # является реальным давлением на текущую цену.
    wall_max_distance_pct: float = field(default_factory=lambda: _f("WALL_MAX_DISTANCE_PCT", 0.3))
    # Реальный "wall advantage ratio", а не просто факт наличия стенки -
    # прямая просьба пользователя. Пример: BID=$1.2M / ASK=$200K -> ratio=6.0
    # -> явный LONG edge. BID=$1.2M / ASK=$1.0M -> ratio=1.2 -> NO TRADE,
    # встречная ликвидность слишком большая, преимущество недостаточное.
    wall_advantage_ratio_min: float = field(default_factory=lambda: _f("WALL_ADVANTAGE_RATIO_MIN", 2.5))
    # Анти-спуф / анти-случайная-заявка: одной стороне нужно оставаться
    # ЛУЧШЕЙ (см. wall_advantage_ratio_min) минимум WALL_CONFIRM_UPDATES
    # обновлений стакана подряд И минимум WALL_MIN_PERSIST_SEC секунд,
    # прежде чем это станет реальным сигналом на вход - прямая просьба
    # пользователя "не входить из-за одной случайной заявки" + "проверять,
    # что крупная заявка держится несколько обновлений". Стенка, которая
    # появилась и тут же пропала (типичный спуф), никогда не набирает нужное
    # число подтверждений - отдельного "антиспуф"-флага не нужно, это прямое
    # следствие самого требования персистентности.
    wall_confirm_updates: int = field(default_factory=lambda: _i("WALL_CONFIRM_UPDATES", 4))
    wall_min_persist_sec: float = field(default_factory=lambda: _f("WALL_MIN_PERSIST_SEC", 1.0))
    # Если сигнал успел устареть к моменту, когда бот его реально обрабатывает
    # (задержка сети/CPU) - отменяем вход, структура могла уже измениться.
    max_signal_age_ms: float = field(default_factory=lambda: _f("MAX_SIGNAL_AGE_MS", 400.0))
    # Пауза перед новым входом с нуля после закрытия по этому же символу -
    # не влезаем сразу обратно в тот же шум сразу после CLOSE.
    reentry_cooldown_sec: float = field(default_factory=lambda: _f("REENTRY_COOLDOWN_SEC", 10.0))

    # === EXIT: постоянный пересчёт LONG EDGE / SHORT EDGE на открытой
    # позиции (см. order_manager.py::_watch_position) ===
    # Тот же принцип персистентности, что и на входе, но короче - пользователь
    # прямо просил реагировать НЕМЕДЛЕННО на разворот edge ("немедленно
    # пересчитать позицию"), это не про "тезис входа был неверным" (там нужна
    # осторожность), а про новый факт на рынке прямо сейчас. 2 тика (~0.8-1с
    # при BINANCE_WS_SPEED_MS=400) - всё ещё гасит одиночное мигание стенки в
    # снепшоте книги, но не тянет с реакцией.
    edge_exit_confirm_ticks: int = field(default_factory=lambda: _i("EDGE_EXIT_CONFIRM_TICKS", 2))
    # Если edge на нашей стороне ослаб (уже не проходит WALL_ADVANTAGE_RATIO_MIN),
    # но ещё не развернулся ПОЛНОСТЬЮ против нас - REDUCE на эту долю
    # оставшегося размера (один раз за эпизод ослабления, см. ManagedPosition.edge_state).
    edge_reduce_fraction: float = field(default_factory=lambda: _f("EDGE_REDUCE_FRACTION", 0.5))

    # === Риск / размер позиции ===
    account_equity_usd: float = field(default_factory=lambda: _f("ACCOUNT_EQUITY_USD", 100))
    risk_per_trade_pct: float = field(default_factory=lambda: _f("RISK_PER_TRADE_PCT", 1.0))
    # Запасной вариант для risk.build_plan(), если по какой-то причине нет
    # цены стенки-опоры (в норме не используется, стоп считается от структуры).
    stop_loss_pct: float = field(default_factory=lambda: _f("STOP_LOSS_PCT", 0.6))
    # "Умный" стоп = |entry - reference_price EDGE-стенки| + буфер, зажатый в
    # [MIN_STOP_PCT, MAX_STOP_PCT] от цены входа - структура рынка на момент
    # сигнала определяет дистанцию, а не одно и то же число для всех сделок.
    min_stop_pct: float = field(default_factory=lambda: _f("MIN_STOP_PCT", 0.05))
    max_stop_pct: float = field(default_factory=lambda: _f("MAX_STOP_PCT", 1.5))
    stop_buffer_pct: float = field(default_factory=lambda: _f("STOP_BUFFER_PCT", 0.05))
    # ПРИМЕЧАНИЕ: rr_target_1/take_profit_1_size/trailing_stop_pct ниже
    # больше НЕ управляют выходом (см. новую EXIT-механику выше - CLOSE/REDUCE/
    # HOLD решает исключительно edge, не фиксированный TP/трейлинг). Оставлены
    # только потому, что risk.TradePlan (risk.py, не тронут в этой рестройке)
    # всё ещё их считает - order_manager.py эти поля TradePlan просто не читает.
    rr_target_1: float = field(default_factory=lambda: _f("RR_TARGET_1", 1.5))
    take_profit_1_size: float = field(default_factory=lambda: _f("TAKE_PROFIT_1_SIZE", 0.5))
    trailing_stop_pct: float = field(default_factory=lambda: _f("TRAILING_STOP_PCT", 0.4))
    max_leverage: float = field(default_factory=lambda: _f("MAX_LEVERAGE", 50))
    daily_loss_limit_pct: float = field(default_factory=lambda: _f("DAILY_LOSS_LIMIT_PCT", 3.0))
    max_consecutive_losses: int = field(default_factory=lambda: _i("MAX_CONSECUTIVE_LOSSES", 3))
    cooldown_minutes: float = field(default_factory=lambda: _f("COOLDOWN_MINUTES", 120))

    # === Исполнение на Lighter (см. order_manager.py) ===
    order_fill_timeout_sec: float = field(default_factory=lambda: _f("ORDER_FILL_TIMEOUT_SEC", 8))
    max_reprice_attempts: int = field(default_factory=lambda: _i("MAX_REPRICE_ATTEMPTS", 3))
    # Как часто (сек) проверять открытую позицию: стоп по цене и пересчёт
    # EDGE (CLOSE/REDUCE/HOLD).
    position_check_interval_sec: float = field(default_factory=lambda: _f("POSITION_CHECK_INTERVAL_SEC", 1.0))
    # Render free tier периодически перезапускает процесс сам по себе - не
    # обрабатываем сигналы первые STARTUP_GRACE_SEC секунд после запуска,
    # пока стакан/wall-tracking не устаканятся после свежего WS-коннекта.
    startup_grace_sec: float = field(default_factory=lambda: _f("STARTUP_GRACE_SEC", 15.0))
    # lighter.PaperClient умеет исполнять только IOC, пересекающие спред -
    # небольшой буфер пересечения на входе/выходе (не используется в live).
    paper_cross_buffer_pct: float = field(default_factory=lambda: _f("PAPER_CROSS_BUFFER_PCT", 0.05))
    paper_cross_buffer_escalation_pct: float = field(
        default_factory=lambda: _f("PAPER_CROSS_BUFFER_ESCALATION_PCT", 0.05))
    paper_cross_buffer_max_pct: float = field(default_factory=lambda: _f("PAPER_CROSS_BUFFER_MAX_PCT", 0.5))

    # Веб-дашборд. Render и другие облачные платформы сами прокидывают порт через PORT.
    dashboard_port: int = field(default_factory=lambda: _i("PORT", _i("DASHBOARD_PORT", 8080)))

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
