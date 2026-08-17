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
    stop_loss_pct: float = field(default_factory=lambda: _f("STOP_LOSS_PCT", 0.6))
    take_profit_1_pct: float = field(default_factory=lambda: _f("TAKE_PROFIT_1_PCT", 0.6))
    take_profit_1_size: float = field(default_factory=lambda: _f("TAKE_PROFIT_1_SIZE", 0.5))
    trailing_stop_pct: float = field(default_factory=lambda: _f("TRAILING_STOP_PCT", 0.4))
    max_leverage: float = field(default_factory=lambda: _f("MAX_LEVERAGE", 3))
    daily_loss_limit_pct: float = field(default_factory=lambda: _f("DAILY_LOSS_LIMIT_PCT", 3.0))
    max_consecutive_losses: int = field(default_factory=lambda: _i("MAX_CONSECUTIVE_LOSSES", 3))
    cooldown_minutes: float = field(default_factory=lambda: _f("COOLDOWN_MINUTES", 120))

    order_fill_timeout_sec: float = field(default_factory=lambda: _f("ORDER_FILL_TIMEOUT_SEC", 8))
    max_reprice_attempts: int = field(default_factory=lambda: _i("MAX_REPRICE_ATTEMPTS", 3))
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
