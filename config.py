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
    # Снижено с 0.15% до 0.05% по прямой просьбе пользователя (18.08, разбор
    # скрина стакана Lighter) - раньше этот "пол" почти на каждой сделке
    # перебивал реальную дистанцию до ближайшей стенки (вход происходит прямо
    # у стенки, поэтому |entry-wall|+буфер обычно меньше 0.15%), и стоп по
    # факту всегда получался одинаковым фиксированным числом, а не от
    # структуры, как задумано. 0.05% - нижняя граница, ниже которой не
    # опускаемся: PAPER_CROSS_BUFFER_PCT=0.05% пересекается на входе И на
    # выходе (round-trip ~0.1%), совсем узкий стоп (0.03% и ниже) означал бы,
    # что даже штатное срабатывание стопа съедается проскальзыванием почти
    # целиком - реальный убыток был бы заметно больше номинального риска.
    min_stop_pct: float = field(default_factory=lambda: _f("MIN_STOP_PCT", 0.05))
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
    # Общий INVALIDATION_CONFIRM_TICKS (см. ниже) даёт этой проверке всего
    # ~2 тика (~2 сек при POSITION_CHECK_INTERVAL_SEC=1) подряд, ДЕЛЯЩИХСЯ
    # между ВСЕМИ ветками _thesis_invalidated сразу - а сравнение стенок само
    # по себе самое шумное из них (стенки "дёргаются" за доли секунды, см.
    # комментарий у wall_dominance_ratio выше). Найдено в проде 18.08 вечером:
    # именно эта ветка (а не подложка и не возраст стенки) была главным
    # источником серии из 69 сделок за 26 минут по 8-40 сек каждая. Даём ЭТОЙ
    # конкретной проверке свой отдельный счётчик подряд идущих тиков (см.
    # pos.wall_dominance_streak в order_manager.py) - страхуется отдельно от
    # общего invalidation_streak, а не расходует с ним общий лимит.
    wall_dominance_confirm_ticks: int = field(default_factory=lambda: _i("WALL_DOMINANCE_CONFIRM_TICKS", 3))

    # === Стенку "съели", а цена не пошла - выходим примерно в ноль ===
    # Прямая просьба пользователя: "если 1.5кк лимитку сьело и цена стоит на
    # месте плюс минус, то выходить в ноль или около того". Раньше стенка,
    # от которой фейдили (см. pos.reference_price), была "поводом выйти" только
    # если И пропала, И цена уже пробила её уровень (реальный слом структуры,
    # обычно это уже минус). Ситуация "стенку съели, но цена просто стоит на
    # месте" не ловилась вообще - позиция висела дальше в ожидании стопа/тейка,
    # хотя сам повод для сделки уже исчез. Если стенки нет и текущий PnL в
    # пределах +/- этого % от входа - считаем это "плоско", выходим сейчас, не
    # дожидаясь ни стопа, ни тейка. Если цена УЖЕ ушла в нашу пользу за пределы
    # этой полосы - НЕ форсируем выход (см. order_manager._thesis_invalidated) -
    # это уже не "тезис умер", а скорее подтверждение (см. отдельный сигнал
    # BREAKOUT в signals.py, построенный ровно на "стенку съели И цена пошла").
    wall_eaten_flat_pct: float = field(default_factory=lambda: _f("WALL_EATEN_FLAT_PCT", 0.08))

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
    # === Эскалация буфера при "зависшем" закрытии (найдено в проде 18.08) ===
    # Реальный случай: позиция ETH на 1.5788 не могла закрыться >3 минут -
    # каждые ~3с закрывающий IOC-ордер с тем же крошечным PAPER_CROSS_BUFFER_PCT
    # снова и снова цеплял одну и ту же тонкую верхушку стакана и исполнялся
    # лишь на ~0.12 ETH за попытку (буфер гарантирует пересечение спреда, но
    # НЕ гарантирует пересечение достаточной ГЛУБИНЫ книги для всего объёма).
    # Бот не "завис" - он честно пытался выйти каждую проверку, просто без
    # эскалации буфер был слишком мал для объёма позиции. Теперь при каждой
    # неполной попытке закрытия буфер расширяется на этот % (см. pos.close_stall_count
    # в order_manager.py), пока не пробьёт достаточно глубоко в книгу, чтобы
    # закрыть остаток целиком - вместо того чтобы долбить одну и ту же цену
    # неопределённо долго.
    paper_cross_buffer_escalation_pct: float = field(
        default_factory=lambda: _f("PAPER_CROSS_BUFFER_ESCALATION_PCT", 0.05))
    # Потолок эскалации - не даём буферу расти бесконечно (это был бы уже не
    # "пересечь спред", а исполнение почти по любой цене).
    paper_cross_buffer_max_pct: float = field(default_factory=lambda: _f("PAPER_CROSS_BUFFER_MAX_PCT", 0.5))

    # Веб-дашборд. Render и другие облачные платформы сами прокидывают порт через PORT -
    # если он задан, он в приоритете над DASHBOARD_PORT.
    dashboard_port: int = field(default_factory=lambda: _i("PORT", _i("DASHBOARD_PORT", 8080)))

    # Трендовый/волатильностный фильтр (см. trend_filter.py)
    trend_ema_fast_sec: float = field(default_factory=lambda: _f("TREND_EMA_FAST_SEC", 15))
    trend_ema_slow_sec: float = field(default_factory=lambda: _f("TREND_EMA_SLOW_SEC", 90))
    vol_lookback: int = field(default_factory=lambda: _i("VOL_LOOKBACK", 60))
    vol_spike_mult: float = field(default_factory=lambda: _f("VOL_SPIKE_MULT", 3.0))

    # === Детектор "мёртвого" рынка (найдено в проде 18.08) ===
    # allows_fade() блокирует фейд ABSORPTION только ПРОТИВ тренда - при
    # trend=="flat" пропускает сигналы в ОБЕ стороны без ограничений. Реальный
    # случай: BTC простоял в коридоре ~25 пунктов (0.04% от цены) 6 минут
    # подряд - бот открыл 8 сделок (фейды то в лонг, то в шорт на один и тот
    # же шум), все 8 в минус, только на проскальзывании/буфере исполнения -
    # реального движения там ловить было нечего. Раньше это было отмечено как
    # "отдельная более глубокая тема" (см. комментарий у REENTRY_COOLDOWN_SEC) -
    # теперь реализовано: если весь диапазон цены за DEAD_RANGE_LOOKBACK_SEC
    # секунд уже, чем DEAD_RANGE_MIN_PCT - ABSORPTION не генерируется вообще
    # (BREAKOUT не трогаем - там сигнал сам по себе означает, что цена ТОЛЬКО
    # ЧТО вышла из диапазона, это не "мёртвый" рынок, а конец боковика).
    # DEAD_RANGE_MIN_PCT=0.08 - первая прикидка по реальному логу того самого
    # дохлого участка (диапазон был 0.04%), не откалибровано по большой
    # выборке - см. WALL_CANDIDATE-подход в signals.py, стоит собрать логи и
    # уточнить, если увидим либо слишком частые "рынок мёртвый" в логах на
    # нормальном рынке, либо что чоп всё равно проскакивает.
    dead_range_lookback_sec: float = field(default_factory=lambda: _f("DEAD_RANGE_LOOKBACK_SEC", 90.0))
    dead_range_min_pct: float = field(default_factory=lambda: _f("DEAD_RANGE_MIN_PCT", 0.08))
    dead_range_min_coverage_sec: float = field(
        default_factory=lambda: _f("DEAD_RANGE_MIN_COVERAGE_SEC", 45.0))

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
    # тике. Нужен отдельный, заметно более высокий порог. Снижено с 2М до 1.5М по
    # прямой просьбе пользователя - "хочу чтобы заходил от 1.5кк".
    binance_wall_min_usd: float = field(default_factory=lambda: _f("BINANCE_WALL_MIN_USD", 1_500_000))
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

    # === Кулдаун на новый вход после закрытия (найдено в проде 18.08) ===
    # Реальный случай сразу после деплоя: BTC болтался в диапазоне ~30 пунктов
    # (64200-64230, ~0.05%) - trend_filter.allows_fade() блокирует фейд только
    # ПРОТИВ тренда (up/down), а при trend=="flat" (ровно этот случай) пропускает
    # ABSORPTION в ОБЕ стороны без ограничений. В итоге бот открывал лонг,
    # структура тут же "разваливалась" (умный выход, см. _thesis_invalidated),
    # закрывал в минус, тут же по новому ABSORPTION-сигналу открывал шорт на
    # том же самом диапазоне, тот тоже закрывался в минус - и так по кругу
    # (7 сделок за 4 минуты, 6 из них в минус, только на комиссии/проскальзывание
    # круговой сделки). НЕ трогаем reversal_signal (немедленный разворот при
    # НАСТОЯЩЕМ сигнале против позиции - отдельная, ранее прямо запрошенная
    # пользователем логика) - кулдаун применяется только к НОВОМУ входу с нуля
    # после закрытия по stop_loss/opposing_wall/structure_invalidated, чтобы не
    # влезать сразу же обратно в тот же шум/чоп, который только что и привёл к
    # закрытию. Не решает чоп полностью (это отдельная, более глубокая тема -
    # детектор "мёртвого" рынка по волатильности), но обрывает самый быстрый и
    # дорогой цикл - мгновенный флип-флоп на одном и том же уровне.
    # Было снижено с 15 до 1 сек 18.08 в расчёте на то, что основной защитой от
    # мёртвого боковика станет DEAD_RANGE_MIN_PCT (см. выше) - но тот гейт в
    # итоге в тот же день отключён (см. is_dead в signals.py, "if False and
    # trend_state.is_dead"), и обе защиты одновременно оказались выключены.
    # Найдено в проде 18.08 вечером: при 1 сек кулдауне бот закрывал позицию
    # по structure_invalidated (см. _thesis_invalidated) и через 4-25 сек уже
    # снова открывал ту же сторону по той же, по сути повторяющейся, ABSORPTION-
    # сигнатуре у того же уровня - 69 сделок за 26 минут в плоском рынке,
    # PnL -13.59, почти все закрытия за 8-40 сек. Поднято обратно до 20 сек -
    # не полные 15 (как было исходно), но достаточно, чтобы не заходить сразу
    # обратно в тот же шум, пока не появится новый реальный повод.
    reentry_cooldown_sec: float = field(default_factory=lambda: _f("REENTRY_COOLDOWN_SEC", 20.0))

    # === Кулдаун на ПОВТОРНЫЙ разворот (найдено в проде 18.08) ===
    # REENTRY_COOLDOWN_SEC выше не действует на развороты (existing is not
    # None в handle_signal) - это сознательно, по прямой просьбе пользователя
    # "после лонга надо сразу шортить, а не тянуть". В трендовом/спокойном
    # рынке это правильно. Но в боковике (trend=="flat") тренд-фильтр вообще
    # не мешает фейдить в обе стороны - и на реальном проде это дало серию
    # мгновенных разворотов подряд (long->short->long->short 4 раза за
    # ~26 секунд), почти все в минус - каждый разворот сам по себе "по
    # правилам" (сигнал реально пришёл против позиции), просто сигналы шли
    # слишком часто в узком коридоре. Этот кулдаун ограничивает не сам факт
    # входа (как REENTRY_COOLDOWN_SEC), а частоту РАЗВОРОТОВ - если только
    # что развернулись, следующий разворот по этому символу игнорируем
    # (сигнал пропускается, текущая позиция остаётся и живёт по своим
    # обычным правилам выхода - стоп/тейк/structure_invalidated), пока не
    # пройдёт этот интервал.
    # Снижено с 10 до 1 сек по прямой просьбе пользователя 18.08 - см. тот же
    # комментарий у REENTRY_COOLDOWN_SEC выше.
    reversal_cooldown_sec: float = field(default_factory=lambda: _f("REVERSAL_COOLDOWN_SEC", 1.0))

    # === Аудит стратегии 18.08 - этап 2 ("убрать очевидный мусор") ===
    # zone_cancels_5m раньше только логировался внутри WALL_SCORE (ни на что
    # не влиял) - если в одной и той же ценовой зоне заявки регулярно
    # снимаются именно при подходе цены (>= этого числа за последние 5 минут,
    # см. _zone_cancel_count в signals.py), это подозрительно похоже на
    # спуфинг, и сигнал по такой зоне (и ABSORPTION, и BREAKOUT) пропускается.
    # ПОРОГ НЕ ОТКАЛИБРОВАН по выборке - первая прикидка (как когда-то
    # DEAD_RANGE_MIN_PCT), отклонённые кандидаты по-прежнему логируются
    # (reason=spoof_zone_cancels) для последующей проверки по накопленным
    # данным (wall_event_log.py).
    spoof_zone_cancel_max: int = field(default_factory=lambda: _i("SPOOF_ZONE_CANCEL_MAX", 4))

    # === TIME_EXIT (аудит стратегии 18.08, этап 2.3) ===
    # Если сделка открыта дольше TIME_EXIT_SEC и за это время так и не сдвинулась
    # в нашу пользу дальше TIME_EXIT_MIN_PROFIT_PCT - тезис явно не отрабатывает
    # (ни быстрый импульс, на который и рассчитана эта стратегия, ни нормальный
    # ход к TP1), закрываем вместо того чтобы висеть неопределённо долго в
    # ожидании стопа. БАЗОВАЯ версия - плоское время без учёта волатильности/
    # режима рынка; это будет уточнено в этапе 5 аудита (TIME_EXIT должен
    # работать только для micro-scalping и учитывать текущую волатильность).
    time_exit_sec: float = field(default_factory=lambda: _f("TIME_EXIT_SEC", 90.0))
    time_exit_min_profit_pct: float = field(default_factory=lambda: _f("TIME_EXIT_MIN_PROFIT_PCT", 0.05))
    # === Уточнение TIME_EXIT (аудит, этап 5) ===
    # TIME_EXIT задуман для micro-scalp тезиса ABSORPTION ("либо отрабатывает
    # быстро, либо повода держать больше нет") - BREAKOUT это трендовое
    # продолжение, у него законно может уйти больше времени на разгон, поэтому
    # свой, намного более длинный лимит.
    time_exit_breakout_sec: float = field(default_factory=lambda: _f("TIME_EXIT_BREAKOUT_SEC", 240.0))
    # Масштабирование TIME_EXIT_SEC текущей волатильностью на входе (см.
    # Signal.volatility_pct/pos.entry_volatility_pct) - при более высокой
    # волатильности, чем этот референс, тезис должен отработать быстрее
    # (окно сокращается, до 0.5x), при более низкой - разумно дать больше
    # времени (окно растёт, до 2x), зажато в [TIME_EXIT_MIN_SEC, TIME_EXIT_MAX_SEC].
    time_exit_vol_ref_pct: float = field(default_factory=lambda: _f("TIME_EXIT_VOL_REF_PCT", 0.03))
    time_exit_min_sec: float = field(default_factory=lambda: _f("TIME_EXIT_MIN_SEC", 30.0))
    time_exit_max_sec: float = field(default_factory=lambda: _f("TIME_EXIT_MAX_SEC", 300.0))

    # === THESIS WEAKENING - частичный выход по затуханию импульса (аудит,
    # этап 5) ===
    # Отдельно от THESIS INVALIDATED (структура реально сломалась - см.
    # _thesis_invalidated, полное закрытие) - WEAKENING ловит момент, когда
    # позиция УЖЕ была в заметном плюсе (MFE), а потом заметная часть этого
    # плюса откатилась НАЗАД, хотя формальная структура (стенка/уровень) ещё
    # не инвалидирована. Частичный выход фиксирует часть уже заработанного,
    # остаток продолжает жить по обычным правилам (SL/TP1/трейлинг/INVALIDATED).
    # MFE должен быть заметно больше шумовой полосы (WALL_EATEN_FLAT_PCT),
    # иначе это сработает на обычном шуме сразу после входа.
    weakening_mfe_min_mult: float = field(default_factory=lambda: _f("WEAKENING_MFE_MIN_MULT", 1.5))
    # Доля отката ОТ MFE (не от entry), после которой считаем импульс
    # затухающим - например 0.5 значит "откатили половину уже набранного пути".
    momentum_decay_retrace_pct: float = field(default_factory=lambda: _f("MOMENTUM_DECAY_RETRACE_PCT", 0.5))
    # Какую долю ОСТАВШЕГОСЯ размера закрыть при первом срабатывании WEAKENING
    # (срабатывает не больше одного раза за сделку - см. ManagedPosition.weakening_partial_done).
    weakening_partial_close_pct: float = field(default_factory=lambda: _f("WEAKENING_PARTIAL_CLOSE_PCT", 0.3))

    # === Shadow-режим для новой ABSORPTION/BREAKOUT логики (аудит, этап 3/4) ===
    # ВАЖНО: эти пороги НЕ гейтят реальные сигналы - только логируются как
    # WOULD_ENTER/WOULD_SKIP в wall_event_log.py (shadow_evals) для сравнения с
    # реальным исходом (WALL_OUTCOME) ПОСЛЕ накопления данных, по прямой
    # договорённости с пользователем ("не оптимизируй пороги, пока не
    # накопится достаточно данных"). Все значения ниже - первые прикидки,
    # не откалиброваны.
    shadow_min_executed_ratio: float = field(default_factory=lambda: _f("SHADOW_MIN_EXECUTED_RATIO", 0.15))
    shadow_weakening_flow_ratio: float = field(default_factory=lambda: _f("SHADOW_WEAKENING_FLOW_RATIO", 0.7))
    shadow_breakout_min_age_sec: float = field(default_factory=lambda: _f("SHADOW_BREAKOUT_MIN_AGE_SEC", 4.0))
    shadow_breakout_min_executed_ratio: float = field(
        default_factory=lambda: _f("SHADOW_BREAKOUT_MIN_EXECUTED_RATIO", 0.2))
    shadow_breakout_eaten_ratio: float = field(default_factory=lambda: _f("SHADOW_BREAKOUT_EATEN_RATIO", 0.5))

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
