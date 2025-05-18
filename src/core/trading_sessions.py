from dataclasses import dataclass
from datetime import time, date, datetime, timedelta
import pytz

@dataclass
class SessionDefinition:
    """
    Определяет торговую сессию через ее локальное время на бирже и часовой пояс биржи.
    """
    name: str                 # Название сессии (например, "Xetra Trading", "NYSE Trading")
    exchange_timezone: str    # Часовой пояс биржи (например, "Europe/Berlin", "America/New_York", "UTC")
    local_start_time: time    # Локальное время начала сессии на бирже
    local_end_time: time      # Локальное время окончания сессии на бирже
    # Поле для описания, если нужно
    description: str = ""

    def __post_init__(self):
        # Проверка, что часовой пояс валидный
        try:
            pytz.timezone(self.exchange_timezone)
        except pytz.exceptions.UnknownTimeZoneError:
            raise ValueError(f"Неизвестный часовой пояс: {self.exchange_timezone}")

def get_utc_session_boundaries_for_date(
    session_def: SessionDefinition, 
    target_date: date
) -> tuple[datetime, datetime]:
    """
    Рассчитывает время начала и конца сессии в UTC для конкретной даты,
    с учетом часового пояса биржи и перехода на летнее/зимнее время (DST).

    Args:
        session_def: Определение сессии (SessionDefinition).
        target_date: Конкретная дата, для которой рассчитываются границы сессии.

    Returns:
        Кортеж (start_utc_dt, end_utc_dt) с datetime объектами в UTC.
        Возбуждает исключение, если не удается определить время (например, из-за DST перехода в момент открытия/закрытия).
    """
    tz = pytz.timezone(session_def.exchange_timezone)

    # Создаем datetime объекты в локальном времени биржи
    # is_dst=None используется для обработки неоднозначного времени при переходе DST
    # (хотя для биржевых часов это обычно не проблема, т.к. они фиксированы)
    local_start_dt_naive = datetime.combine(target_date, session_def.local_start_time)
    local_start_dt = tz.localize(local_start_dt_naive, is_dst=None) # is_dst=None вызовет AmbiguousTimeError/NonExistentTimeError если надо

    # Если сессия пересекает полночь в локальном времени (например, 22:00 - 02:00)
    if session_def.local_end_time < session_def.local_start_time:
        local_end_dt_naive = datetime.combine(target_date + timedelta(days=1), session_def.local_end_time)
    else:
        local_end_dt_naive = datetime.combine(target_date, session_def.local_end_time)
    
    local_end_dt = tz.localize(local_end_dt_naive, is_dst=None)

    # Конвертируем в UTC
    start_utc_dt = local_start_dt.astimezone(pytz.utc)
    end_utc_dt = local_end_dt.astimezone(pytz.utc)

    return start_utc_dt, end_utc_dt

# --- Примеры определений сессий --- 

# Xetra (Франкфурт) для GER40/DAX
# Часы работы: 09:00 - 17:30 Europe/Berlin (включая аукционы)
# Основная торговля: 09:00 - 17:30 CET/CEST. Фьючерсы торгуются дольше.
# Для простоты берем основные часы.
XETRA_FRANKFURT_SESSION = SessionDefinition(
    name="Frankfurt_Xetra_Main",
    exchange_timezone="Europe/Berlin",
    local_start_time=time(9, 0),
    local_end_time=time(17, 30),
    description="Основные торговые часы Xetra (DAX/GER40)"
)

# London Stock Exchange (LSE)
# Часы работы: 08:00 - 16:30 Europe/London
LSE_LONDON_SESSION = SessionDefinition(
    name="London_LSE_Main",
    exchange_timezone="Europe/London",
    local_start_time=time(8, 0),
    local_end_time=time(16, 30),
    description="Основные торговые часы London Stock Exchange"
)

# New York Stock Exchange (NYSE)
# Часы работы: 09:30 - 16:00 America/New_York
NYSE_NEWYORK_SESSION = SessionDefinition(
    name="NewYork_NYSE_Main",
    exchange_timezone="America/New_York",
    local_start_time=time(9, 30),
    local_end_time=time(16, 0),
    description="Основные торговые часы New York Stock Exchange"
)

# Условная "Азиатская сессия" - можно определить как некий общий диапазон UTC,
# так как "Азия" - это много разных бирж с разным временем.
# Или можно определить конкретную биржу, например, Токио.
# Tokyo Stock Exchange (TSE): 09:00-11:30, 12:30-15:00 Asia/Tokyo
# Здесь для примера оставим обобщенную UTC-сессию, если она нужна для каких-то широких оценок.
# Но для точности лучше использовать конкретные биржи.
ASIA_GENERIC_UTC_SESSION = SessionDefinition(
    name="Asia_Generic_UTC",
    exchange_timezone="UTC", # Указываем UTC как таймзону
    local_start_time=time(0, 0), # 00:00 UTC
    local_end_time=time(9, 0),   # 09:00 UTC
    description="Обобщенная Азиатская сессия в UTC (00:00-09:00 UTC)"
)

# Пример для Токийской биржи (с перерывом, который здесь не учитывается в одной сессии)
# Чтобы учесть перерыв, нужно будет определять две сессии или усложнять логику.
TSE_TOKYO_MORNING_SESSION = SessionDefinition(
    name="Tokyo_TSE_Morning",
    exchange_timezone="Asia/Tokyo",
    local_start_time=time(9,0),
    local_end_time=time(11,30)
)
TSE_TOKYO_AFTERNOON_SESSION = SessionDefinition(
    name="Tokyo_TSE_Afternoon",
    exchange_timezone="Asia/Tokyo",
    local_start_time=time(12,30),
    local_end_time=time(15,0)
)

# Словарь для быстрого доступа к определениям сессий
SUPPORTED_SESSIONS = {
    "frankfurt_xetra": XETRA_FRANKFURT_SESSION,
    "london_lse": LSE_LONDON_SESSION,
    "newyork_nyse": NYSE_NEWYORK_SESSION,
    "asia_generic_utc": ASIA_GENERIC_UTC_SESSION,
    "tokyo_morning": TSE_TOKYO_MORNING_SESSION,
    "tokyo_afternoon": TSE_TOKYO_AFTERNOON_SESSION,
}

# TODO (из предыдущей версии, актуализировано):
# 1. Учет праздников для бирж: get_utc_session_boundaries_for_date сейчас не знает о праздниках.
#    Это потребует источника данных о праздниках (например, библиотека `holidays` или API).
# 2. Более сложная обработка перерывов в торговле (как у TSE_TOKYO) в SessionAnalyzer.
#    Текущий SessionAnalyzer обрабатывает одну непрерывную сессию за день.
# 3. Управление конфигурацией сессий: вынести определения сессий (SUPPORTED_SESSIONS)
#    в отдельный конфигурационный файл (JSON, YAML) для гибкости.


# --- Пример использования функции get_utc_session_boundaries_for_date ---
if __name__ == '__main__':
    # Даты для проверки DST перехода (пример для Европы: последнее воскресенье марта и октября)
    # В 2023 для Europe/Berlin: DST начался 26 марта, закончился 29 октября.
    date_before_dst_start = date(2023, 3, 25) # Суббота, еще зимнее время (UTC+1)
    date_on_dst_start = date(2023, 3, 26)   # Воскресенье, переход на летнее время (UTC+2)
    date_after_dst_start = date(2023, 3, 27)  # Понедельник, уже летнее время

    date_before_dst_end = date(2023, 10, 28) # Суббота, еще летнее время (UTC+2)
    date_on_dst_end = date(2023, 10, 29)     # Воскресенье, переход на зимнее время (UTC+1)
    date_after_dst_end = date(2023, 10, 30)   # Понедельник, уже зимнее время

    xetra_def = SUPPORTED_SESSIONS["frankfurt_xetra"]
    print(f"--- {xetra_def.name} ({xetra_def.exchange_timezone}, local: {xetra_def.local_start_time}-{xetra_def.local_end_time}) ---")

    print(f"\nDate: {date_before_dst_start} (Winter Time CET = UTC+1)")
    start_utc, end_utc = get_utc_session_boundaries_for_date(xetra_def, date_before_dst_start)
    print(f"  Local Start: {datetime.combine(date_before_dst_start, xetra_def.local_start_time).strftime('%H:%M:%S %Z%z')} (naive, shown for context)")
    print(f"  Localized Start: {pytz.timezone(xetra_def.exchange_timezone).localize(datetime.combine(date_before_dst_start, xetra_def.local_start_time)).strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
    print(f"  Session UTC: {start_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')} - {end_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
    # Ожидаем: 09:00 CET (UTC+1) -> 08:00 UTC; 17:30 CET (UTC+1) -> 16:30 UTC

    print(f"\nDate: {date_on_dst_start} (DST Transition Day)")
    # На большинстве бирж, если переход DST происходит в неторговый день (воскресенье), 
    # то первый торговый день после перехода уже будет по новому времени.
    # Если бы переход был в торговый день, это могло бы вызвать проблемы.
    start_utc, end_utc = get_utc_session_boundaries_for_date(xetra_def, date_on_dst_start)
    print(f"  Session UTC: {start_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')} - {end_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
    # Ожидаем: 09:00 CEST (UTC+2) -> 07:00 UTC; 17:30 CEST (UTC+2) -> 15:30 UTC

    print(f"\nDate: {date_after_dst_start} (Summer Time CEST = UTC+2)")
    start_utc, end_utc = get_utc_session_boundaries_for_date(xetra_def, date_after_dst_start)
    print(f"  Session UTC: {start_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')} - {end_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
    # Ожидаем: 09:00 CEST (UTC+2) -> 07:00 UTC; 17:30 CEST (UTC+2) -> 15:30 UTC

    print(f"\nDate: {date_before_dst_end} (Summer Time CEST = UTC+2)")
    start_utc, end_utc = get_utc_session_boundaries_for_date(xetra_def, date_before_dst_end)
    print(f"  Session UTC: {start_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')} - {end_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")

    print(f"\nDate: {date_on_dst_end} (DST Transition Day)")
    start_utc, end_utc = get_utc_session_boundaries_for_date(xetra_def, date_on_dst_end)
    print(f"  Session UTC: {start_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')} - {end_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
    
    print(f"\nDate: {date_after_dst_end} (Winter Time CET = UTC+1)")
    start_utc, end_utc = get_utc_session_boundaries_for_date(xetra_def, date_after_dst_end)
    print(f"  Session UTC: {start_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')} - {end_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")

    # Пример для NYSE
    nyse_def = SUPPORTED_SESSIONS["newyork_nyse"]
    # DST в США в 2023: начался 12 марта, закончился 5 ноября.
    date_nyse_summer = date(2023, 7, 10) # Летнее время (EDT = UTC-4)
    date_nyse_winter = date(2023, 12, 10) # Зимнее время (EST = UTC-5)
    print(f"\n--- {nyse_def.name} ({nyse_def.exchange_timezone}, local: {nyse_def.local_start_time}-{nyse_def.local_end_time}) ---")
    start_utc, end_utc = get_utc_session_boundaries_for_date(nyse_def, date_nyse_summer)
    print(f"Date: {date_nyse_summer} (Summer EDT = UTC-4) -> UTC: {start_utc.strftime('%H:%M')}-{end_utc.strftime('%H:%M')}")
    # Ожидаем: 09:30 EDT (UTC-4) -> 13:30 UTC; 16:00 EDT (UTC-4) -> 20:00 UTC
    start_utc, end_utc = get_utc_session_boundaries_for_date(nyse_def, date_nyse_winter)
    print(f"Date: {date_nyse_winter} (Winter EST = UTC-5) -> UTC: {start_utc.strftime('%H:%M')}-{end_utc.strftime('%H:%M')}")
    # Ожидаем: 09:30 EST (UTC-5) -> 14:30 UTC; 16:00 EST (UTC-5) -> 21:00 UTC

    # Пример для сессии, пересекающей полночь (гипотетический)
    # Важно: yfinance обычно не дает данных для бирж, которые так работают, но функция должна это обработать
    midnight_cross_session = SessionDefinition(
        name="Hypothetical Midnight Cross",
        exchange_timezone="Australia/Sydney", # AEDT (UTC+11) / AEST (UTC+10)
        local_start_time=time(22,0), # 10 PM Sydney
        local_end_time=time(5,0)     # 5 AM Sydney next day
    )
    date_sydney = date(2023,12,5)
    print(f"\n--- {midnight_cross_session.name} ({midnight_cross_session.exchange_timezone}, local: {midnight_cross_session.local_start_time}-{midnight_cross_session.local_end_time}) ---")
    start_utc, end_utc = get_utc_session_boundaries_for_date(midnight_cross_session, date_sydney)
    print(f"Date: {date_sydney} -> UTC: {start_utc.strftime('%Y-%m-%d %H:%M %Z')} - {end_utc.strftime('%Y-%m-%d %H:%M %Z')}")
    # Sydney 5 Dec 22:00 AEDT (UTC+11) -> 5 Dec 11:00 UTC
    # Sydney 6 Dec 05:00 AEDT (UTC+11) -> 5 Dec 18:00 UTC (если это не ошибка, UTC тоже должен перейти на следующий день?)
    # Нет, 6 Dec 05:00 AEDT -> 5 Dec 18:00 UTC. Верно. Энд дата должна быть +1 от таргет даты.
    # Если target_date = 5 Dec, то local_end_dt_naive будет для 6 Dec.
    # local_start_dt (5 Dec 22:00 +11) -> 5 Dec 11:00 UTC
    # local_end_dt   (6 Dec 05:00 +11) -> 5 Dec 18:00 UTC (не верно, должно быть 6 Dec 05:00 AEDT -> 5 Dec 18:00 UTC)
    # Ошибка в рассуждении. 6 Dec 05:00 local (AEDT = UTC+11) это 5 Dec 18:00 UTC. Это правильно.
    # 2023-12-05 22:00:00 в Australia/Sydney это 2023-12-05 11:00:00 UTC.
    # 2023-12-06 05:00:00 в Australia/Sydney это 2023-12-05 18:00:00 UTC. 
    # Похоже, моя проверка в уме была неверна, pytz должен делать это правильно.
    # Да, проверил: 2023-12-06 05:00:00 AEDT == 2023-12-05 18:00:00 UTC. Все верно. 