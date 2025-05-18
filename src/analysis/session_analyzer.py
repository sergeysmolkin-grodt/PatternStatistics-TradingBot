import pandas as pd
from datetime import datetime, time, date
import pytz # Добавим pytz для работы с часовыми поясами
from typing import Optional

from src.core.trading_sessions import SessionDefinition
from src.core.data_manager import DataManager # Может понадобиться для получения данных
from src.data_ingestion.yahoo_finance_connector import YahooFinanceConnector # Для примера
import logging

logger = logging.getLogger(__name__)

class SessionAnalyzer:
    """
    Анализирует рыночные данные в контексте определенных торговых сессий.
    """

    def __init__(self, data_manager: DataManager):
        self.data_manager = data_manager

    def get_session_data(
        self,
        symbol: str,
        session_definition: SessionDefinition,
        start_date_dt: datetime,
        end_date_dt: datetime,
        data_interval: str = "1h" # Интервал данных, которые будем запрашивать
    ) -> pd.DataFrame:
        """
        Извлекает данные для указанной торговой сессии за определенный период.

        Args:
            symbol: Тикер инструмента.
            session_definition: Определение торговой сессии (SessionDefinition).
            start_date_dt: Начальная дата периода для анализа (datetime объект).
            end_date_dt: Конечная дата периода для анализа (datetime объект).
            data_interval: Интервал запрашиваемых данных (например, "1h", "15m"). 
                           Важно, чтобы он был достаточно мелким для анализа сессии.

        Returns:
            pandas.DataFrame, содержащий только те свечи, которые попадают
            во временные рамки указанной сессии для каждого дня в периоде.
            Индекс DataFrame будет оригинальным DatetimeIndex.
            Возвращает пустой DataFrame, если данные не найдены или произошла ошибка.
        """
        logger.info(f"Fetching data for {symbol} from {start_date_dt} to {end_date_dt} with interval {data_interval} for session analysis.")
        # Загружаем данные с помощью DataManager
        # Убедимся, что start_date_dt и end_date_dt передаются как datetime объекты
        market_data = self.data_manager.get_data(symbol, start_date_dt, end_date_dt, interval=data_interval)

        if market_data.empty:
            logger.warning(f"No market data found for {symbol} to analyze session {session_definition.name}.")
            return pd.DataFrame()

        # Убедимся, что индекс DataFrame - это DatetimeIndex и он локализован в UTC, 
        # так как SessionDefinition.start_time_utc и end_time_utc заданы в UTC.
        # yfinance обычно возвращает данные с таймзоной (например, America/New_York для акций США, Europe/Berlin для DAX)
        # или наивные datetime, которые предполагаются UTC для некоторых случаев.
        # Нам нужно привести все к UTC для согласованного сравнения.
        if market_data.index.tz is None:
            logger.debug(f"Localizing naive DatetimeIndex for {symbol} to UTC.")
            market_data.index = market_data.index.tz_localize('UTC')
        else:
            logger.debug(f"Converting DatetimeIndex for {symbol} to UTC from {market_data.index.tz}.")
            market_data.index = market_data.index.tz_convert('UTC')
        
        # Фильтрация данных по времени сессии
        # DataFrame.between_time будет использовать start_time_utc и end_time_utc
        # Важно: если start_time_utc > end_time_utc (сессия пересекает полночь), 
        # between_time по умолчанию не будет работать как ожидается. 
        # Нам нужно будет это обработать отдельно.

        start_utc = session_definition.start_time_utc
        end_utc = session_definition.end_time_utc

        session_candles_list = []

        if start_utc <= end_utc: # Сессия не пересекает полночь
            logger.debug(f"Session {session_definition.name} ({start_utc}-{end_utc} UTC) does not cross midnight.")
            # Используем inclusive='both' для совместимости
            session_data = market_data.between_time(start_utc, end_utc, inclusive='both')
            session_candles_list.append(session_data)
        else: # Сессия пересекает полночь (например, 22:00 - 02:00 UTC)
            logger.debug(f"Session {session_definition.name} ({start_utc}-{end_utc} UTC) crosses midnight.")
            # Часть 1: от start_utc до конца дня (23:59:59...)
            part1 = market_data.between_time(start_utc, time(23, 59, 59, 999999), inclusive='both')
            # Часть 2: от начала дня (00:00:00) до end_utc
            part2 = market_data.between_time(time(0, 0, 0), end_utc, inclusive='both')
            session_candles_list.append(part1)
            session_candles_list.append(part2)

        if not session_candles_list:
            logger.warning(f"No data matched session {session_definition.name} for {symbol}.")
            return pd.DataFrame()
            
        result_df = pd.concat(session_candles_list)
        if result_df.empty:
            logger.warning(f"Resulting DataFrame for session {session_definition.name} for {symbol} is empty after concatenation.")
        else:
            logger.info(f"Successfully extracted {len(result_df)} candles for session {session_definition.name} for {symbol}.")
        
        return result_df.sort_index() # Сортируем на всякий случай

    def analyze_session_trend(
        self, 
        session_data: pd.DataFrame,
        session_definition: SessionDefinition # Добавим для логирования
    ) -> Optional[str]:
        """
        Анализирует тренд сессии (например, "bullish", "bearish", "flat").
        Это очень упрощенный пример.

        Args:
            session_data: DataFrame с данными OHLCV только для одной сессии одного дня.
                          Ожидается, что данные уже отфильтрованы для конкретной сессии.
            session_definition: Определение анализируемой сессии (для логирования).

        Returns:
            Строка, описывающая тренд ("bullish", "bearish", "flat"), или None, если данных недостаточно.
        """
        if session_data.empty or len(session_data) < 1:
            logger.warning(f"Not enough data to analyze trend for session {session_definition.name}. Data has {len(session_data)} rows.")
            return None

        # Упрощенный анализ: первая цена открытия и последняя цена закрытия в рамках сессии
        # Убедимся, что DataFrame отсортирован по времени, если это не гарантировано ранее.
        # get_session_data уже сортирует, но для надежности можно добавить session_data = session_data.sort_index()
        
        open_price = session_data['Open'].iloc[0]
        close_price = session_data['Close'].iloc[-1]

        logger.debug(f"Analyzing trend for session {session_definition.name}: Open={open_price}, Close={close_price}")

        if close_price > open_price:
            return "bullish"
        elif close_price < open_price:
            return "bearish"
        else:
            return "flat"

    def get_daily_session_trends(
        self,
        symbol: str,
        session_definition: SessionDefinition,
        start_date_dt: datetime,
        end_date_dt: datetime,
        data_interval: str = "1h"
    ) -> pd.DataFrame:
        """
        Получает данные для сессии и анализирует тренд для каждого дня.

        Returns:
            DataFrame с колонками ['Date', 'SessionName', 'Trend', 'SessionOpen', 'SessionClose', 'SessionHigh', 'SessionLow'].
        """
        all_session_data = self.get_session_data(symbol, session_definition, start_date_dt, end_date_dt, data_interval)
        
        if all_session_data.empty:
            logger.warning(f"No session data to analyze daily trends for {symbol}, session {session_definition.name}.")
            return pd.DataFrame(columns=['Date', 'SessionName', 'Trend', 'SessionOpen', 'SessionClose', 'SessionHigh', 'SessionLow'])

        results = []        
        # Группируем данные по дням. Индекс должен быть DatetimeIndex в UTC.
        for day, daily_data_for_session in all_session_data.groupby(all_session_data.index.date):
            if daily_data_for_session.empty:
                continue
            
            trend = self.analyze_session_trend(daily_data_for_session, session_definition)
            if trend:
                session_open = daily_data_for_session['Open'].iloc[0]
                session_close = daily_data_for_session['Close'].iloc[-1]
                session_high = daily_data_for_session['High'].max()
                session_low = daily_data_for_session['Low'].min()
                results.append({
                    'Date': pd.to_datetime(day), # Преобразуем date обратно в datetime для консистентности, если нужно
                    'SessionName': session_definition.name,
                    'Trend': trend,
                    'SessionOpen': session_open,
                    'SessionClose': session_close,
                    'SessionHigh': session_high,
                    'SessionLow': session_low,
                })
        
        if not results:
            logger.info(f"No daily session trends could be analyzed for {symbol}, session {session_definition.name}.")
            return pd.DataFrame(columns=['Date', 'SessionName', 'Trend', 'SessionOpen', 'SessionClose', 'SessionHigh', 'SessionLow'])

        return pd.DataFrame(results)

# Пример использования (для тестирования):
if __name__ == '__main__':
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir)) # ../src -> ../ 
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from src.core.trading_sessions import SUPPORTED_SESSIONS
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Инициализация
    yahoo_conn = YahooFinanceConnector()
    data_mngr = DataManager(data_source=yahoo_conn)
    session_analyzer = SessionAnalyzer(data_manager=data_mngr)

    # --- Пример для GER40 (DAX: ^GDAXI) и "Азиатской" сессии --- 
    # Важно: "Азиатская сессия" для GER40 - это неформальное понятие.
    # Здесь мы просто берем данные по ^GDAXI (торгуется в Европе) и смотрим, что было в азиатские часы UTC.
    # Это может не отражать реальную ликвидность или специфику торговли GER40 в это время.
    symbol_ger40 = "^GDAXI" 
    # Для GER40 "азиатская сессия" - это обычно пред-торговая активность или торговля фьючерсами.
    # Используем "asia" из SUPPORTED_SESSIONS (00:00-09:00 UTC)
    # Для более точного анализа сессий GER40, нужны данные с меньшим интервалом и точные часы торгов.
    # Например, основные торги Xetra 09:00-17:30 CET. Это 08:00-16:30 UTC (зимой) или 07:00-15:30 UTC (летом)
    
    asia_def = SUPPORTED_SESSIONS.get("asia")
    if not asia_def:
        logger.error("Asia session definition not found.")
        exit()

    start_period = datetime(2023, 12, 1)
    end_period = datetime(2023, 12, 10) # Небольшой период для теста
    
    logger.info(f"\n--- Analysing {symbol_ger40} for {asia_def.name} session ({asia_def.start_time_utc}-{asia_def.end_time_utc} UTC) --- ")
    logger.info(f"Period: {start_period.date()} to {end_period.date()}, Data Interval: 1h")

    # Получение данных только для азиатской сессии
    # asian_session_ger40_data = session_analyzer.get_session_data(
    #     symbol_ger40, 
    #     asia_def, 
    #     start_period, 
    #     end_period, 
    #     data_interval="1h"
    # )
    # if not asian_session_ger40_data.empty:
    #     print(f"\n{asia_def.name} session data for {symbol_ger40}:")
    #     print(asian_session_ger40_data)
    #     # Можно сохранить в CSV для проверки
    #     # asian_session_ger40_data.to_csv(f"{symbol_ger40}_{asia_def.name}_session_data.csv")
    # else:
    #     print(f"\nNo {asia_def.name} session data found for {symbol_ger40} in the period.")

    # Анализ тренда для каждого дня в азиатской сессии
    daily_trends_asia_ger40 = session_analyzer.get_daily_session_trends(
        symbol_ger40,
        asia_def,
        start_period,
        end_period,
        data_interval="1h"
    )

    if not daily_trends_asia_ger40.empty:
        print(f"\nDaily trends for {symbol_ger40} during {asia_def.name} session:")
        print(daily_trends_asia_ger40)
        # Сколько раз сессия была бычьей?
        bullish_sessions = daily_trends_asia_ger40[daily_trends_asia_ger40['Trend'] == 'bullish']
        print(f"Number of bullish '{asia_def.name}' sessions for {symbol_ger40}: {len(bullish_sessions)} out of {len(daily_trends_asia_ger40)} analyzed days.")
    else:
        print(f"\nNo daily trends could be analyzed for {symbol_ger40} during {asia_def.name} session.")

    # --- Пример для MSFT и сессии Нью-Йорка ---
    # symbol_msft = "MSFT"
    # ny_def = SUPPORTED_SESSIONS.get("newyork")
    # if not ny_def:
    #     logger.error("New York session definition not found.")
    #     exit()
    
    # start_period_msft = datetime(2023, 12, 1)
    # end_period_msft = datetime(2023, 12, 5)
    # logger.info(f"\n--- Analysing {symbol_msft} for {ny_def.name} session ({ny_def.start_time_utc}-{ny_def.end_time_utc} UTC) --- ")
    # logger.info(f"Period: {start_period_msft.date()} to {end_period_msft.date()}, Data Interval: 1h")

    # daily_trends_ny_msft = session_analyzer.get_daily_session_trends(
    #     symbol_msft,
    #     ny_def,
    #     start_period_msft,
    #     end_period_msft,
    #     data_interval="1h" # yfinance для 1h требует период < 730 дней
    # )
    # if not daily_trends_ny_msft.empty:
    #     print(f"\nDaily trends for {symbol_msft} during {ny_def.name} session:")
    #     print(daily_trends_ny_msft)
    # else:
    #     print(f"\nNo daily trends could be analyzed for {symbol_msft} during {ny_def.name} session.")

    # --- Пример для Франкфуртской сессии GER40 ---
    # frankfurt_def = SUPPORTED_SESSIONS.get("frankfurt")
    # if not frankfurt_def:
    #     logger.error("Frankfurt session definition not found.")
    #     exit()

    # logger.info(f"\n--- Analysing {symbol_ger40} for {frankfurt_def.name} session ({frankfurt_def.start_time_utc}-{frankfurt_def.end_time_utc} UTC) --- ")
    # logger.info(f"Period: {start_period.date()} to {end_period.date()}, Data Interval: 1h")

    # daily_trends_frankfurt_ger40 = session_analyzer.get_daily_session_trends(
    #     symbol_ger40,
    #     frankfurt_def,
    #     start_period,
    #     end_period,
    #     data_interval="1h"
    # )
    # if not daily_trends_frankfurt_ger40.empty:
    #     print(f"\nDaily trends for {symbol_ger40} during {frankfurt_def.name} session:")
    #     print(daily_trends_frankfurt_ger40)
    # else:
    #     print(f"\nNo daily trends could be analyzed for {symbol_ger40} during {frankfurt_def.name} session.") 