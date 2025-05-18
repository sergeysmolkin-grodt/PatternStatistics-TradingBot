import pandas as pd
from datetime import datetime, time, date
import pytz # Добавим pytz для работы с часовыми поясами
from typing import Optional, Dict, Any

from src.core.trading_sessions import SessionDefinition, get_utc_session_boundaries_for_date
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
        session_def: SessionDefinition,
        start_date_dt: datetime, # Это datetime, но для цикла будем использовать date часть
        end_date_dt: datetime,   # Аналогично
        data_interval: str = "1h"
    ) -> pd.DataFrame:
        """
        Извлекает данные для указанной торговой сессии за определенный период,
        корректно обрабатывая DST для сессий, определенных в локальных часовых поясах.
        """
        logger.info(f"Fetching raw data for {symbol} from {start_date_dt.date()} to {end_date_dt.date()} with interval {data_interval}.")
        market_data = self.data_manager.get_data(symbol, start_date_dt, end_date_dt, interval=data_interval)

        if market_data.empty:
            logger.warning(f"No market data found for {symbol} to analyze session {session_def.name}.")
            return pd.DataFrame()

        if market_data.index.tz is None:
            logger.debug(f"Localizing naive DatetimeIndex for {symbol} to UTC.")
            market_data.index = market_data.index.tz_localize('UTC', ambiguous='infer')
        elif market_data.index.tz != pytz.UTC:
            logger.debug(f"Converting DatetimeIndex for {symbol} to UTC from {market_data.index.tz}.")
            market_data.index = market_data.index.tz_convert(pytz.UTC)
        
        # Если сессия определена напрямую в UTC, старая логика с between_time может быть эффективнее
        # Но для универсальности и точности с DST, будем итерировать по дням и использовать datetime границы.
        # Это может быть менее производительно для очень больших диапазонов и UTC-определенных сессий.
        if session_def.exchange_timezone == "UTC":
            # Можно использовать старую, более быструю логику для UTC-сессий, если производительность станет проблемой.
            # Пока оставим универсальный подход.
            logger.info(f"Session {session_def.name} is defined in UTC. Using daily boundary calculation for consistency.")

        all_session_candles = []
        current_date = start_date_dt.date()
        while current_date <= end_date_dt.date():
            try:
                # Получаем UTC границы для текущей даты
                session_start_utc, session_end_utc = get_utc_session_boundaries_for_date(session_def, current_date)
                logger.debug(f"For date {current_date}, session '{session_def.name}' UTC boundaries: {session_start_utc} - {session_end_utc}")
                
                # Фильтруем данные для текущего дня по UTC datetime границам
                # DataFrame.loc будет включать обе границы, если они точно совпадают с индексом.
                # Если нужны строгие границы (>) и (<), нужно будет настроить.
                # Для свечей, обычно включаем начальную и конечную свечу сессии.
                # A[ (A.index >= S) & (A.index <= E) ]
                daily_market_data = market_data[
                    (market_data.index >= session_start_utc) & 
                    (market_data.index <= session_end_utc)
                ]
                
                if not daily_market_data.empty:
                    all_session_candles.append(daily_market_data)
                    logger.debug(f"  Found {len(daily_market_data)} candles for {session_def.name} on {current_date}.")
                else:
                    logger.debug(f"  No candles found for {session_def.name} on {current_date} within calculated UTC boundaries.")
            
            except pytz.exceptions.AmbiguousTimeError as ate:
                logger.warning(f"Ambiguous time for session {session_def.name} on {current_date} due to DST: {ate}. Skipping day.")
            except pytz.exceptions.NonExistentTimeError as nete:
                logger.warning(f"Non-existent time for session {session_def.name} on {current_date} due to DST: {nete}. Skipping day.")
            except Exception as e:
                logger.error(f"Error processing session {session_def.name} for {current_date}: {e}. Skipping day.")
            
            current_date += pd.Timedelta(days=1)

        if not all_session_candles:
            logger.warning(f"No data matched session {session_def.name} for {symbol} in the entire period.")
            return pd.DataFrame()
            
        result_df = pd.concat(all_session_candles)
        if result_df.empty:
            logger.warning(f"Resulting DataFrame for session {session_def.name} for {symbol} is empty after concatenation.")
        else:
            logger.info(f"Successfully extracted {len(result_df)} total candles for session {session_def.name} for {symbol}.")
        
        return result_df.sort_index()

    def analyze_session_details(
        self, 
        session_data: pd.DataFrame,
        session_definition: SessionDefinition 
    ) -> Optional[Dict[str, Any]]:
        """
        Анализирует детали сессии: тренд, количество бычьих/медвежьих свечей, общий объем.

        Args:
            session_data: DataFrame с данными OHLCV только для одной сессии одного дня.
            session_definition: Определение анализируемой сессии (для логирования).

        Returns:
            Словарь с деталями сессии или None, если данных недостаточно.
            Пример словаря: {
                'trend': 'bullish', 
                'bullish_candles': 5, 
                'bearish_candles': 3, 
                'neutral_candles': 1, 
                'total_volume': 1000000,
                'session_open': 150.00,
                'session_close': 151.00,
                'session_high': 151.50,
                'session_low': 149.50
            }
        """
        if session_data.empty or len(session_data) < 1:
            logger.warning(f"Not enough data to analyze details for session {session_definition.name}. Data has {len(session_data)} rows.")
            return None

        # Тренд по Open/Close сессии
        open_price = session_data['Open'].iloc[0]
        close_price = session_data['Close'].iloc[-1]
        high_price = session_data['High'].max()
        low_price = session_data['Low'].min()
        
        trend = "flat"
        if close_price > open_price:
            trend = "bullish"
        elif close_price < open_price:
            trend = "bearish"

        # Подсчет типов свечей
        bullish_candles = len(session_data[session_data['Close'] > session_data['Open']])
        bearish_candles = len(session_data[session_data['Close'] < session_data['Open']])
        neutral_candles = len(session_data[session_data['Close'] == session_data['Open']])

        # Общий объем
        total_volume = session_data['Volume'].sum()

        logger.debug(
            f"Session {session_definition.name} details: Trend={trend}, O={open_price}, C={close_price}, H={high_price}, L={low_price}, "
            f"Bullish={bullish_candles}, Bearish={bearish_candles}, Neutral={neutral_candles}, Volume={total_volume}"
        )

        return {
            'trend': trend,
            'session_open': open_price,
            'session_close': close_price,
            'session_high': high_price,
            'session_low': low_price,
            'bullish_candles': bullish_candles,
            'bearish_candles': bearish_candles,
            'neutral_candles': neutral_candles,
            'total_volume': total_volume
        }

    def get_daily_session_analysis(
        self,
        symbol: str,
        session_definition: SessionDefinition,
        start_date_dt: datetime,
        end_date_dt: datetime,
        data_interval: str = "1h"
    ) -> pd.DataFrame:
        """
        Получает данные для сессии и анализирует ее характеристики для каждого дня.

        Returns:
            DataFrame с колонками, включающими 'Date', 'SessionName', 'Trend', 
            'SessionOpen', 'SessionClose', 'SessionHigh', 'SessionLow',
            'BullishCandles', 'BearishCandles', 'NeutralCandles', 'TotalVolume'.
        """
        all_session_data = self.get_session_data(symbol, session_definition, start_date_dt, end_date_dt, data_interval)
        
        if all_session_data.empty:
            logger.warning(f"No session data to analyze daily characteristics for {symbol}, session {session_definition.name}.")
            return pd.DataFrame()

        results = []
        output_columns = [
            'Date', 'SessionName', 'Trend', 'SessionOpen', 'SessionClose', 'SessionHigh', 'SessionLow',
            'BullishCandles', 'BearishCandles', 'NeutralCandles', 'TotalVolume'
        ]
        
        for day_date, daily_data_for_session in all_session_data.groupby(all_session_data.index.date):
            if daily_data_for_session.empty:
                continue
            
            analysis_results = self.analyze_session_details(daily_data_for_session, session_definition)
            if analysis_results:
                results.append({
                    'Date': pd.to_datetime(day_date), 
                    'SessionName': session_definition.name,
                    'Trend': analysis_results['trend'],
                    'SessionOpen': analysis_results['session_open'],
                    'SessionClose': analysis_results['session_close'],
                    'SessionHigh': analysis_results['session_high'],
                    'SessionLow': analysis_results['session_low'],
                    'BullishCandles': analysis_results['bullish_candles'],
                    'BearishCandles': analysis_results['bearish_candles'],
                    'NeutralCandles': analysis_results['neutral_candles'],
                    'TotalVolume': analysis_results['total_volume']
                })
        
        if not results:
            logger.info(f"No daily session analysis could be performed for {symbol}, session {session_definition.name}.")
            return pd.DataFrame(columns=output_columns)

        return pd.DataFrame(results, columns=output_columns)

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

    # --- Пример для GER40 (DAX: ^GDAXI) с использованием Xetra сессии --- 
    symbol_target = "^GDAXI"
    session_def_target = SUPPORTED_SESSIONS.get("frankfurt_xetra")
    if not session_def_target:
        logger.error(f"Session definition for Xetra not found.")
        exit()

    # Тест перехода на летнее время (март 2025)
    # DST в Европе обычно: конец марта - конец октября.
    # Для 2025 в Europe/Berlin: DST начнется 30 марта 2025.
    start_dst_transition_test_period = datetime(2025, 3, 25) # Вторник, до перехода
    end_dst_transition_test_period = datetime(2025, 4, 5)   # Суббота, после перехода
    
    logger.info(f"\n--- Analysing {symbol_target} for {session_def_target.name} session ({session_def_target.local_start_time}-{session_def_target.local_end_time} {session_def_target.exchange_timezone}) --- ")
    logger.info(f"Period (DST Start Test): {start_dst_transition_test_period.date()} to {end_dst_transition_test_period.date()}, Data Interval: 1h")

    daily_analysis_results = session_analyzer.get_daily_session_analysis(
        symbol_target,
        session_def_target,
        start_dst_transition_test_period,
        end_dst_transition_test_period,
        data_interval="1h"
    )

    if not daily_analysis_results.empty:
        print(f"\nDaily session analysis for {symbol_target} during {session_def_target.name} session (around DST start):")
        print(daily_analysis_results)
        # Пример дополнительной статистики на основе новых колонок
        avg_bullish_candles = daily_analysis_results['BullishCandles'].mean()
        avg_bearish_candles = daily_analysis_results['BearishCandles'].mean()
        avg_total_volume = daily_analysis_results['TotalVolume'].mean()
        print(f"Avg Bullish Candles per session: {avg_bullish_candles:.2f}")
        print(f"Avg Bearish Candles per session: {avg_bearish_candles:.2f}")
        print(f"Avg Total Volume per session: {avg_total_volume:,.0f}")
        
        # Подсчет количества "лонговых" сессий на основе нового определения (например, больше бычьих свечей чем медвежьих)
        # Это можно вынести в отдельную функцию или делать по месту
        daily_analysis_results['IsLongByCandleCount'] = daily_analysis_results['BullishCandles'] > daily_analysis_results['BearishCandles']
        num_long_by_candles = daily_analysis_results['IsLongByCandleCount'].sum()
        print(f"Number of sessions considered 'long' by candle count: {num_long_by_candles} out of {len(daily_analysis_results)} analyzed days.")

    else:
        print(f"\nNo daily session analysis could be performed for {symbol_target} during {session_def_target.name} session.")

    # Тест перехода на зимнее время (октябрь/ноябрь 2024)
    # Для 2024 в Europe/Berlin: DST закончится 27 октября 2024.
    start_winter_transition_test_period = datetime(2024, 10, 22) # Вторник, до перехода
    end_winter_transition_test_period = datetime(2024, 11, 2)   # Суббота, после перехода
    logger.info(f"\n--- Analysing {symbol_target} for {session_def_target.name} (around DST end) --- ")
    logger.info(f"Period (DST End Test): {start_winter_transition_test_period.date()} to {end_winter_transition_test_period.date()}, Data Interval: 1h")

    daily_analysis_results = session_analyzer.get_daily_session_analysis(
        symbol_target,
        session_def_target,
        start_winter_transition_test_period,
        end_winter_transition_test_period,
        data_interval="1h"
    )
    if not daily_analysis_results.empty:
        print(f"\nDaily session analysis for {symbol_target} during {session_def_target.name} session (around DST end):")
        print(daily_analysis_results)
        # Пример дополнительной статистики на основе новых колонок
        avg_bullish_candles = daily_analysis_results['BullishCandles'].mean()
        avg_bearish_candles = daily_analysis_results['BearishCandles'].mean()
        avg_total_volume = daily_analysis_results['TotalVolume'].mean()
        print(f"Avg Bullish Candles per session: {avg_bullish_candles:.2f}")
        print(f"Avg Bearish Candles per session: {avg_bearish_candles:.2f}")
        print(f"Avg Total Volume per session: {avg_total_volume:,.0f}")
        
        # Подсчет количества "лонговых" сессий на основе нового определения (например, больше бычьих свечей чем медвежьих)
        # Это можно вынести в отдельную функцию или делать по месту
        daily_analysis_results['IsLongByCandleCount'] = daily_analysis_results['BullishCandles'] > daily_analysis_results['BearishCandles']
        num_long_by_candles = daily_analysis_results['IsLongByCandleCount'].sum()
        print(f"Number of sessions considered 'long' by candle count: {num_long_by_candles} out of {len(daily_analysis_results)} analyzed days.")
    else:
        print(f"\nNo daily session analysis could be performed for {symbol_target} during {session_def_target.name} session.")


    # --- Проверка с "Asia Generic UTC" (должна работать как раньше, но через новый механизм) ---
    # asia_def_generic = SUPPORTED_SESSIONS.get("asia_generic_utc")
    # if not asia_def_generic:
    #     logger.error("Asia Generic UTC session definition not found.")
    #     exit()
    # start_period_asia = datetime(2023, 12, 1)
    # end_period_asia = datetime(2023, 12, 5) 
    # logger.info(f"\n--- Analysing {symbol_target} for {asia_def_generic.name} session --- ")
    # daily_trends_asia_generic = session_analyzer.get_daily_session_trends(
    #     symbol_target,
    #     asia_def_generic,
    #     start_period_asia,
    #     end_period_asia,
    #     data_interval="1h"
    # )
    # if not daily_trends_asia_generic.empty:
    #     print(f"\nDaily trends for {symbol_target} during {asia_def_generic.name} session:")
    #     print(daily_trends_asia_generic)
    # else:
    #     print(f"\nNo daily trends could be analyzed for {symbol_target} during {asia_def_generic.name} session.") 