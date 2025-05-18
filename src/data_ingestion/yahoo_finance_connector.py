import yfinance as yf
import pandas as pd
from datetime import datetime
from src.core.data_source import DataSource
import logging

logger = logging.getLogger(__name__)

class YahooFinanceConnector(DataSource):
    """
    Коннектор для загрузки данных с Yahoo Finance.
    """

    def fetch_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d"
    ) -> pd.DataFrame:
        """
        Загружает исторические данные (OHLCV) для указанного символа и периода с Yahoo Finance.

        Args:
            symbol: Тикер инструмента (например, "AAPL", "EURUSD=X", "^GDAXI" для GER40).
            start_date: Начальная дата периода (объект datetime).
            end_date: Конечная дата периода (объект datetime).
            interval: Временной интервал свечей (например, "1d", "1h", "1wk", "1mo").
                      Поддерживаемые интервалы yfinance: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 
                      1d, 5d, 1wk, 1mo, 3mo.
                      Для интервалов меньше 1 дня, данные ограничены последними 60 днями.
                      Для интервала '1m' данные доступны только за последние 7 дней и только для некоторых тикеров.

        Returns:
            pandas.DataFrame с колонками [Open, High, Low, Close, Volume] и DatetimeIndex.
            В случае ошибки или отсутствия данных возвращает пустой DataFrame.
        """
        try:
            ticker = yf.Ticker(symbol)
            # yfinance ожидает даты в формате 'YYYY-MM-DD'
            data = ticker.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                interval=interval
            )
            if data.empty:
                logger.warning(f"No data found for symbol {symbol} from {start_date} to {end_date} with interval {interval}.")
                return pd.DataFrame()
            
            # yfinance может возвращать дивиденды и сплиты, нам нужны только OHLCV
            # Также проверим, что необходимые колонки существуют
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in data.columns for col in required_columns):
                logger.error(f"Missing required columns for symbol {symbol}. Available columns: {data.columns.tolist()}")
                return pd.DataFrame()
                
            return data[required_columns]
        except Exception as e:
            logger.error(f"Error fetching data for symbol {symbol} from Yahoo Finance: {e}")
            return pd.DataFrame()

    def get_info(self, symbol: str) -> dict:
        """
        Возвращает информацию об инструменте с Yahoo Finance.

        Args:
            symbol: Тикер инструмента.

        Returns:
            Словарь с информацией об инструменте или пустой словарь в случае ошибки.
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            if not info or (isinstance(info, dict) and info.get('regularMarketPrice') is None and info.get('previousClose') is None):
                # ticker.info может вернуть {'regularMarketPrice': None, 'preMarketPrice': None, ...} для невалидных тикеров
                # или если нет данных. Проверяем наличие ключевых полей.
                logger.warning(f"Could not retrieve valid info for symbol {symbol} from Yahoo Finance. It might be an invalid ticker.")
                return {}
            return info
        except Exception as e:
            logger.error(f"Error fetching info for symbol {symbol} from Yahoo Finance: {e}")
            return {}

    def get_available_symbols(self) -> list[str]:
        """
        Yahoo Finance не предоставляет прямого способа получить список всех доступных символов через yfinance.
        Этот метод не реализован.

        Returns:
            NotImplementedError
        """
        logger.warning("get_available_symbols is not directly supported by yfinance.")
        raise NotImplementedError("Yahoo Finance does not provide a direct API to list all available symbols through yfinance.")

# Пример использования (можно раскомментировать для быстрой проверки):
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    connector = YahooFinanceConnector()

    # Пример 1: Загрузка дневных данных для AAPL
    symbol_to_test = "AAPL"
    start = datetime(2023, 1, 1)
    end = datetime(2023, 12, 31)
    data_aapl = connector.fetch_data(symbol_to_test, start, end, interval="1d")
    if not data_aapl.empty:
        print(f"\nData for {symbol_to_test} ({start.date()} to {end.date()}):")
        print(data_aapl.head())
        print(data_aapl.tail())
    else:
        print(f"\nNo data retrieved for {symbol_to_test}.")

    # Пример 2: Загрузка часовых данных для EUR/USD (для интервалов < 1 дня, yfinance имеет ограничения по периоду)
    symbol_to_test = "EURUSD=X"
    start = datetime(2023, 12, 1)
    end = datetime(2023, 12, 31)
    data_eurusd_1h = connector.fetch_data(symbol_to_test, start, end, interval="1h") # Ограничение по датам для часовиков!
    if not data_eurusd_1h.empty:
        print(f"\nHourly data for {symbol_to_test} ({start.date()} to {end.date()}):")
        print(data_eurusd_1h.head())
    else:
        print(f"\nNo hourly data retrieved for {symbol_to_test}.")

  # Пример 3: Загрузка данных для GER40 (DAX) - тикер ^GDAXI
  symbol_ger40 = "^GDAXI"
  start_ger40 = datetime(2023, 1, 1)
  end_ger40 = datetime(2023, 12, 31)
  data_ger40 = connector.fetch_data(symbol_ger40, start_ger40, end_ger40, interval="1d")
  if not data_ger40.empty:
      print(f"\nData for {symbol_ger40} ({start_ger40.date()} to {end_ger40.date()}):")
      print(data_ger40.head())
  else:
      print(f"\nNo data retrieved for {symbol_ger40}.")

  # Пример 4: Получение информации об инструменте
  info_aapl = connector.get_info("AAPL")
  if info_aapl:
      print(f"\nInfo for AAPL:")
      # print(info_aapl) # Распечатает всю информацию
      print(f"  Name: {info_aapl.get('shortName', 'N/A')}")
      print(f"  Sector: {info_aapl.get('sector', 'N/A')}")
      print(f"  Industry: {info_aapl.get('industry', 'N/A')}")
  else:
      print("\nNo info retrieved for AAPL.")

  info_invalid = connector.get_info("INVALIDTICKERXYZ123")
  if not info_invalid:
      print("\nCorrectly handled invalid ticker for get_info.")

  # Пример 5: Попытка получить список доступных символов (вызовет NotImplementedError)
  try:
      connector.get_available_symbols()
  except NotImplementedError as e:
      print(f"\nSuccessfully caught: {e}")

  # Пример с GER40, который ты упоминал, для азиатской сессии
  # Обрати внимание, что yfinance вернет дневные свечи. 
  # Анализ конкретных сессий (азиатская, европейская, американская) на основе дневных свечей потребует дополнительной логики
  # или данных с более мелким таймфреймом и точными временными метками сессий.
  # Для примера, как в твоем запросе про GER40, тикер на Yahoo Finance обычно ^GDAXI
  symbol_ger40_example = "^GDAXI" # Тикер для DAX (GER40)
  start_example = datetime(2023, 1, 1)
  end_example = datetime(2023, 12, 31)
    
    print(f"\nFetching data for {symbol_ger40_example} to demonstrate part of your initial query...")
    data_ger40_example = connector.fetch_data(symbol_ger40_example, start_example, end_example, interval="1d")
    if not data_ger40_example.empty:
        print(f"Successfully fetched {len(data_ger40_example)} days of data for {symbol_ger40_example}.")
        print(data_ger40_example.head())
        # Дальнейший анализ (например, определение какая свеча относится к азиатской сессии
        # и была ли она бычьей) потребует дополнительной логики и, возможно, данных о времени открытия/закрытия сессий.
    else:
        print(f"Failed to fetch data for {symbol_ger40_example}.") 