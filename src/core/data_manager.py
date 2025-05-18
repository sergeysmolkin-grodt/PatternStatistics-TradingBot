import pandas as pd
from datetime import datetime, date
import os
import hashlib
import logging
from typing import Optional

from src.core.data_source import DataSource

logger = logging.getLogger(__name__)

DEFAULT_CACHE_DIR = "data/cache"

class DataManager:
    """
    Отвечает за управление получением и кэшированием данных.
    Использует DataSource для получения данных из внешних источников
    и сохраняет их в локальном кэше (Parquet файлы) для ускорения доступа.
    """

    def __init__(self, data_source: DataSource, cache_dir: str = DEFAULT_CACHE_DIR):
        """
        Инициализирует DataManager.

        Args:
            data_source: Экземпляр класса, реализующего DataSource (например, YahooFinanceConnector).
            cache_dir: Директория для хранения кэшированных файлов.
        """
        self.data_source = data_source
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True) # Создаем директорию кэша, если ее нет

    def _generate_cache_filename(self, symbol: str, start_date: datetime, end_date: datetime, interval: str) -> str:
        """
        Генерирует имя файла для кэша на основе параметров запроса.
        Использует хэш для уникальности и сокращения длины имени.
        """
        # Нормализуем даты до строк, чтобы хэш был консистентным
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        # Создаем уникальную строку для хэширования
        unique_string = f"{symbol}_{start_str}_{end_str}_{interval}"
        # Используем SHA256 для хэширования, берем первые 16 символов для компактности
        hash_suffix = hashlib.sha256(unique_string.encode()).hexdigest()[:16]
        
        filename = f"{symbol}_{interval}_{hash_suffix}.parquet"
        return os.path.join(self.cache_dir, filename)

    def get_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
        force_refresh: bool = False # Флаг для принудительной загрузки без использования кэша
    ) -> pd.DataFrame:
        """
        Получает исторические данные. Сначала пытается загрузить из кэша.
        Если в кэше нет или force_refresh=True, загружает через DataSource и сохраняет в кэш.

        Args:
            symbol: Тикер инструмента.
            start_date: Начальная дата.
            end_date: Конечная дата.
            interval: Интервал свечей.
            force_refresh: Если True, данные будут загружены из источника, даже если есть в кэше.

        Returns:
            pandas.DataFrame с данными или пустой DataFrame в случае ошибки.
        """
        cache_filepath = self._generate_cache_filename(symbol, start_date, end_date, interval)

        if not force_refresh and os.path.exists(cache_filepath):
            try:
                # Проверяем "свежесть" кэша для дневных данных
                # Если данные дневные и файл кэша создан не сегодня, и конечная дата >= сегодня, то стоит обновить
                # Это очень упрощенная проверка, можно усложнить (например, проверять время последней торговой сессии)
                if interval == "1d":
                    file_mod_date = date.fromtimestamp(os.path.getmtime(cache_filepath))
                    # Если конечная дата включает сегодняшний или будущий день, и файл не сегодняшний, 
                    # то данные могут быть неполными за сегодня.
                    if end_date.date() >= date.today() and file_mod_date < date.today():
                        logger.info(f"Cache file {cache_filepath} for interval '1d' might be stale for today. Refreshing.")
                        return self._fetch_and_cache(symbol, start_date, end_date, interval, cache_filepath)
                
                logger.info(f"Loading data for {symbol} from cache: {cache_filepath}")
                return pd.read_parquet(cache_filepath)
            except Exception as e:
                logger.warning(f"Error reading from cache file {cache_filepath}: {e}. Fetching from source.")
                # Если ошибка чтения кэша, пробуем загрузить заново
                return self._fetch_and_cache(symbol, start_date, end_date, interval, cache_filepath)
        
        # Если кэша нет или force_refresh=True
        return self._fetch_and_cache(symbol, start_date, end_date, interval, cache_filepath)

    def _fetch_and_cache(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str,
        cache_filepath: str
    ) -> pd.DataFrame:
        """
        Вспомогательный метод: загружает данные из DataSource и сохраняет их в кэш.
        """
        logger.info(f"Fetching data for {symbol} from {self.data_source.__class__.__name__}.")
        data = self.data_source.fetch_data(symbol, start_date, end_date, interval)

        if not data.empty:
            try:
                data.to_parquet(cache_filepath)
                logger.info(f"Data for {symbol} cached to {cache_filepath}")
            except Exception as e:
                logger.error(f"Error saving data to cache file {cache_filepath}: {e}")
        else:
            logger.warning(f"No data fetched for {symbol}, not caching.")
        
        return data

    def get_info(self, symbol: str, force_refresh: bool = False) -> Optional[dict]:
        """
        Получает информацию об инструменте. Кэширование информации об инструменте (например, в JSON файл) 
        также может быть реализовано по аналогии с get_data, если это необходимо.
        Пока что просто проксирует вызов к data_source.
        
        Args:
            symbol: Тикер инструмента.
            force_refresh: Если True, информация будет запрошена из источника заново 
                           (полезно, если информация могла обновиться).
                           Текущая реализация не использует кэш для get_info.
        Returns:
            Словарь с информацией или None.
        """
        # TODO: Реализовать кэширование для информации об инструменте, если актуально.
        #       Можно использовать JSON-файлы, например.
        #       Имя файла можно генерировать: f"{symbol}_info.json"
        logger.info(f"Fetching info for {symbol} directly from {self.data_source.__class__.__name__} (caching not implemented for info yet).")
        try:
            info = self.data_source.get_info(symbol)
            return info if info else None
        except Exception as e:
            logger.error(f"Error fetching info for {symbol} via data_source: {e}")
            return None

# Пример использования (можно добавить в конец файла для тестирования)
if __name__ == '__main__':
    import sys
    # Добавляем корень проекта в sys.path, чтобы можно было импортировать src
    # Это нужно только если запускать этот файл напрямую для теста
    # При запуске через `python -m src.core.data_manager` это не требуется
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir)) # src -> project_root
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from src.data_ingestion.yahoo_finance_connector import YahooFinanceConnector # Импорт здесь, после модификации sys.path

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # Инициализация коннектора и менеджера данных
    yahoo_connector = YahooFinanceConnector()
    data_manager = DataManager(data_source=yahoo_connector)

    # Параметры для запроса данных
    symbol_test = "MSFT"
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime(2023, 6, 30)
    interval_test = "1d"

    # 1. Первая загрузка (данных в кэше нет)
    print(f"\n1. First fetch for {symbol_test} (should be from source):")
    df_msft1 = data_manager.get_data(symbol_test, start_dt, end_dt, interval_test)
    if not df_msft1.empty:
        print(f"Successfully fetched {len(df_msft1)} rows for {symbol_test}.")
        print(df_msft1.head())
    else:
        print(f"Failed to fetch data for {symbol_test}.")

    # 2. Вторая загрузка (данные должны загрузиться из кэша)
    print(f"\n2. Second fetch for {symbol_test} (should be from cache):")
    df_msft2 = data_manager.get_data(symbol_test, start_dt, end_dt, interval_test)
    if not df_msft2.empty:
        print(f"Successfully fetched {len(df_msft2)} rows for {symbol_test} (from cache).")
        print(df_msft2.head())
    else:
        print(f"Failed to fetch data for {symbol_test} (from cache).")

    # 3. Загрузка с force_refresh (данные должны загрузиться из источника, кэш обновится)
    print(f"\n3. Fetch for {symbol_test} with force_refresh=True (should be from source):")
    df_msft3 = data_manager.get_data(symbol_test, start_dt, end_dt, interval_test, force_refresh=True)
    if not df_msft3.empty:
        print(f"Successfully fetched {len(df_msft3)} rows for {symbol_test} (forced refresh).")
        print(df_msft3.head())
    else:
        print(f"Failed to fetch data for {symbol_test} (forced refresh).")

    # 4. Пример с другой датой (новый файл кэша)
    start_dt_new = datetime(2023, 7, 1)
    end_dt_new = datetime(2023, 12, 31)
    print(f"\n4. Fetch for {symbol_test} with new date range (should be from source, new cache file):")
    df_msft4 = data_manager.get_data(symbol_test, start_dt_new, end_dt_new, interval_test)
    if not df_msft4.empty:
        print(f"Successfully fetched {len(df_msft4)} rows for {symbol_test} (new date range).")
        print(df_msft4.head())
    else:
        print(f"Failed to fetch data for {symbol_test} (new date range).")

    # 5. Получение информации об инструменте (без кэширования пока)
    print(f"\n5. Fetching info for {symbol_test}:")
    info_msft = data_manager.get_info(symbol_test)
    if info_msft:
        print(f"Info for {symbol_test}: Name - {info_msft.get('shortName', 'N/A')}, Sector - {info_msft.get('sector', 'N/A')}")
    else:
        print(f"Failed to fetch info for {symbol_test}.")

    # Проверка "свежести" кэша для данных, включающих сегодня
    # Чтобы этот тест был показательным, конечная дата должна быть сегодняшней или будущей
    # end_dt_today = datetime.now() # Можно использовать datetime.now() или конкретную будущую дату
    # start_dt_recent = datetime(end_dt_today.year, end_dt_today.month, 1 if end_dt_today.day > 1 else end_dt_today.day) 
    # if end_dt_today.day == 1 and end_dt_today.month == 1:
    #     start_dt_recent = datetime(end_dt_today.year-1, 12, 1) # Корректировка для начала года
    # elif end_dt_today.day == 1:
    #    # Предыдущий месяц, если сегодня 1-е число
    #    prev_month_end = end_dt_today.replace(day=1) - timedelta(days=1)
    #    start_dt_recent = prev_month_end.replace(day=1)
    # else:
    #    start_dt_recent = end_dt_today.replace(day=1)
    
    # print(f"\n6. Testing cache freshness for {symbol_test} up to today:")
    # # Сначала загружаем (если кэша нет или он не сегодняшний)
    # data_manager.get_data(symbol_test, start_dt_recent, end_dt_today, interval_test)
    # # Затем сразу пытаемся загрузить снова. Если кэш был создан сегодня, должен использоваться он.
    # # Если он был создан вчера, а end_date включает сегодня, он должен был обновиться.
    # data_manager.get_data(symbol_test, start_dt_recent, end_dt_today, interval_test) 