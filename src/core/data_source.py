from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime

class DataSource(ABC):
    """
    Абстрактный базовый класс для источников данных.
    Определяет интерфейс для загрузки исторических данных.
    """

    @abstractmethod
    def fetch_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d"  # e.g., "1m", "5m", "1h", "1d", "1wk"
    ) -> pd.DataFrame:
        """
        Загружает исторические данные (OHLCV) для указанного символа и периода.

        Args:
            symbol: Тикер инструмента (например, "AAPL", "EURUSD=X", "GER40.DE").
            start_date: Начальная дата периода.
            end_date: Конечная дата периода.
            interval: Временной интервал свечей (например, "1d" для дневных, "1h" для часовых).
                      Формат может зависеть от конкретного API источника.

        Returns:
            pandas.DataFrame с колонками [Open, High, Low, Close, Volume, (Datetime index)].
            В случае ошибки или отсутствия данных должен возвращать пустой DataFrame.
        """
        pass

    @abstractmethod
    def get_available_symbols(self) -> list[str]:
        """
        Возвращает список доступных символов для данного источника данных.
        Этот метод может быть не реализован для всех источников.
        """
        pass

    @abstractmethod
    def get_info(self, symbol: str) -> dict:
        """
        Возвращает информацию об инструменте (например, название компании, сектор, индустрия).
        Этот метод может быть не реализован для всех источников.
        """
        pass 