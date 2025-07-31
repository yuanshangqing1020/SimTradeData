"""
技术指标计算器

负责计算各种技术指标，如移动平均线、MACD、RSI等。
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd

from ..config import Config
from ..database import DatabaseManager

logger = logging.getLogger(__name__)


class TechnicalIndicators:
    """技术指标计算器"""

    def __init__(self, config: Config = None):
        """
        初始化技术指标计算器

        Args:
            config: 配置对象
        """
        self.config = config or Config()

        # 指标配置 - 从技术指标配置中读取
        from ..config.defaults import get_technical_indicators_config

        indicators_config = get_technical_indicators_config()

        self.ma_periods = indicators_config.get("ma", {}).get(
            "periods", [5, 10, 20, 60]
        )
        self.enable_ma = indicators_config.get("ma", {}).get("enabled", True)
        self.enable_ema = indicators_config.get("ema", {}).get("enabled", False)
        self.enable_macd = indicators_config.get("macd", {}).get("enabled", False)
        self.enable_rsi = indicators_config.get("rsi", {}).get("enabled", False)
        self.enable_kdj = indicators_config.get("kdj", {}).get("enabled", False)

        # 计算所需的历史数据天数
        self.history_days = max(self.ma_periods) + 10 if self.ma_periods else 70

        logger.info("技术指标计算器初始化完成")

    def calculate_indicators(
        self,
        ptrade_data: Dict[str, Any],
        symbol: str,
        db_manager: DatabaseManager = None,
    ) -> Dict[str, Any]:
        """
        计算技术指标

        Args:
            ptrade_data: PTrade格式数据
            symbol: 股票代码
            db_manager: 数据库管理器 (用于获取历史数据)

        Returns:
            Dict[str, Any]: 包含技术指标的数据
        """
        if not ptrade_data or not self.enable_ma:
            return ptrade_data

        try:
            logger.debug(f"开始计算技术指标: {symbol}")

            # 获取历史数据用于计算指标
            historical_data = self._get_historical_data(
                symbol, ptrade_data.get("date"), db_manager
            )

            if not historical_data:
                logger.debug(f"无足够历史数据计算指标: {symbol}")
                return ptrade_data

            # 添加当前数据到历史数据
            current_data = {
                "date": ptrade_data.get("date"),
                "close": ptrade_data.get("close", 0),
                "high": ptrade_data.get("high", 0),
                "low": ptrade_data.get("low", 0),
                "volume": ptrade_data.get("volume", 0),
            }
            historical_data.append(current_data)

            # 转换为DataFrame
            df = pd.DataFrame(historical_data)
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df = df.dropna()

            if len(df) < 5:  # 简化：只需要5天数据
                logger.debug(f"历史数据不足，无法计算指标: {symbol}, 数据量: {len(df)}")
                return ptrade_data

            # 计算各种指标
            indicators = {}

            if self.enable_ma:
                ma_indicators = self._calculate_moving_averages(df)
                indicators.update(ma_indicators)

            if self.enable_ema:
                ema_indicators = self._calculate_ema(df)
                indicators.update(ema_indicators)

            if self.enable_macd:
                macd_indicators = self._calculate_macd(df)
                indicators.update(macd_indicators)

            if self.enable_rsi:
                rsi_indicators = self._calculate_rsi(df)
                indicators.update(rsi_indicators)

            if self.enable_kdj:
                kdj_indicators = self._calculate_kdj(df)
                indicators.update(kdj_indicators)

            # 将指标添加到PTrade数据中
            ptrade_data.update(indicators)

            logger.debug(f"技术指标计算完成: {symbol}, 指标数量: {len(indicators)}")
            return ptrade_data

        except Exception as e:
            logger.error(f"技术指标计算失败 {symbol}: {e}")
            return ptrade_data

    def _get_historical_data(
        self, symbol: str, current_date: str, db_manager: DatabaseManager = None
    ) -> List[Dict[str, Any]]:
        """获取历史数据"""
        if not db_manager:
            return []

        try:
            # 计算开始日期
            current_dt = datetime.strptime(current_date, "%Y-%m-%d")
            start_date = current_dt - timedelta(days=self.history_days)

            sql = """
            SELECT date, close, high, low, volume
            FROM market_data 
            WHERE symbol = ? AND frequency = '1d' 
            AND date >= ? AND date < ?
            ORDER BY date
            """

            results = db_manager.fetchall(
                sql, (symbol, start_date.strftime("%Y-%m-%d"), current_date)
            )

            historical_data = []
            for row in results:
                historical_data.append(
                    {
                        "date": row["date"],
                        "close": float(row["close"]) if row["close"] else 0,
                        "high": float(row["high"]) if row["high"] else 0,
                        "low": float(row["low"]) if row["low"] else 0,
                        "volume": float(row["volume"]) if row["volume"] else 0,
                    }
                )

            logger.debug(f"获取历史数据: {symbol}, 数量: {len(historical_data)}")
            return historical_data

        except Exception as e:
            logger.error(f"获取历史数据失败 {symbol}: {e}")
            return []

    def _calculate_moving_averages(self, df: pd.DataFrame) -> Dict[str, float]:
        """计算移动平均线"""
        try:
            ma_indicators = {}

            for period in self.ma_periods:
                if len(df) >= period:
                    ma_values = df["close"].rolling(window=period).mean()
                    latest_ma = ma_values.iloc[-1]

                    if not pd.isna(latest_ma):
                        ma_indicators[f"ma{period}"] = round(float(latest_ma), 3)
                    else:
                        ma_indicators[f"ma{period}"] = 0.0
                else:
                    ma_indicators[f"ma{period}"] = 0.0

            return ma_indicators

        except Exception as e:
            logger.error(f"移动平均线计算失败: {e}")
            return {}

    def _calculate_ema(self, df: pd.DataFrame) -> Dict[str, float]:
        """计算指数移动平均线"""
        try:
            ema_indicators = {}
            ema_periods = [12, 26]

            for period in ema_periods:
                if len(df) >= period:
                    ema_values = df["close"].ewm(span=period).mean()
                    latest_ema = ema_values.iloc[-1]

                    if not pd.isna(latest_ema):
                        ema_indicators[f"ema{period}"] = round(float(latest_ema), 3)
                    else:
                        ema_indicators[f"ema{period}"] = 0.0
                else:
                    ema_indicators[f"ema{period}"] = 0.0

            return ema_indicators

        except Exception as e:
            logger.error(f"指数移动平均线计算失败: {e}")
            return {}

    def _calculate_macd(self, df: pd.DataFrame) -> Dict[str, float]:
        """计算MACD指标"""
        try:
            if len(df) < 26:
                return {"macd": 0.0, "macd_signal": 0.0, "macd_hist": 0.0}

            # 计算EMA12和EMA26
            ema12 = df["close"].ewm(span=12).mean()
            ema26 = df["close"].ewm(span=26).mean()

            # 计算MACD线
            macd_line = ema12 - ema26

            # 计算信号线 (MACD的9日EMA)
            signal_line = macd_line.ewm(span=9).mean()

            # 计算MACD柱状图
            macd_histogram = macd_line - signal_line

            return {
                "macd_dif": round(float(macd_line.iloc[-1]), 4),
                "macd_dea": round(float(signal_line.iloc[-1]), 4),
                "macd_histogram": round(float(macd_histogram.iloc[-1]), 4),
            }

        except Exception as e:
            logger.error(f"MACD计算失败: {e}")
            return {"macd_dif": 0.0, "macd_dea": 0.0, "macd_histogram": 0.0}

    def _calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> Dict[str, float]:
        """计算RSI指标"""
        try:
            if len(df) < period + 1:
                return {"rsi_12": 50.0}  # 使用数据库字段名

            # 计算价格变化
            price_changes = df["close"].diff()

            # 分离上涨和下跌
            gains = price_changes.where(price_changes > 0, 0)
            losses = -price_changes.where(price_changes < 0, 0)

            # 计算平均收益和平均损失
            avg_gains = gains.rolling(window=period).mean()
            avg_losses = losses.rolling(window=period).mean()

            # 计算RS和RSI
            rs = avg_gains / avg_losses
            rsi = 100 - (100 / (1 + rs))

            latest_rsi = rsi.iloc[-1]

            if pd.isna(latest_rsi):
                return {"rsi_12": 50.0}

            return {"rsi_12": round(float(latest_rsi), 2)}

        except Exception as e:
            logger.error(f"RSI计算失败: {e}")
            return {"rsi_12": 50.0}

    def _calculate_bollinger_bands(
        self, df: pd.DataFrame, period: int = 20, std_dev: float = 2.0
    ) -> Dict[str, float]:
        """计算布林带指标"""
        try:
            if len(df) < period:
                return {"bb_upper": 0.0, "bb_middle": 0.0, "bb_lower": 0.0}

            # 计算中轨 (移动平均线)
            middle_band = df["close"].rolling(window=period).mean()

            # 计算标准差
            std = df["close"].rolling(window=period).std()

            # 计算上轨和下轨
            upper_band = middle_band + (std * std_dev)
            lower_band = middle_band - (std * std_dev)

            return {
                "bb_upper": round(float(upper_band.iloc[-1]), 3),
                "bb_middle": round(float(middle_band.iloc[-1]), 3),
                "bb_lower": round(float(lower_band.iloc[-1]), 3),
            }

        except Exception as e:
            logger.error(f"布林带计算失败: {e}")
            return {"bb_upper": 0.0, "bb_middle": 0.0, "bb_lower": 0.0}

    def _calculate_kdj(
        self, df: pd.DataFrame, k_period: int = 9, d_period: int = 3, j_period: int = 3
    ) -> Dict[str, float]:
        """计算KDJ指标"""
        try:
            if len(df) < k_period:
                return {"kdj_k": 50.0, "kdj_d": 50.0, "kdj_j": 50.0}

            # 计算最高价和最低价的滚动窗口
            high_max = df["high"].rolling(window=k_period).max()
            low_min = df["low"].rolling(window=k_period).min()

            # 计算RSV
            rsv = (df["close"] - low_min) / (high_max - low_min) * 100

            # 计算K值 (RSV的移动平均)
            k_values = rsv.ewm(alpha=1 / d_period).mean()

            # 计算D值 (K值的移动平均)
            d_values = k_values.ewm(alpha=1 / d_period).mean()

            # 计算J值
            j_values = 3 * k_values - 2 * d_values

            return {
                "kdj_k": round(float(k_values.iloc[-1]), 2),
                "kdj_d": round(float(d_values.iloc[-1]), 2),
                "kdj_j": round(float(j_values.iloc[-1]), 2),
            }

        except Exception as e:
            logger.error(f"KDJ计算失败: {e}")
            return {"kdj_k": 50.0, "kdj_d": 50.0, "kdj_j": 50.0}

    def calculate_batch_indicators(
        self, symbols: List[str], target_date: str, db_manager: DatabaseManager
    ) -> Dict[str, Dict[str, float]]:
        """批量计算技术指标"""
        try:
            results = {}

            for symbol in symbols:
                # 获取当日数据
                sql = """
                SELECT * FROM market_data 
                WHERE symbol = ? AND trade_date = ? AND frequency = '1d'
                """
                current_data = db_manager.fetchone(sql, (symbol, target_date))

                if current_data:
                    ptrade_data = dict(current_data)
                    indicators_data = self.calculate_indicators(
                        ptrade_data, symbol, db_manager
                    )

                    # 提取指标
                    indicators = {}
                    for key, value in indicators_data.items():
                        if key.startswith(("ma", "ema", "macd", "rsi", "bb_", "kdj_")):
                            indicators[key] = value

                    results[symbol] = indicators

            return results

        except Exception as e:
            logger.error(f"批量计算技术指标失败: {e}")
            return {}

    def get_indicator_config(self) -> Dict[str, Any]:
        """获取指标配置信息"""
        return {
            "ma_periods": self.ma_periods,
            "enable_ma": self.enable_ma,
            "enable_ema": self.enable_ema,
            "enable_macd": self.enable_macd,
            "enable_rsi": self.enable_rsi,
            "history_days": self.history_days,
        }
