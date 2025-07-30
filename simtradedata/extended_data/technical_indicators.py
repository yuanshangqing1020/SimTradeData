"""
技术指标管理器

负责技术指标的预计算、缓存和自定义指标支持。
"""

# 标准库导入
import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

# 第三方库导入（可选）
try:
    import numpy as np
    import pandas as pd
except ImportError:
    # 如果没有安装numpy和pandas，使用简化版本
    np = None
    pd = None

# 项目内导入
from ..core.base_manager import BaseManager
from ..database import DatabaseManager

logger = logging.getLogger(__name__)


class TechnicalIndicatorManager(BaseManager):
    """技术指标管理器"""

    def __init__(self, db_manager: DatabaseManager = None, config=None, **dependencies):
        """
        初始化技术指标管理器

        Args:
            db_manager: 数据库管理器
            config: 配置对象
            **dependencies: 其他依赖对象
        """
        # 获取数据库管理器 - 在super().__init__前设置
        self.db_manager = db_manager
        if not self.db_manager:
            raise ValueError("数据库管理器不能为空")

        # 内置技术指标 - 在super().__init__前设置
        self.builtin_indicators = {
            "ma": self._calculate_ma,  # 移动平均线
            "ema": self._calculate_ema,  # 指数移动平均线
            "rsi": self._calculate_rsi,  # 相对强弱指数
            "macd": self._calculate_macd,  # MACD
            "bollinger": self._calculate_bollinger,  # 布林带
            "kdj": self._calculate_kdj,  # KDJ
            "atr": self._calculate_atr,  # 平均真实波幅
            "obv": self._calculate_obv,  # 成交量平衡指标
            "cci": self._calculate_cci,  # 商品通道指数
            "williams_r": self._calculate_williams_r,  # 威廉指标
        }

        # 调用BaseManager初始化
        super().__init__(config=config, db_manager=db_manager, **dependencies)

        # 自定义指标
        self.custom_indicators = {}

        # 指标缓存配置
        self.cache_enabled = self.config.get("indicators.cache_enabled", True)
        self.cache_ttl = self.config.get("indicators.cache_ttl", 3600)  # 1小时

        self.logger.info("技术指标管理器初始化完成")

    def _init_components(self):
        """初始化技术指标组件"""
        pass  # 组件初始化在__init__中完成

    def _get_required_attributes(self) -> list:
        """获取必需属性列表"""
        return ["db_manager", "builtin_indicators"]

    def calculate_indicator(
        self,
        symbol: str,
        indicator_name: str,
        params: Dict[str, Any] = None,
        start_date: date = None,
        end_date: date = None,
    ) -> List[Dict[str, Any]]:
        """
        计算技术指标

        Args:
            symbol: 股票代码
            indicator_name: 指标名称
            params: 指标参数
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            List[Dict[str, Any]]: 指标数据
        """
        try:
            if params is None:
                params = {}

            if end_date is None:
                end_date = datetime.now().date()

            if start_date is None:
                start_date = end_date - timedelta(days=365)  # 默认一年

            # 检查缓存
            if self.cache_enabled:
                cached_result = self._get_cached_indicator(
                    symbol, indicator_name, params, start_date, end_date
                )
                if cached_result:
                    return cached_result

            # 获取价格数据
            price_data = self._get_price_data(symbol, start_date, end_date)

            if not price_data:
                logger.warning(f"无法获取价格数据: {symbol}")
                return []

            # 计算指标
            if indicator_name in self.builtin_indicators:
                indicator_func = self.builtin_indicators[indicator_name]
                result = indicator_func(price_data, params)
            elif indicator_name in self.custom_indicators:
                indicator_func = self.custom_indicators[indicator_name]
                result = indicator_func(price_data, params)
            else:
                logger.error(f"未知的技术指标: {indicator_name}")
                return []

            # 缓存结果
            if self.cache_enabled and result:
                self._cache_indicator(
                    symbol, indicator_name, params, start_date, end_date, result
                )

            logger.debug(
                f"技术指标计算完成: {symbol} {indicator_name}, 数据点: {len(result)}"
            )
            return result

        except Exception as e:
            logger.error(f"计算技术指标失败: {e}")
            return []

    def batch_calculate_indicators(
        self,
        symbols: List[str],
        indicator_configs: List[Dict[str, Any]],
        start_date: date = None,
        end_date: date = None,
    ) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """
        批量计算技术指标

        Args:
            symbols: 股票代码列表
            indicator_configs: 指标配置列表 [{'name': 'ma', 'params': {'period': 20}}]
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            Dict[str, Dict[str, List[Dict[str, Any]]]]: 批量指标数据
        """
        try:
            results = {}

            for symbol in symbols:
                results[symbol] = {}

                for config in indicator_configs:
                    indicator_name = config["name"]
                    params = config.get("params", {})

                    indicator_data = self.calculate_indicator(
                        symbol, indicator_name, params, start_date, end_date
                    )
                    results[symbol][indicator_name] = indicator_data

            logger.info(
                f"批量指标计算完成: {len(symbols)} 只股票, {len(indicator_configs)} 个指标"
            )
            return results

        except Exception as e:
            logger.error(f"批量计算技术指标失败: {e}")
            return {}

    def register_custom_indicator(
        self, name: str, func: Callable, description: str = ""
    ) -> bool:
        """
        注册自定义指标

        Args:
            name: 指标名称
            func: 指标计算函数
            description: 指标描述

        Returns:
            bool: 是否注册成功
        """
        try:
            self.custom_indicators[name] = func

            # 保存到数据库
            sql = """
            INSERT OR REPLACE INTO ptrade_custom_indicators 
            (indicator_name, description, created_time, last_update)
            VALUES (?, ?, ?, ?)
            """

            self.db_manager.execute(
                sql,
                (
                    name,
                    description,
                    datetime.now().isoformat(),
                    datetime.now().isoformat(),
                ),
            )

            logger.info(f"自定义指标注册成功: {name}")
            return True

        except Exception as e:
            logger.error(f"注册自定义指标失败: {e}")
            return False

    def get_available_indicators(self) -> Dict[str, List[str]]:
        """
        获取可用指标列表

        Returns:
            Dict[str, List[str]]: 可用指标
        """
        return {
            "builtin": list(self.builtin_indicators.keys()),
            "custom": list(self.custom_indicators.keys()),
        }

    def _get_price_data(
        self, symbol: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """获取价格数据"""
        try:
            sql = """
            SELECT trade_date, open, high, low, close, volume, money
            FROM market_data 
            WHERE symbol = ? AND trade_date >= ? AND trade_date <= ? 
            AND frequency = '1d'
            ORDER BY trade_date
            """

            results = self.db_manager.fetchall(
                sql, (symbol, str(start_date), str(end_date))
            )

            if not results:
                return pd.DataFrame()

            # 转换为DataFrame
            df = pd.DataFrame([dict(row) for row in results])
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df.set_index("trade_date", inplace=True)

            # 确保数据类型
            for col in ["open", "high", "low", "close", "volume", "money"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            return df

        except Exception as e:
            logger.error(f"获取价格数据失败: {e}")
            return pd.DataFrame()

    def _calculate_ma(
        self, data: pd.DataFrame, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """计算移动平均线"""
        period = params.get("period", 20)
        price_col = params.get("price", "close")

        if price_col not in data.columns:
            return []

        ma_values = data[price_col].rolling(window=period).mean()

        result = []
        for date, value in ma_values.items():
            if not pd.isna(value):
                result.append(
                    {
                        "trade_date": date.strftime("%Y-%m-%d"),
                        "indicator": "ma",
                        "value": round(float(value), 4),
                        "params": params,
                    }
                )

        return result

    def _calculate_ema(
        self, data: pd.DataFrame, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """计算指数移动平均线"""
        period = params.get("period", 20)
        price_col = params.get("price", "close")

        if price_col not in data.columns:
            return []

        ema_values = data[price_col].ewm(span=period).mean()

        result = []
        for date, value in ema_values.items():
            if not pd.isna(value):
                result.append(
                    {
                        "trade_date": date.strftime("%Y-%m-%d"),
                        "indicator": "ema",
                        "value": round(float(value), 4),
                        "params": params,
                    }
                )

        return result

    def _calculate_rsi(
        self, data: pd.DataFrame, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """计算RSI"""
        period = params.get("period", 14)
        price_col = params.get("price", "close")

        if price_col not in data.columns:
            return []

        delta = data[price_col].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        result = []
        for date, value in rsi.items():
            if not pd.isna(value):
                result.append(
                    {
                        "trade_date": date.strftime("%Y-%m-%d"),
                        "indicator": "rsi",
                        "value": round(float(value), 4),
                        "params": params,
                    }
                )

        return result

    def _calculate_macd(
        self, data: pd.DataFrame, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """计算MACD"""
        fast_period = params.get("fast_period", 12)
        slow_period = params.get("slow_period", 26)
        signal_period = params.get("signal_period", 9)
        price_col = params.get("price", "close")

        if price_col not in data.columns:
            return []

        ema_fast = data[price_col].ewm(span=fast_period).mean()
        ema_slow = data[price_col].ewm(span=slow_period).mean()

        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal_period).mean()
        histogram = macd_line - signal_line

        result = []
        for i, date in enumerate(data.index):
            if not pd.isna(macd_line.iloc[i]):
                result.append(
                    {
                        "trade_date": date.strftime("%Y-%m-%d"),
                        "indicator": "macd",
                        "macd": round(float(macd_line.iloc[i]), 4),
                        "signal": (
                            round(float(signal_line.iloc[i]), 4)
                            if not pd.isna(signal_line.iloc[i])
                            else None
                        ),
                        "histogram": (
                            round(float(histogram.iloc[i]), 4)
                            if not pd.isna(histogram.iloc[i])
                            else None
                        ),
                        "params": params,
                    }
                )

        return result

    def _calculate_bollinger(
        self, data: pd.DataFrame, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """计算布林带"""
        period = params.get("period", 20)
        std_dev = params.get("std_dev", 2)
        price_col = params.get("price", "close")

        if price_col not in data.columns:
            return []

        sma = data[price_col].rolling(window=period).mean()
        std = data[price_col].rolling(window=period).std()

        upper_band = sma + (std * std_dev)
        lower_band = sma - (std * std_dev)

        result = []
        for i, date in enumerate(data.index):
            if not pd.isna(sma.iloc[i]):
                result.append(
                    {
                        "trade_date": date.strftime("%Y-%m-%d"),
                        "indicator": "bollinger",
                        "middle": round(float(sma.iloc[i]), 4),
                        "upper": round(float(upper_band.iloc[i]), 4),
                        "lower": round(float(lower_band.iloc[i]), 4),
                        "params": params,
                    }
                )

        return result

    def _calculate_kdj(
        self, data: pd.DataFrame, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """计算KDJ"""
        period = params.get("period", 9)
        k_period = params.get("k_period", 3)
        d_period = params.get("d_period", 3)

        required_cols = ["high", "low", "close"]
        if not all(col in data.columns for col in required_cols):
            return []

        lowest_low = data["low"].rolling(window=period).min()
        highest_high = data["high"].rolling(window=period).max()

        rsv = (data["close"] - lowest_low) / (highest_high - lowest_low) * 100

        k_values = rsv.ewm(alpha=1 / k_period).mean()
        d_values = k_values.ewm(alpha=1 / d_period).mean()
        j_values = 3 * k_values - 2 * d_values

        result = []
        for i, date in enumerate(data.index):
            if not pd.isna(k_values.iloc[i]):
                result.append(
                    {
                        "trade_date": date.strftime("%Y-%m-%d"),
                        "indicator": "kdj",
                        "k": round(float(k_values.iloc[i]), 4),
                        "d": round(float(d_values.iloc[i]), 4),
                        "j": round(float(j_values.iloc[i]), 4),
                        "params": params,
                    }
                )

        return result

    def _calculate_atr(
        self, data: pd.DataFrame, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """计算平均真实波幅"""
        period = params.get("period", 14)

        required_cols = ["high", "low", "close"]
        if not all(col in data.columns for col in required_cols):
            return []

        high_low = data["high"] - data["low"]
        high_close_prev = np.abs(data["high"] - data["close"].shift(1))
        low_close_prev = np.abs(data["low"] - data["close"].shift(1))

        true_range = np.maximum(high_low, np.maximum(high_close_prev, low_close_prev))
        atr = true_range.rolling(window=period).mean()

        result = []
        for date, value in atr.items():
            if not pd.isna(value):
                result.append(
                    {
                        "trade_date": date.strftime("%Y-%m-%d"),
                        "indicator": "atr",
                        "value": round(float(value), 4),
                        "params": params,
                    }
                )

        return result

    def _calculate_obv(
        self, data: pd.DataFrame, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """计算成交量平衡指标"""
        required_cols = ["close", "volume"]
        if not all(col in data.columns for col in required_cols):
            return []

        price_change = data["close"].diff()
        volume_direction = np.where(
            price_change > 0,
            data["volume"],
            np.where(price_change < 0, -data["volume"], 0),
        )

        obv = volume_direction.cumsum()

        result = []
        for date, value in obv.items():
            if not pd.isna(value):
                result.append(
                    {
                        "trade_date": date.strftime("%Y-%m-%d"),
                        "indicator": "obv",
                        "value": round(float(value), 0),
                        "params": params,
                    }
                )

        return result

    def _calculate_cci(
        self, data: pd.DataFrame, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """计算商品通道指数"""
        period = params.get("period", 20)

        required_cols = ["high", "low", "close"]
        if not all(col in data.columns for col in required_cols):
            return []

        typical_price = (data["high"] + data["low"] + data["close"]) / 3
        sma = typical_price.rolling(window=period).mean()
        mean_deviation = typical_price.rolling(window=period).apply(
            lambda x: np.mean(np.abs(x - x.mean()))
        )

        cci = (typical_price - sma) / (0.015 * mean_deviation)

        result = []
        for date, value in cci.items():
            if not pd.isna(value):
                result.append(
                    {
                        "trade_date": date.strftime("%Y-%m-%d"),
                        "indicator": "cci",
                        "value": round(float(value), 4),
                        "params": params,
                    }
                )

        return result

    def _calculate_williams_r(
        self, data: pd.DataFrame, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """计算威廉指标"""
        period = params.get("period", 14)

        required_cols = ["high", "low", "close"]
        if not all(col in data.columns for col in required_cols):
            return []

        highest_high = data["high"].rolling(window=period).max()
        lowest_low = data["low"].rolling(window=period).min()

        williams_r = (highest_high - data["close"]) / (highest_high - lowest_low) * -100

        result = []
        for date, value in williams_r.items():
            if not pd.isna(value):
                result.append(
                    {
                        "trade_date": date.strftime("%Y-%m-%d"),
                        "indicator": "williams_r",
                        "value": round(float(value), 4),
                        "params": params,
                    }
                )

        return result

    def _get_cached_indicator(
        self,
        symbol: str,
        indicator_name: str,
        params: Dict[str, Any],
        start_date: date,
        end_date: date,
    ) -> Optional[List[Dict[str, Any]]]:
        """获取缓存的指标数据"""
        try:
            cache_key = self._generate_cache_key(
                symbol, indicator_name, params, start_date, end_date
            )

            sql = """
            SELECT indicator_data, cache_time FROM ptrade_indicator_cache 
            WHERE cache_key = ? AND cache_time > ?
            """

            cutoff_time = datetime.now() - timedelta(seconds=self.cache_ttl)
            result = self.db_manager.fetchone(sql, (cache_key, cutoff_time.isoformat()))

            if result:
                return json.loads(result["indicator_data"])

            return None

        except Exception as e:
            logger.error(f"获取缓存指标失败: {e}")
            return None

    def _cache_indicator(
        self,
        symbol: str,
        indicator_name: str,
        params: Dict[str, Any],
        start_date: date,
        end_date: date,
        data: List[Dict[str, Any]],
    ):
        """缓存指标数据"""
        try:
            cache_key = self._generate_cache_key(
                symbol, indicator_name, params, start_date, end_date
            )

            sql = """
            INSERT OR REPLACE INTO ptrade_indicator_cache 
            (cache_key, symbol, indicator_name, indicator_data, cache_time)
            VALUES (?, ?, ?, ?, ?)
            """

            self.db_manager.execute(
                sql,
                (
                    cache_key,
                    symbol,
                    indicator_name,
                    json.dumps(data),
                    datetime.now().isoformat(),
                ),
            )

        except Exception as e:
            logger.error(f"缓存指标数据失败: {e}")

    def _generate_cache_key(
        self,
        symbol: str,
        indicator_name: str,
        params: Dict[str, Any],
        start_date: date,
        end_date: date,
    ) -> str:
        """生成缓存键"""
        import hashlib

        key_data = f"{symbol}_{indicator_name}_{json.dumps(params, sort_keys=True)}_{start_date}_{end_date}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def clear_cache(self, symbol: str = None, indicator_name: str = None):
        """清空指标缓存"""
        try:
            conditions = []
            params = []

            if symbol:
                conditions.append("symbol = ?")
                params.append(symbol)

            if indicator_name:
                conditions.append("indicator_name = ?")
                params.append(indicator_name)

            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

            sql = f"DELETE FROM ptrade_indicator_cache {where_clause}"
            self.db_manager.execute(sql, params)

            logger.info(
                f"指标缓存清空完成: symbol={symbol}, indicator={indicator_name}"
            )

        except Exception as e:
            logger.error(f"清空指标缓存失败: {e}")

    def get_manager_stats(self) -> Dict[str, Any]:
        """获取管理器统计信息"""
        try:
            # 缓存统计
            cache_stats_sql = """
            SELECT COUNT(*) as total_cache, 
                   COUNT(DISTINCT symbol) as cached_symbols,
                   COUNT(DISTINCT indicator_name) as cached_indicators
            FROM ptrade_indicator_cache
            """
            cache_stats = self.db_manager.fetchone(cache_stats_sql)

            return {
                "builtin_indicators": list(self.builtin_indicators.keys()),
                "custom_indicators": list(self.custom_indicators.keys()),
                "cache_enabled": self.cache_enabled,
                "cache_ttl": self.cache_ttl,
                "cache_stats": dict(cache_stats) if cache_stats else {},
                "supported_features": [
                    "内置技术指标计算",
                    "自定义指标支持",
                    "批量指标计算",
                    "指标缓存优化",
                    "多参数指标配置",
                ],
            }

        except Exception as e:
            logger.error(f"获取管理器统计失败: {e}")
            return {}
