"""
美股市场适配器

负责美股特有的数据处理、字段映射和交易时间管理。
"""

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional

import pytz

from ..config import Config
from ..core import BaseManager
from ..database import DatabaseManager

logger = logging.getLogger(__name__)


class USMarketAdapter(BaseManager):
    """美股市场适配器"""

    def __init__(self, db_manager: DatabaseManager, config: Config = None, **kwargs):
        """
        初始化美股市场适配器

        Args:
            db_manager: 数据库管理器
            config: 配置对象
        """
        # 设置数据库管理器依赖
        self.db_manager = db_manager
        if not self.db_manager:
            raise ValueError("数据库管理器不能为空")

        # 调用BaseManager初始化
        super().__init__(config=config, db_manager=db_manager, **kwargs)

    def _init_specific_config(self):
        """初始化美股适配器特定配置"""
        # 美股市场配置
        self.market_code = self._get_config("market_code", "US")
        self.currency = self._get_config("currency", "USD")
        self.timezone_name = self._get_config("timezone", "America/New_York")
        self.timezone = pytz.timezone(self.timezone_name)

    def _init_components(self):
        """初始化美股适配器组件"""
        # 美股交易时间 (东部时间)
        self.trading_sessions = {
            "premarket": {"start": time(4, 0), "end": time(9, 30)},  # 盘前交易
            "regular": {"start": time(9, 30), "end": time(16, 0)},  # 常规交易
            "afterhours": {"start": time(16, 0), "end": time(20, 0)},  # 盘后交易
        }

        # 美股交易所映射
        self.exchanges = {
            "NASDAQ": "NASDAQ",
            "NYSE": "NYSE",
            "AMEX": "AMEX",
            "BATS": "BATS",
            "IEX": "IEX",
        }

        # 美股股票类型
        self.stock_types = {
            "common": "普通股",
            "preferred": "优先股",
            "etf": "ETF",
            "etn": "ETN",
            "reit": "REITs",
            "adr": "ADR",
            "warrant": "权证",
            "unit": "单位",
        }

        # 美股特有字段映射
        self.field_mapping = {
            "shares_outstanding": "shares_outstanding",  # 流通股本
            "market_cap": "market_cap",  # 市值
            "beta": "beta",  # 贝塔系数
            "dividend_yield": "dividend_yield",  # 股息率
            "ex_dividend_date": "ex_dividend_date",  # 除息日
            "earnings_date": "earnings_date",  # 财报日期
            "forward_pe": "forward_pe",  # 预期市盈率
            "peg_ratio": "peg_ratio",  # PEG比率
        }

        logger.info("美股市场适配器初始化完成")

    def _get_required_attributes(self) -> List[str]:
        """必需属性列表"""
        return [
            "db_manager",
            "trading_sessions",
            "exchanges",
            "stock_types",
            "field_mapping",
        ]

    def adapt_stock_info(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        适配美股股票信息

        Args:
            raw_data: 原始股票信息数据

        Returns:
            Dict[str, Any]: 适配后的股票信息
        """
        try:
            adapted_data = {
                "symbol": self._normalize_symbol(raw_data.get("symbol", "")),
                "name": raw_data.get("name", ""),
                "market": self.market_code,
                "exchange": self._map_exchange(raw_data.get("exchange", "")),
                "currency": self.currency,
                "timezone": str(self.timezone),
                "trading_hours": self._format_trading_hours(),
                "status": self._map_stock_status(raw_data.get("status", "")),
                "stock_type": self._map_stock_type(raw_data.get("type", "")),
                "industry": raw_data.get("industry", ""),
                "sector": raw_data.get("sector", ""),
                "list_date": self._parse_date(raw_data.get("list_date")),
                "delist_date": self._parse_date(raw_data.get("delist_date")),
                "total_share": self._parse_number(raw_data.get("total_share")),
                "float_share": self._parse_number(raw_data.get("float_share")),
            }

            # 美股特有字段
            us_specific = {
                "shares_outstanding": self._parse_number(
                    raw_data.get("shares_outstanding")
                ),
                "market_cap": self._parse_number(raw_data.get("market_cap")),
                "beta": self._parse_number(raw_data.get("beta")),
                "dividend_yield": self._parse_number(raw_data.get("dividend_yield")),
                "ex_dividend_date": self._parse_date(raw_data.get("ex_dividend_date")),
                "earnings_date": self._parse_date(raw_data.get("earnings_date")),
                "forward_pe": self._parse_number(raw_data.get("forward_pe")),
                "peg_ratio": self._parse_number(raw_data.get("peg_ratio")),
                "country": raw_data.get("country", "US"),
                "description": raw_data.get("description", ""),
                "website": raw_data.get("website", ""),
                "ceo": raw_data.get("ceo", ""),
                "employees": self._parse_number(raw_data.get("employees")),
            }

            adapted_data.update(us_specific)

            logger.debug(f"美股股票信息适配完成: {adapted_data['symbol']}")
            return adapted_data

        except Exception as e:
            logger.error(f"美股股票信息适配失败: {e}")
            return raw_data

    def adapt_price_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        适配美股价格数据

        Args:
            raw_data: 原始价格数据

        Returns:
            Dict[str, Any]: 适配后的价格数据
        """
        try:
            adapted_data = {
                "symbol": self._normalize_symbol(raw_data.get("symbol", "")),
                "market": self.market_code,
                "trade_date": raw_data.get("trade_date", ""),
                "trade_time": raw_data.get("trade_time", ""),
                "frequency": raw_data.get("frequency", "1d"),
                "currency": self.currency,
                "timezone": str(self.timezone),
                # 基础价格数据
                "open": self._parse_price(raw_data.get("open")),
                "high": self._parse_price(raw_data.get("high")),
                "low": self._parse_price(raw_data.get("low")),
                "close": self._parse_price(raw_data.get("close")),
                "volume": self._parse_number(raw_data.get("volume")),
                "money": self._parse_number(raw_data.get("money")),
                "preclose": self._parse_price(raw_data.get("preclose")),
                # 美股特有字段
                "adj_close": self._parse_price(raw_data.get("adj_close")),  # 复权收盘价
                "split_factor": self._parse_number(
                    raw_data.get("split_factor", 1.0)
                ),  # 拆股因子
                "dividend": self._parse_number(raw_data.get("dividend", 0.0)),  # 股息
                # 扩展交易数据
                "premarket_open": self._parse_price(raw_data.get("premarket_open")),
                "premarket_close": self._parse_price(raw_data.get("premarket_close")),
                "premarket_volume": self._parse_number(
                    raw_data.get("premarket_volume")
                ),
                "afterhours_open": self._parse_price(raw_data.get("afterhours_open")),
                "afterhours_close": self._parse_price(raw_data.get("afterhours_close")),
                "afterhours_volume": self._parse_number(
                    raw_data.get("afterhours_volume")
                ),
                # 计算字段
                "change": 0.0,
                "change_percent": 0.0,
                "amplitude": 0.0,
                "turnover_rate": 0.0,
            }

            # 计算涨跌额和涨跌幅
            if adapted_data["close"] and adapted_data["preclose"]:
                adapted_data["change"] = (
                    adapted_data["close"] - adapted_data["preclose"]
                )
                if adapted_data["preclose"] != 0:
                    adapted_data["change_percent"] = (
                        adapted_data["change"] / adapted_data["preclose"]
                    ) * 100

            # 计算振幅
            if (
                adapted_data["high"]
                and adapted_data["low"]
                and adapted_data["preclose"]
            ):
                if adapted_data["preclose"] != 0:
                    adapted_data["amplitude"] = (
                        (adapted_data["high"] - adapted_data["low"])
                        / adapted_data["preclose"]
                    ) * 100

            # 美股没有涨跌停限制
            adapted_data["high_limit"] = None
            adapted_data["low_limit"] = None
            adapted_data["unlimited"] = True

            # 计算成交额 (如果没有提供)
            if (
                not adapted_data["money"]
                and adapted_data["volume"]
                and adapted_data["close"]
            ):
                adapted_data["money"] = adapted_data["volume"] * adapted_data["close"]

            logger.debug(f"美股价格数据适配完成: {adapted_data['symbol']}")
            return adapted_data

        except Exception as e:
            logger.error(f"美股价格数据适配失败: {e}")
            return raw_data

    def get_trading_calendar(
        self, start_date: date, end_date: date
    ) -> List[Dict[str, Any]]:
        """
        获取美股交易日历

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            List[Dict[str, Any]]: 交易日历数据
        """
        try:
            calendar_data = []
            current_date = start_date

            while current_date <= end_date:
                is_trading = self._is_trading_day(current_date)

                calendar_data.append(
                    {
                        "trade_date": current_date.strftime("%Y-%m-%d"),
                        "market": self.market_code,
                        "is_trading": int(is_trading),
                        "is_weekend": int(current_date.weekday() >= 5),
                        "is_holiday": int(self._is_holiday(current_date)),
                        "trading_sessions": (
                            self.trading_sessions if is_trading else None
                        ),
                        "dst_active": int(self._is_dst_active(current_date)),  # 夏令时
                    }
                )

                current_date += timedelta(days=1)

            logger.info(
                f"美股交易日历生成完成: {start_date} 到 {end_date}, 共 {len(calendar_data)} 天"
            )
            return calendar_data

        except Exception as e:
            logger.error(f"美股交易日历生成失败: {e}")
            return []

    def _normalize_symbol(self, symbol: str) -> str:
        """标准化美股代码"""
        if not symbol:
            return ""

        # 移除空格并转换为大写
        symbol = symbol.strip().upper()

        # 美股代码格式：字母 + .US
        if symbol.isalpha():
            return f"{symbol}.US"
        elif symbol.endswith(".US"):
            return symbol
        elif "." not in symbol and symbol.isalpha():
            return f"{symbol}.US"

        return symbol

    def _parse_price(self, value: Any) -> Optional[float]:
        """解析价格数据"""
        if value is None or value == "":
            return None

        try:
            price = float(value)
            return round(price, 4)  # 美股价格精度到4位小数
        except (ValueError, TypeError):
            return None

    def _parse_number(self, value: Any) -> Optional[float]:
        """解析数值数据"""
        if value is None or value == "":
            return None

        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_date(self, value: Any) -> Optional[str]:
        """解析日期数据"""
        if value is None or value == "":
            return None

        try:
            if isinstance(value, str):
                # 尝试解析不同的日期格式
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"]:
                    try:
                        dt = datetime.strptime(value, fmt)
                        return dt.strftime("%Y-%m-%d")
                    except ValueError:
                        continue
            elif isinstance(value, (date, datetime)):
                return value.strftime("%Y-%m-%d")

            return str(value)
        except Exception:
            return None

    def _map_exchange(self, exchange: str) -> str:
        """映射交易所"""
        exchange = exchange.upper()
        return self.exchanges.get(exchange, exchange)

    def _map_stock_status(self, status: str) -> str:
        """映射股票状态"""
        status_mapping = {
            "ACTIVE": "active",
            "INACTIVE": "inactive",
            "DELISTED": "delisted",
            "SUSPENDED": "suspended",
        }

        return status_mapping.get(status.upper(), "active")

    def _map_stock_type(self, stock_type: str) -> str:
        """映射股票类型"""
        type_mapping = {
            "CS": "common",  # Common Stock
            "PS": "preferred",  # Preferred Stock
            "ET": "etf",  # ETF
            "EN": "etn",  # ETN
            "RT": "reit",  # REIT
            "AD": "adr",  # ADR
            "WR": "warrant",  # Warrant
            "UN": "unit",  # Unit
        }

        return type_mapping.get(stock_type.upper(), "common")

    def _format_trading_hours(self) -> str:
        """格式化交易时间"""
        regular = self.trading_sessions["regular"]
        return f"{regular['start'].strftime('%H:%M')}-{regular['end'].strftime('%H:%M')} ET"

    def _is_trading_day(self, target_date: date) -> bool:
        """判断是否为交易日"""
        # 周末不交易
        if target_date.weekday() >= 5:
            return False

        # 检查是否为节假日
        if self._is_holiday(target_date):
            return False

        return True

    def _is_holiday(self, target_date: date) -> bool:
        """判断是否为美股节假日"""
        year = target_date.year

        # 美股主要节假日
        holidays = [
            # 元旦
            date(year, 1, 1),
            # 马丁·路德·金纪念日 (1月第三个周一)
            self._get_nth_weekday(year, 1, 0, 3),
            # 总统日 (2月第三个周一)
            self._get_nth_weekday(year, 2, 0, 3),
            # 耶稣受难日 (复活节前的周五，需要动态计算)
            # 阵亡将士纪念日 (5月最后一个周一)
            self._get_last_weekday(year, 5, 0),
            # 独立日
            date(year, 7, 4),
            # 劳动节 (9月第一个周一)
            self._get_nth_weekday(year, 9, 0, 1),
            # 感恩节 (11月第四个周四)
            self._get_nth_weekday(year, 11, 3, 4),
            # 圣诞节
            date(year, 12, 25),
        ]

        # 如果节假日在周末，通常会调休
        adjusted_holidays = []
        for holiday in holidays:
            if holiday.weekday() == 5:  # 周六
                adjusted_holidays.append(holiday - timedelta(days=1))  # 周五
            elif holiday.weekday() == 6:  # 周日
                adjusted_holidays.append(holiday + timedelta(days=1))  # 周一
            else:
                adjusted_holidays.append(holiday)

        return target_date in adjusted_holidays

    def _get_nth_weekday(self, year: int, month: int, weekday: int, n: int) -> date:
        """获取某月第n个指定星期几"""
        first_day = date(year, month, 1)
        first_weekday = first_day.weekday()

        # 计算第一个指定星期几的日期
        days_ahead = weekday - first_weekday
        if days_ahead < 0:
            days_ahead += 7

        first_target = first_day + timedelta(days=days_ahead)
        return first_target + timedelta(weeks=n - 1)

    def _get_last_weekday(self, year: int, month: int, weekday: int) -> date:
        """获取某月最后一个指定星期几"""
        # 下个月第一天
        if month == 12:
            next_month = date(year + 1, 1, 1)
        else:
            next_month = date(year, month + 1, 1)

        # 上个月最后一天
        last_day = next_month - timedelta(days=1)

        # 向前找到最后一个指定星期几
        days_back = (last_day.weekday() - weekday) % 7
        return last_day - timedelta(days=days_back)

    def _is_dst_active(self, target_date: date) -> bool:
        """判断是否为夏令时"""
        # 美国夏令时：3月第二个周日到11月第一个周日
        year = target_date.year

        # 夏令时开始：3月第二个周日
        dst_start = self._get_nth_weekday(year, 3, 6, 2)

        # 夏令时结束：11月第一个周日
        dst_end = self._get_nth_weekday(year, 11, 6, 1)

        return dst_start <= target_date < dst_end

    def get_market_info(self) -> Dict[str, Any]:
        """获取美股市场信息"""
        return {
            "market_code": self.market_code,
            "market_name": "美国股票市场",
            "market_name_en": "United States Stock Market",
            "exchanges": list(self.exchanges.values()),
            "currency": self.currency,
            "timezone": str(self.timezone),
            "trading_hours": self._format_trading_hours(),
            "trading_sessions": self.trading_sessions,
            "price_precision": 4,
            "min_price_change": 0.0001,
            "has_price_limit": False,
            "supports_premarket": True,
            "supports_afterhours": True,
            "supported_frequencies": ["1m", "5m", "15m", "30m", "60m", "1d"],
            "stock_types": list(self.stock_types.keys()),
        }
