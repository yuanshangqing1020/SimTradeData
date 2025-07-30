"""
港股市场适配器

负责港股特有的数据处理、字段映射和交易日历管理。
"""

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional

import pytz

from ..config import Config
from ..core import BaseManager
from ..database import DatabaseManager

logger = logging.getLogger(__name__)


class HKMarketAdapter(BaseManager):
    """港股市场适配器"""

    def __init__(self, db_manager: DatabaseManager, config: Config = None, **kwargs):
        """
        初始化港股市场适配器

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
        """初始化港股适配器特定配置"""
        # 港股市场配置
        self.market_code = self._get_config("market_code", "HK")
        self.currency = self._get_config("currency", "HKD")
        self.timezone_name = self._get_config("timezone", "Asia/Hong_Kong")
        self.timezone = pytz.timezone(self.timezone_name)

    def _init_components(self):
        """初始化港股适配器组件"""
        # 港股交易时间
        self.trading_sessions = {
            "morning": {"start": time(9, 30), "end": time(12, 0)},
            "afternoon": {"start": time(13, 0), "end": time(16, 0)},
        }

        # 港股特有字段映射
        self.field_mapping = {
            "lot_size": "lot_size",  # 每手股数
            "board_lot": "board_lot",  # 买卖单位
            "warrant_type": "warrant_type",  # 权证类型
            "underlying_code": "underlying_code",  # 正股代码
            "conversion_ratio": "conversion_ratio",  # 换股比率
            "strike_price": "strike_price",  # 行权价
            "maturity_date": "maturity_date",  # 到期日
            "listing_date": "listing_date",  # 上市日期
            "delisting_date": "delisting_date",  # 退市日期
        }

        # 港股股票类型
        self.stock_types = {
            "ordinary": "普通股",
            "preference": "优先股",
            "warrant": "权证",
            "cbbc": "牛熊证",
            "etf": "ETF",
            "reit": "REITs",
            "stapled": "合订证券",
        }

        logger.info("港股市场适配器初始化完成")

    def _get_required_attributes(self) -> List[str]:
        """必需属性列表"""
        return ["db_manager", "trading_sessions", "field_mapping", "stock_types"]

    def adapt_stock_info(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        适配港股股票信息

        Args:
            raw_data: 原始股票信息数据

        Returns:
            Dict[str, Any]: 适配后的股票信息
        """
        try:
            adapted_data = {
                "symbol": self._normalize_symbol(raw_data.get("symbol", "")),
                "name": raw_data.get("name", ""),
                "name_en": raw_data.get("name_en", ""),
                "market": self.market_code,
                "exchange": "HKEX",
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

            # 港股特有字段
            hk_specific = {
                "lot_size": self._parse_number(raw_data.get("lot_size", 100)),
                "board_lot": self._parse_number(raw_data.get("board_lot", 1)),
                "par_value": self._parse_number(raw_data.get("par_value")),
                "listing_date": self._parse_date(raw_data.get("listing_date")),
            }

            # 权证特有字段
            if adapted_data["stock_type"] in ["warrant", "cbbc"]:
                warrant_fields = {
                    "warrant_type": raw_data.get("warrant_type", ""),
                    "underlying_code": self._normalize_symbol(
                        raw_data.get("underlying_code", "")
                    ),
                    "conversion_ratio": self._parse_number(
                        raw_data.get("conversion_ratio")
                    ),
                    "strike_price": self._parse_number(raw_data.get("strike_price")),
                    "maturity_date": self._parse_date(raw_data.get("maturity_date")),
                    "issuer": raw_data.get("issuer", ""),
                }
                hk_specific.update(warrant_fields)

            adapted_data.update(hk_specific)

            logger.debug(f"港股股票信息适配完成: {adapted_data['symbol']}")
            return adapted_data

        except Exception as e:
            logger.error(f"港股股票信息适配失败: {e}")
            return raw_data

    def adapt_price_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        适配港股价格数据

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
                # 港股特有字段
                "turnover": self._parse_number(raw_data.get("turnover")),  # 成交额
                "lot_volume": self._parse_number(
                    raw_data.get("lot_volume")
                ),  # 成交手数
                "bid_price": self._parse_price(raw_data.get("bid_price")),  # 买入价
                "ask_price": self._parse_price(raw_data.get("ask_price")),  # 卖出价
                "bid_volume": self._parse_number(raw_data.get("bid_volume")),  # 买入量
                "ask_volume": self._parse_number(raw_data.get("ask_volume")),  # 卖出量
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

            # 港股没有涨跌停限制
            adapted_data["high_limit"] = None
            adapted_data["low_limit"] = None
            adapted_data["unlimited"] = True

            logger.debug(f"港股价格数据适配完成: {adapted_data['symbol']}")
            return adapted_data

        except Exception as e:
            logger.error(f"港股价格数据适配失败: {e}")
            return raw_data

    def get_trading_calendar(
        self, start_date: date, end_date: date
    ) -> List[Dict[str, Any]]:
        """
        获取港股交易日历

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
                    }
                )

                current_date += timedelta(days=1)

            logger.info(
                f"港股交易日历生成完成: {start_date} 到 {end_date}, 共 {len(calendar_data)} 天"
            )
            return calendar_data

        except Exception as e:
            logger.error(f"港股交易日历生成失败: {e}")
            return []

    def _normalize_symbol(self, symbol: str) -> str:
        """标准化港股代码"""
        if not symbol:
            return ""

        # 移除空格并转换为大写
        symbol = symbol.strip().upper()

        # 港股代码格式：5位数字 + .HK
        if symbol.isdigit() and len(symbol) == 5:
            return f"{symbol}.HK"
        elif symbol.endswith(".HK"):
            return symbol
        elif symbol.isdigit() and len(symbol) <= 5:
            # 补齐到5位
            return f"{symbol.zfill(5)}.HK"

        return symbol

    def _parse_price(self, value: Any) -> Optional[float]:
        """解析价格数据"""
        if value is None or value == "":
            return None

        try:
            price = float(value)
            return round(price, 3)  # 港股价格精度到3位小数
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
                for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"]:
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

    def _map_stock_status(self, status: str) -> str:
        """映射股票状态"""
        status_mapping = {
            "L": "active",  # 正常交易
            "S": "suspended",  # 停牌
            "D": "delisted",  # 退市
            "N": "new",  # 新股
            "P": "pending",  # 待上市
        }

        return status_mapping.get(status.upper(), "active")

    def _map_stock_type(self, stock_type: str) -> str:
        """映射股票类型"""
        type_mapping = {
            "O": "ordinary",  # 普通股
            "P": "preference",  # 优先股
            "W": "warrant",  # 权证
            "C": "cbbc",  # 牛熊证
            "E": "etf",  # ETF
            "R": "reit",  # REITs
            "S": "stapled",  # 合订证券
        }

        return type_mapping.get(stock_type.upper(), "ordinary")

    def _format_trading_hours(self) -> str:
        """格式化交易时间"""
        morning = self.trading_sessions["morning"]
        afternoon = self.trading_sessions["afternoon"]

        return (
            f"{morning['start'].strftime('%H:%M')}-{morning['end'].strftime('%H:%M')},"
            f"{afternoon['start'].strftime('%H:%M')}-{afternoon['end'].strftime('%H:%M')}"
        )

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
        """判断是否为港股节假日"""
        # 港股主要固定节假日
        # 注意：农历节假日需要使用专门的农历计算库来准确计算
        holidays = [
            # 元旦
            (1, 1),
            # 农历新年（需要动态计算）
            # 清明节（需要动态计算）
            # 劳动节
            (5, 1),
            # 佛诞（需要动态计算）
            # 端午节（需要动态计算）
            # 香港特别行政区成立纪念日
            (7, 1),
            # 中秋节翌日（需要动态计算）
            # 国庆节
            (10, 1),
            # 重阳节（需要动态计算）
            # 圣诞节
            (12, 25),
            # 节礼日
            (12, 26),
        ]

        # 检查固定节假日
        for month, day in holidays:
            if target_date.month == month and target_date.day == day:
                return True

        # TODO: 添加农历节假日计算
        # 建议使用专门的农历计算库（如 lunardate）来准确计算农历节假日

        return False

    def get_market_info(self) -> Dict[str, Any]:
        """获取港股市场信息"""
        return {
            "market_code": self.market_code,
            "market_name": "香港交易所",
            "market_name_en": "Hong Kong Stock Exchange",
            "exchange": "HKEX",
            "currency": self.currency,
            "timezone": str(self.timezone),
            "trading_hours": self._format_trading_hours(),
            "trading_sessions": self.trading_sessions,
            "price_precision": 3,
            "min_price_change": 0.001,
            "has_price_limit": False,
            "supported_frequencies": ["1m", "5m", "15m", "30m", "60m", "1d"],
            "stock_types": list(self.stock_types.keys()),
        }
