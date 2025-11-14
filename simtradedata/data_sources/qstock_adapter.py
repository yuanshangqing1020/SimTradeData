"""
QStock数据源适配器

提供QStock数据源的统一接口实现。
"""

import logging
import threading
from datetime import date
from typing import Any, Dict, List, Optional, Union

from .base import BaseDataSource, DataSourceDataError

logger = logging.getLogger(__name__)

# 全局标志：是否正在中断
_interrupting = threading.Event()


class QStockAdapter(BaseDataSource):
    """QStock数据源适配器"""

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化QStock适配器

        Args:
            config: 配置参数
        """
        super().__init__("qstock", config)
        self._qstock = None

        # QStock特定配置
        self.token = self.config.get("token", "")

    def connect(self) -> bool:
        """连接QStock"""
        import qstock as qs

        self._qstock = qs
        if self.token:
            qs.set_token(self.token)
        self._connected = True
        return True

    def disconnect(self):
        """断开QStock连接"""
        # 如果正在中断过程中，立即返回，避免阻塞
        if _interrupting.is_set():
            logger.debug("interrupt detected , skip QStock cleanup")
            self._qstock = None
            self._connected = False
            return

        if self._qstock is not None:
            # 尝试清理qstock的全局session资源
            # 由于qstock的session.close()可能阻塞很久，我们异步关闭并立即返回
            try:
                from qstock.data import util

                if hasattr(util, "session"):
                    # 在后台daemon线程中关闭，不等待完成
                    def _close_session():
                        try:
                            util.session.close()
                            logger.debug("QStock global session closed")
                        except Exception as e:
                            logger.debug(f"close session error occurred : {e}")

                    close_thread = threading.Thread(target=_close_session, daemon=True)
                    close_thread.start()
                    # 不等待线程完成，立即返回
                    logger.debug("QStock session closing in background")
            except KeyboardInterrupt:
                # 如果在close过程中被中断，设置全局标志并立即返回
                _interrupting.set()
                logger.debug("QStock cleanup interrupted")
                raise
            except Exception as e:
                logger.debug(f"cleaning QStock session failed (ignore ) : {e}")

        self._qstock = None
        self._connected = False
        logger.info("QStock connection disconnected")

    def is_connected(self) -> bool:
        """检查连接状态"""
        return self._connected and self._qstock is not None

    def get_daily_data(
        self,
        symbol: str,
        start_date: Union[str, date],
        end_date: Union[str, date] = None,
    ) -> Dict[str, Any]:
        """
        获取日线数据

        Args:
            symbol: 股票代码 (如: 000001.SZ)
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            Dict[str, Any]: 日线数据
        """
        if not self.is_connected():
            self.connect()

        symbol = self._normalize_symbol(symbol)
        start_date = self._normalize_date(start_date)
        end_date = self._normalize_date(end_date) if end_date else start_date

        def _fetch_data():
            # 转换为QStock格式
            qs_symbol = self._convert_to_qstock_symbol(symbol)

            # 获取日线数据
            df = self._qstock.get_data(
                qs_symbol, start=start_date, end=end_date, fqt=1  # 前复权
            )

            return df

        return self._retry_request(_fetch_data)

    def get_minute_data(
        self, symbol: str, trade_date: Union[str, date], frequency: str = "5m"
    ) -> Dict[str, Any]:
        """
        获取分钟线数据

        Args:
            symbol: 股票代码
            trade_date: 交易日期
            frequency: 频率 (1m/5m/15m/30m/60m)

        Returns:
            Dict[str, Any]: 分钟线数据
        """
        if not self.is_connected():
            self.connect()

        symbol = self._normalize_symbol(symbol)
        trade_date = self._normalize_date(trade_date)
        frequency = self._validate_frequency(frequency)

        def _fetch_data():
            # 转换为QStock格式
            qs_symbol = self._convert_to_qstock_symbol(symbol)

            # 转换频率
            freq_map = {
                "1m": "1min",
                "5m": "5min",
                "15m": "15min",
                "30m": "30min",
                "60m": "60min",
            }

            qs_freq = freq_map[frequency]

            # 获取分钟线数据
            df = self._qstock.get_data(
                qs_symbol, start=trade_date, end=trade_date, klt=qs_freq, fqt=1
            )

            return df

        return self._retry_request(_fetch_data)

    def get_stock_info(
        self, symbol: str = None
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        获取股票基础信息

        Args:
            symbol: 股票代码，为None时返回所有股票

        Returns:
            Union[Dict, List[Dict]]: 股票信息
        """
        if not self.is_connected():
            self.connect()

        def _fetch_data():
            if symbol:
                # 获取单个股票信息
                symbol_norm = self._normalize_symbol(symbol)
                qs_symbol = self._convert_to_qstock_symbol(symbol_norm)

                # QStock获取股票信息的方法
                info = self._qstock.get_stock_info(qs_symbol)
                return info
            else:
                # 获取所有股票列表
                stock_list = self._qstock.get_stock_list()
                return stock_list

        return self._retry_request(_fetch_data)

    def get_stock_list_by_market(self, market: str) -> List[Dict[str, Any]]:
        """获取指定市场的股票列表 (港股/美股)

        Args:
            market: 市场代码 (HK/US)

        Returns:
            List[Dict[str, Any]]: 股票列表，包含标准化 symbol/name/market
        """
        if not market:
            raise ValueError("market 参数不能为空")

        market = market.upper()
        if market not in {"HK", "US"}:
            raise ValueError(f"暂不支持的市场: {market}")

        if not self.is_connected():
            self.connect()

        def _fetch_data():
            qs_market_map = {"HK": "hk", "US": "us"}
            qs_market = qs_market_map.get(market, market.lower())

            # 不同版本 qstock 的 get_stock_list 签名可能不同，容错处理
            fetch_fn = getattr(self._qstock, "get_stock_list", None)
            if fetch_fn is None:
                raise DataSourceDataError("当前 QStock 版本不支持 get_stock_list")

            try:
                data = fetch_fn(market=qs_market)
            except TypeError:
                # 兼容旧版本将 market 作为位置参数的情况
                data = fetch_fn(qs_market)

            if data is None:
                raise DataSourceDataError(f"未获取到 {market} 股票列表数据")

            return data

        raw_data = self._retry_request(_fetch_data)

        # 将结果统一转换为记录列表
        records: List[Dict[str, Any]] = []
        if hasattr(raw_data, "to_dict"):
            try:
                records = raw_data.to_dict("records")  # type: ignore[assignment]
            except Exception as err:  # pragma: no cover - 防御旧版本行为
                logger.warning(
                    f"QStock 股票列表转换失败({market})，将尝试回退处理: {err}"
                )
        elif isinstance(raw_data, list):
            records = []
            for item in raw_data:
                if isinstance(item, dict):
                    records.append(item)
        elif isinstance(raw_data, dict):
            # 个别版本会返回 {'hk': df, 'us': df} 结构
            candidate = raw_data.get(market.lower()) or raw_data.get(market.upper())
            if hasattr(candidate, "to_dict"):
                records = candidate.to_dict("records")  # type: ignore[assignment]

        if not records:
            raise DataSourceDataError(f"QStock 返回的 {market} 股票列表为空")

        normalized: Dict[str, Dict[str, Any]] = {}
        for item in records:
            if not isinstance(item, dict):
                continue

            raw_code = self._extract_code(item)
            raw_name = self._extract_name(item)

            if not raw_code or not raw_name:
                continue

            symbol = self._format_global_symbol(raw_code, market)
            if not symbol:
                continue

            normalized[symbol] = {
                "symbol": symbol,
                "name": raw_name.strip(),
                "market": market,
            }

        if not normalized:
            raise DataSourceDataError(f"未能解析 {market} 股票列表字段")

        return list(normalized.values())

    def get_fundamentals(
        self, symbol: str, report_date: Union[str, date], report_type: str = "Q4"
    ) -> Dict[str, Any]:
        """
        获取财务数据

        Args:
            symbol: 股票代码
            report_date: 报告期
            report_type: 报告类型

        Returns:
            Dict[str, Any]: 财务数据
        """
        if not self.is_connected():
            self.connect()

        symbol = self._normalize_symbol(symbol)
        report_date = self._normalize_date(report_date)

        def _fetch_data():
            try:
                qs_symbol = self._convert_to_qstock_symbol(symbol)

                # 获取财务数据
                df = self._qstock.get_financial_data(qs_symbol, report_date[:4])

                if df.empty:
                    raise DataSourceDataError(f"未获取到财务数据: {symbol}")

                return df

            except Exception as e:
                logger.error(f"QStock failed to retrieve financial data {symbol}: {e}")
                raise DataSourceDataError(f"获取财务数据失败: {e}")

        return self._retry_request(_fetch_data)

    def get_block_data(self, block_type: str = "industry") -> List[Dict[str, Any]]:
        """
        获取板块数据 (QStock特色功能)

        Args:
            block_type: 板块类型 (industry/concept/area)

        Returns:
            List[Dict]: 板块数据
        """
        if not self.is_connected():
            self.connect()

        def _fetch_data():
            try:
                # 获取板块数据
                df = self._qstock.get_block_data(block_type)
                return df

            except Exception as e:
                logger.error(f"QStock failed to retrieve sector data {block_type}: {e}")
                raise DataSourceDataError(f"获取板块数据失败: {e}")

        return self._retry_request(_fetch_data)

    def get_capital_flow(
        self, symbol: str, trade_date: Union[str, date]
    ) -> Dict[str, Any]:
        """
        获取资金流向数据 (QStock特色功能)

        Args:
            symbol: 股票代码
            trade_date: 交易日期

        Returns:
            Dict: 资金流向数据
        """
        if not self.is_connected():
            self.connect()

        symbol = self._normalize_symbol(symbol)
        trade_date = self._normalize_date(trade_date)

        def _fetch_data():
            try:
                qs_symbol = self._convert_to_qstock_symbol(symbol)

                # 获取资金流向数据
                df = self._qstock.get_capital_flow(qs_symbol, trade_date)
                return df

            except Exception as e:
                logger.error(f"QStock failed to retrieve capital flow {symbol}: {e}")
                raise DataSourceDataError(f"获取资金流向失败: {e}")

        return self._retry_request(_fetch_data)

    def get_balance_sheet(
        self, symbol: str, report_date: Union[str, date] = None
    ) -> Dict[str, Any]:
        """
        获取资产负债表（110+科目）

        Args:
            symbol: 股票代码
            report_date: 报表日期，格式'YYYYMMDD'，如'20220630'。为None时返回最新报表

        Returns:
            Dict[str, Any]: 资产负债表数据
        """
        if not self.is_connected():
            self.connect()

        def _fetch_data():
            try:
                # QStock使用financial_statement接口
                # 参数: flag='资产负债表'或'zcfz', date=报表日期
                date_param = None
                if report_date:
                    # 转换日期格式为YYYYMMDD
                    date_str = self._normalize_date(report_date).replace("-", "")
                    date_param = date_str

                df = self._qstock.financial_statement("资产负债表", date=date_param)

                if df is None or (hasattr(df, "empty") and df.empty):
                    raise DataSourceDataError(f"未获取到资产负债表数据: {symbol}")

                return df

            except Exception as e:
                logger.error(f"QStock failed to retrieve balance sheet {symbol}: {e}")
                raise DataSourceDataError(f"获取资产负债表失败: {e}")

        return self._retry_request(_fetch_data)

    def get_income_statement(
        self, symbol: str, report_date: Union[str, date] = None
    ) -> Dict[str, Any]:
        """
        获取利润表（55+科目）

        Args:
            symbol: 股票代码
            report_date: 报表日期，格式'YYYYMMDD'，如'20220630'。为None时返回最新报表

        Returns:
            Dict[str, Any]: 利润表数据
        """
        if not self.is_connected():
            self.connect()

        def _fetch_data():
            try:
                date_param = None
                if report_date:
                    date_str = self._normalize_date(report_date).replace("-", "")
                    date_param = date_str

                df = self._qstock.financial_statement("利润表", date=date_param)

                if df is None or (hasattr(df, "empty") and df.empty):
                    raise DataSourceDataError(f"未获取到利润表数据: {symbol}")

                return df

            except Exception as e:
                logger.error(
                    f"QStock failed to retrieve income statement {symbol}: {e}"
                )
                raise DataSourceDataError(f"获取利润表失败: {e}")

        return self._retry_request(_fetch_data)

    def get_cash_flow(
        self, symbol: str, report_date: Union[str, date] = None
    ) -> Dict[str, Any]:
        """
        获取现金流量表（75+科目）

        Args:
            symbol: 股票代码
            report_date: 报表日期，格式'YYYYMMDD'，如'20220630'。为None时返回最新报表

        Returns:
            Dict[str, Any]: 现金流量表数据
        """
        if not self.is_connected():
            self.connect()

        def _fetch_data():
            try:
                date_param = None
                if report_date:
                    date_str = self._normalize_date(report_date).replace("-", "")
                    date_param = date_str

                df = self._qstock.financial_statement("现金流量表", date=date_param)

                if df is None or (hasattr(df, "empty") and df.empty):
                    raise DataSourceDataError(f"未获取到现金流量表数据: {symbol}")

                return df

            except Exception as e:
                logger.error(
                    f"QStock failed to retrieve cash flow statement {symbol}: {e}"
                )
                raise DataSourceDataError(f"获取现金流量表失败: {e}")

        return self._retry_request(_fetch_data)

    def _convert_to_qstock_symbol(self, symbol: str) -> str:
        """转换为QStock股票代码格式"""
        # QStock通常使用6位数字代码
        if "." in symbol:
            return symbol.split(".")[0]
        return symbol

    @staticmethod
    def _extract_code(item: Dict[str, Any]) -> Optional[str]:
        """从不同字段中提取股票代码"""
        keys = [
            "code",
            "symbol",
            "ticker",
            "证券代码",
            "股票代码",
            "Code",
        ]
        for key in keys:
            value = item.get(key)
            if value:
                return str(value).strip()
        return None

    @staticmethod
    def _extract_name(item: Dict[str, Any]) -> Optional[str]:
        """从不同字段中提取股票名称"""
        keys = [
            "name",
            "stock_name",
            "证券简称",
            "公司名称",
            "Name",
        ]
        for key in keys:
            value = item.get(key)
            if value:
                return str(value).strip()
        return None

    @staticmethod
    def _format_global_symbol(code: str, market: str) -> Optional[str]:
        """根据市场格式化股票代码"""
        if not code:
            return None

        clean_code = code.strip().upper()

        if market == "HK":
            # 去除可能的其他后缀
            if clean_code.endswith(".HK"):
                clean_code = clean_code[:-3]
            if clean_code.endswith("HK"):
                clean_code = clean_code[:-2]

            clean_code = clean_code.replace(".", "")
            if clean_code.isdigit():
                clean_code = clean_code.zfill(5)

            return f"{clean_code}.HK"

        if market == "US":
            clean_code = clean_code.replace(".US", "")
            clean_code = clean_code.replace("-US", "")
            clean_code = clean_code.replace(" US", "")
            clean_code = clean_code.split(" ")[0]
            clean_code = clean_code.replace(".", "-")  # 保留点分隔符的兼容

            if not clean_code:
                return None

            return f"{clean_code}.US"

        return None
