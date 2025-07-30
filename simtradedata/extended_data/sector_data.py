"""
板块数据管理器

负责行业分类、概念板块和指数成分股数据的管理。
"""

# 标准库导入
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

# 项目内导入
from ..core.base_manager import BaseManager
from ..database import DatabaseManager

logger = logging.getLogger(__name__)


class SectorDataManager(BaseManager):
    """板块数据管理器"""

    def __init__(self, db_manager: DatabaseManager = None, config=None, **dependencies):
        """
        初始化板块数据管理器

        Args:
            db_manager: 数据库管理器
            config: 配置对象
            **dependencies: 其他依赖对象
        """
        # 获取数据库管理器 - 在super().__init__前设置
        self.db_manager = db_manager
        if not self.db_manager:
            raise ValueError("数据库管理器不能为空")

        # 调用BaseManager初始化
        super().__init__(config=config, db_manager=db_manager, **dependencies)

        self.logger.info("板块数据管理器初始化完成")

        # 行业分类标准
        self.industry_standards = {
            "sw": "申万行业分类",
            "citic": "中信行业分类",
            "zjh": "证监会行业分类",
            "gics": "GICS行业分类",
        }

        # 板块类型
        self.sector_types = {
            "industry": "行业板块",
            "concept": "概念板块",
            "region": "地域板块",
            "theme": "主题板块",
            "index": "指数板块",
        }

        # 指数类型
        self.index_types = {
            "broad": "宽基指数",
            "sector": "行业指数",
            "theme": "主题指数",
            "style": "风格指数",
            "strategy": "策略指数",
        }

    def _init_components(self):
        """初始化板块数据组件"""
        pass  # 组件初始化在__init__中完成

    def _get_required_attributes(self) -> list:
        """获取必需属性列表"""
        return ["db_manager"]

    def save_industry_classification(self, classification_data: Dict[str, Any]) -> bool:
        """
        保存行业分类数据

        Args:
            classification_data: 行业分类数据

        Returns:
            bool: 是否保存成功
        """
        try:
            standardized_data = self._standardize_classification_data(
                classification_data
            )

            sql = """
            INSERT OR REPLACE INTO ptrade_industry_classification 
            (symbol, stock_name, standard, level1_code, level1_name, 
             level2_code, level2_name, level3_code, level3_name, 
             effective_date, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            self.db_manager.execute(
                sql,
                (
                    standardized_data["symbol"],
                    standardized_data.get("stock_name"),
                    standardized_data.get("standard", "sw"),
                    standardized_data.get("level1_code"),
                    standardized_data.get("level1_name"),
                    standardized_data.get("level2_code"),
                    standardized_data.get("level2_name"),
                    standardized_data.get("level3_code"),
                    standardized_data.get("level3_name"),
                    standardized_data.get("effective_date"),
                    datetime.now().isoformat(),
                ),
            )

            logger.debug(f"行业分类保存成功: {standardized_data['symbol']}")
            return True

        except Exception as e:
            logger.error(f"保存行业分类失败: {e}")
            return False

    def save_concept_sector(self, sector_data: Dict[str, Any]) -> bool:
        """
        保存概念板块数据

        Args:
            sector_data: 概念板块数据

        Returns:
            bool: 是否保存成功
        """
        try:
            standardized_data = self._standardize_sector_data(sector_data)

            sql = """
            INSERT OR REPLACE INTO ptrade_concept_sectors 
            (sector_code, sector_name, sector_type, description, 
             creation_date, status, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """

            self.db_manager.execute(
                sql,
                (
                    standardized_data["sector_code"],
                    standardized_data["sector_name"],
                    standardized_data.get("sector_type", "concept"),
                    standardized_data.get("description"),
                    standardized_data.get("creation_date"),
                    standardized_data.get("status", "active"),
                    datetime.now().isoformat(),
                ),
            )

            logger.debug(f"概念板块保存成功: {standardized_data['sector_code']}")
            return True

        except Exception as e:
            logger.error(f"保存概念板块失败: {e}")
            return False

    def save_sector_constituents(
        self,
        sector_code: str,
        constituents: List[Dict[str, Any]],
        effective_date: date = None,
    ) -> bool:
        """
        保存板块成分股

        Args:
            sector_code: 板块代码
            constituents: 成分股列表
            effective_date: 生效日期

        Returns:
            bool: 是否保存成功
        """
        try:
            if effective_date is None:
                effective_date = datetime.now().date()

            # 删除旧的成分股数据
            delete_sql = """
            DELETE FROM ptrade_sector_constituents 
            WHERE sector_code = ? AND effective_date = ?
            """
            self.db_manager.execute(delete_sql, (sector_code, str(effective_date)))

            # 插入新的成分股数据
            insert_sql = """
            INSERT INTO ptrade_sector_constituents 
            (sector_code, symbol, stock_name, weight, market_value, 
             effective_date, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """

            saved_count = 0
            for constituent in constituents:
                try:
                    standardized_constituent = self._standardize_constituent_data(
                        constituent
                    )

                    self.db_manager.execute(
                        insert_sql,
                        (
                            sector_code,
                            standardized_constituent["symbol"],
                            standardized_constituent.get("stock_name"),
                            standardized_constituent.get("weight"),
                            standardized_constituent.get("market_value"),
                            str(effective_date),
                            datetime.now().isoformat(),
                        ),
                    )

                    saved_count += 1

                except Exception as e:
                    logger.error(f"保存单个成分股失败 {constituent}: {e}")

            logger.info(
                f"板块成分股保存完成: {sector_code}, 成功保存 {saved_count} 只成分股"
            )
            return saved_count > 0

        except Exception as e:
            logger.error(f"保存板块成分股失败: {e}")
            return False

    def save_index_info(self, index_data: Dict[str, Any]) -> bool:
        """
        保存指数信息

        Args:
            index_data: 指数信息数据

        Returns:
            bool: 是否保存成功
        """
        try:
            standardized_data = self._standardize_index_data(index_data)

            sql = """
            INSERT OR REPLACE INTO ptrade_index_info 
            (index_code, index_name, index_name_en, market, index_type, 
             base_date, base_value, calculation_method, weighting_method, 
             publisher, launch_date, status, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            self.db_manager.execute(
                sql,
                (
                    standardized_data["index_code"],
                    standardized_data["index_name"],
                    standardized_data.get("index_name_en"),
                    standardized_data.get("market"),
                    standardized_data.get("index_type", "broad"),
                    standardized_data.get("base_date"),
                    standardized_data.get("base_value"),
                    standardized_data.get("calculation_method"),
                    standardized_data.get("weighting_method"),
                    standardized_data.get("publisher"),
                    standardized_data.get("launch_date"),
                    standardized_data.get("status", "active"),
                    datetime.now().isoformat(),
                ),
            )

            logger.debug(f"指数信息保存成功: {standardized_data['index_code']}")
            return True

        except Exception as e:
            logger.error(f"保存指数信息失败: {e}")
            return False

    def get_stock_industry(
        self, symbol: str, standard: str = "sw"
    ) -> Optional[Dict[str, Any]]:
        """
        获取股票行业分类

        Args:
            symbol: 股票代码
            standard: 分类标准

        Returns:
            Optional[Dict[str, Any]]: 行业分类信息
        """
        try:
            sql = """
            SELECT * FROM ptrade_industry_classification 
            WHERE symbol = ? AND standard = ?
            ORDER BY effective_date DESC LIMIT 1
            """

            result = self.db_manager.fetchone(sql, (symbol, standard))

            if result:
                return dict(result)
            else:
                return None

        except Exception as e:
            logger.error(f"获取股票行业分类失败: {e}")
            return None

    def get_sector_constituents(
        self, sector_code: str, effective_date: date = None
    ) -> List[Dict[str, Any]]:
        """
        获取板块成分股

        Args:
            sector_code: 板块代码
            effective_date: 生效日期，默认为最新

        Returns:
            List[Dict[str, Any]]: 成分股列表
        """
        try:
            if effective_date is None:
                # 获取最新生效日期
                date_sql = """
                SELECT MAX(effective_date) as latest_date 
                FROM ptrade_sector_constituents 
                WHERE sector_code = ?
                """
                date_result = self.db_manager.fetchone(date_sql, (sector_code,))

                if not date_result or not date_result["latest_date"]:
                    return []

                effective_date = datetime.strptime(
                    date_result["latest_date"], "%Y-%m-%d"
                ).date()

            sql = """
            SELECT * FROM ptrade_sector_constituents 
            WHERE sector_code = ? AND effective_date = ?
            ORDER BY weight DESC
            """

            results = self.db_manager.fetchall(sql, (sector_code, str(effective_date)))

            return [dict(row) for row in results]

        except Exception as e:
            logger.error(f"获取板块成分股失败: {e}")
            return []

    def get_stock_sectors(self, symbol: str) -> List[Dict[str, Any]]:
        """
        获取股票所属板块

        Args:
            symbol: 股票代码

        Returns:
            List[Dict[str, Any]]: 所属板块列表
        """
        try:
            sql = """
            SELECT DISTINCT sc.sector_code, cs.sector_name, cs.sector_type
            FROM ptrade_sector_constituents sc
            JOIN ptrade_concept_sectors cs ON sc.sector_code = cs.sector_code
            WHERE sc.symbol = ?
            ORDER BY cs.sector_type, cs.sector_name
            """

            results = self.db_manager.fetchall(sql, (symbol,))

            return [dict(row) for row in results]

        except Exception as e:
            logger.error(f"获取股票所属板块失败: {e}")
            return []

    def get_sector_list(self, sector_type: str = None) -> List[Dict[str, Any]]:
        """
        获取板块列表

        Args:
            sector_type: 板块类型筛选

        Returns:
            List[Dict[str, Any]]: 板块列表
        """
        try:
            conditions = []
            params = []

            if sector_type:
                conditions.append("sector_type = ?")
                params.append(sector_type)

            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

            sql = f"""
            SELECT sector_code, sector_name, sector_type, description, status
            FROM ptrade_concept_sectors 
            {where_clause}
            ORDER BY sector_type, sector_name
            """

            results = self.db_manager.fetchall(sql, params)

            return [dict(row) for row in results]

        except Exception as e:
            logger.error(f"获取板块列表失败: {e}")
            return []

    def get_industry_tree(self, standard: str = "sw") -> Dict[str, Any]:
        """
        获取行业分类树

        Args:
            standard: 分类标准

        Returns:
            Dict[str, Any]: 行业分类树
        """
        try:
            sql = """
            SELECT DISTINCT level1_code, level1_name, level2_code, level2_name, 
                   level3_code, level3_name
            FROM ptrade_industry_classification 
            WHERE standard = ?
            ORDER BY level1_code, level2_code, level3_code
            """

            results = self.db_manager.fetchall(sql, (standard,))

            # 构建树形结构
            tree = {}

            for row in results:
                level1_code = row["level1_code"]
                level1_name = row["level1_name"]

                if level1_code not in tree:
                    tree[level1_code] = {
                        "code": level1_code,
                        "name": level1_name,
                        "children": {},
                    }

                if row["level2_code"]:
                    level2_code = row["level2_code"]
                    level2_name = row["level2_name"]

                    if level2_code not in tree[level1_code]["children"]:
                        tree[level1_code]["children"][level2_code] = {
                            "code": level2_code,
                            "name": level2_name,
                            "children": {},
                        }

                    if row["level3_code"]:
                        level3_code = row["level3_code"]
                        level3_name = row["level3_name"]

                        tree[level1_code]["children"][level2_code]["children"][
                            level3_code
                        ] = {"code": level3_code, "name": level3_name, "children": {}}

            return {
                "standard": standard,
                "standard_name": self.industry_standards.get(standard, standard),
                "tree": tree,
            }

        except Exception as e:
            logger.error(f"获取行业分类树失败: {e}")
            return {}

    def calculate_sector_performance(
        self, sector_code: str, days: int = 30
    ) -> Dict[str, Any]:
        """
        计算板块业绩表现

        Args:
            sector_code: 板块代码
            days: 计算天数

        Returns:
            Dict[str, Any]: 板块业绩数据
        """
        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)

            # 获取板块成分股
            constituents = self.get_sector_constituents(sector_code)

            if not constituents:
                return {}

            # 获取成分股价格数据并计算加权收益率
            total_weight = 0
            weighted_return = 0
            valid_stocks = 0

            for constituent in constituents:
                symbol = constituent["symbol"]
                weight = constituent.get("weight", 0) or 0

                # 获取股票价格数据
                price_sql = """
                SELECT close FROM market_data 
                WHERE symbol = ? AND trade_date = ? AND frequency = '1d'
                """

                start_price_result = self.db_manager.fetchone(
                    price_sql, (symbol, str(start_date))
                )
                end_price_result = self.db_manager.fetchone(
                    price_sql, (symbol, str(end_date))
                )

                if start_price_result and end_price_result:
                    start_price = start_price_result["close"]
                    end_price = end_price_result["close"]

                    if start_price and end_price and start_price > 0:
                        stock_return = (end_price - start_price) / start_price
                        weighted_return += stock_return * weight
                        total_weight += weight
                        valid_stocks += 1

            if total_weight > 0:
                sector_return = (weighted_return / total_weight) * 100
            else:
                sector_return = 0.0

            performance = {
                "sector_code": sector_code,
                "period_days": days,
                "start_date": str(start_date),
                "end_date": str(end_date),
                "return_rate": round(sector_return, 4),
                "total_stocks": len(constituents),
                "valid_stocks": valid_stocks,
                "total_weight": round(total_weight, 4),
            }

            logger.debug(
                f"板块业绩计算完成: {sector_code}, 收益率: {sector_return:.2f}%"
            )
            return performance

        except Exception as e:
            logger.error(f"计算板块业绩失败: {e}")
            return {}

    def _standardize_classification_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """标准化行业分类数据"""
        return {
            "symbol": data.get("symbol", "").upper(),
            "stock_name": data.get("stock_name"),
            "standard": data.get("standard", "sw"),
            "level1_code": data.get("level1_code"),
            "level1_name": data.get("level1_name"),
            "level2_code": data.get("level2_code"),
            "level2_name": data.get("level2_name"),
            "level3_code": data.get("level3_code"),
            "level3_name": data.get("level3_name"),
            "effective_date": data.get("effective_date"),
        }

    def _standardize_sector_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """标准化板块数据"""
        return {
            "sector_code": data.get("sector_code", "").upper(),
            "sector_name": data.get("sector_name"),
            "sector_type": data.get("sector_type", "concept"),
            "description": data.get("description"),
            "creation_date": data.get("creation_date"),
            "status": data.get("status", "active"),
        }

    def _standardize_constituent_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """标准化成分股数据"""
        return {
            "symbol": data.get("symbol", "").upper(),
            "stock_name": data.get("stock_name"),
            "weight": self._parse_float(data.get("weight")),
            "market_value": self._parse_float(data.get("market_value")),
        }

    def _standardize_index_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """标准化指数数据"""
        return {
            "index_code": data.get("index_code", "").upper(),
            "index_name": data.get("index_name"),
            "index_name_en": data.get("index_name_en"),
            "market": data.get("market"),
            "index_type": data.get("index_type", "broad"),
            "base_date": data.get("base_date"),
            "base_value": self._parse_float(data.get("base_value")),
            "calculation_method": data.get("calculation_method"),
            "weighting_method": data.get("weighting_method"),
            "publisher": data.get("publisher"),
            "launch_date": data.get("launch_date"),
            "status": data.get("status", "active"),
        }

    def _parse_float(self, value: Any) -> Optional[float]:
        """解析浮点数"""
        if value is None or value == "":
            return None

        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def get_manager_stats(self) -> Dict[str, Any]:
        """获取管理器统计信息"""
        try:
            # 板块数量统计
            sector_count_sql = "SELECT COUNT(*) as total FROM ptrade_concept_sectors"
            sector_count = self.db_manager.fetchone(sector_count_sql)

            # 按类型统计
            type_stats_sql = """
            SELECT sector_type, COUNT(*) as count 
            FROM ptrade_concept_sectors 
            GROUP BY sector_type
            """
            type_stats = self.db_manager.fetchall(type_stats_sql)

            # 行业分类统计
            industry_stats_sql = """
            SELECT standard, COUNT(DISTINCT symbol) as stock_count 
            FROM ptrade_industry_classification 
            GROUP BY standard
            """
            industry_stats = self.db_manager.fetchall(industry_stats_sql)

            return {
                "total_sectors": sector_count["total"] if sector_count else 0,
                "sector_types": self.sector_types,
                "industry_standards": self.industry_standards,
                "type_distribution": {
                    row["sector_type"]: row["count"] for row in type_stats
                },
                "industry_coverage": {
                    row["standard"]: row["stock_count"] for row in industry_stats
                },
                "supported_features": [
                    "行业分类管理",
                    "概念板块跟踪",
                    "板块成分股管理",
                    "指数信息管理",
                    "板块业绩分析",
                ],
            }

        except Exception as e:
            logger.error(f"获取管理器统计失败: {e}")
            return {}
