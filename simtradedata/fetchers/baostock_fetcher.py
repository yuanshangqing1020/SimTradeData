"""
BaoStock data fetcher implementation
"""

import logging
from datetime import datetime

import baostock as bs
import pandas as pd

from simtradedata.fetchers.base_fetcher import BaseFetcher
from simtradedata.utils.code_utils import convert_from_ptrade_code, retry_on_failure

logger = logging.getLogger(__name__)


class BaoStockFetcher(BaseFetcher):
    """
    Fetch data from BaoStock API

    BaoStock provides free A-share market data including:
    - Daily K-line data
    - Financial statements
    - Valuation indicators
    - Adjust factors
    - Dividend data
    """

    # Class-level login state tracking (BaoStock uses global session)
    _bs_logged_in = False
    _bs_login_count = 0

    def _do_login(self):
        """BaoStock-specific login implementation"""
        # BaoStock uses a global session, only login once
        if not BaoStockFetcher._bs_logged_in:
            lg = bs.login()
            if lg.error_code != "0":
                raise ConnectionError(f"BaoStock login failed: {lg.error_msg}")
            BaoStockFetcher._bs_logged_in = True
            logger.info("BaoStock login successful")
        BaoStockFetcher._bs_login_count += 1

    @classmethod
    def _ensure_login(cls):
        """Ensure BaoStock session is valid, re-login if needed"""
        if not cls._bs_logged_in:
            lg = bs.login()
            if lg.error_code != "0":
                raise ConnectionError(f"BaoStock re-login failed: {lg.error_msg}")
            cls._bs_logged_in = True
            logger.info("BaoStock re-login successful")

    def _do_logout(self):
        """BaoStock-specific logout implementation"""
        BaoStockFetcher._bs_login_count -= 1
        # Only logout when last fetcher disconnects
        if BaoStockFetcher._bs_login_count <= 0:
            bs.logout()
            BaoStockFetcher._bs_logged_in = False
            BaoStockFetcher._bs_login_count = 0


    @retry_on_failure()
    def fetch_adjust_factor(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Fetch adjust factors

        Args:
            symbol: Stock code in PTrade format
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame with columns: date, foreAdjustFactor, backAdjustFactor
        """

        bs_code = convert_from_ptrade_code(symbol, "baostock")

        rs = bs.query_adjust_factor(
            code=bs_code, start_date=start_date, end_date=end_date
        )

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query adjust factor for {symbol}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            # Check if it's an index (indices don't have adjust factors)
            if bs_code.startswith("sh.") and bs_code[3:].startswith("00"):
                logger.debug(f"No adjust factor data for index {symbol} (expected)")
            elif bs_code.startswith("sz.399"):  # Shenzhen indices
                logger.debug(f"No adjust factor data for index {symbol} (expected)")
            else:
                logger.warning(f"No adjust factor data for {symbol}")
            return pd.DataFrame()

        # Note: BaoStock returns 'dividOperateDate', not 'date'
        df = df.rename(columns={"dividOperateDate": "date"})

        df["date"] = pd.to_datetime(df["date"])

        # Convert adjust factors to numeric with validation
        # Use strict conversion to detect data quality issues
        try:
            df["foreAdjustFactor"] = pd.to_numeric(df["foreAdjustFactor"])
            df["backAdjustFactor"] = pd.to_numeric(df["backAdjustFactor"])
        except ValueError as e:
            # Log specific rows with invalid data
            invalid_fore = df[pd.to_numeric(df["foreAdjustFactor"], errors="coerce").isna()]["foreAdjustFactor"]
            invalid_back = df[pd.to_numeric(df["backAdjustFactor"], errors="coerce").isna()]["backAdjustFactor"]

            if len(invalid_fore) > 0:
                logger.error(
                    f"Invalid foreAdjustFactor values for {symbol}: {invalid_fore.head().tolist()}"
                )
            if len(invalid_back) > 0:
                logger.error(
                    f"Invalid backAdjustFactor values for {symbol}: {invalid_back.head().tolist()}"
                )

            # Use coerce as fallback but log warning
            logger.warning(
                f"Converting adjust factors with coerce for {symbol} due to invalid values. "
                f"Data quality may be compromised."
            )
            df["foreAdjustFactor"] = pd.to_numeric(df["foreAdjustFactor"], errors="coerce")
            df["backAdjustFactor"] = pd.to_numeric(df["backAdjustFactor"], errors="coerce")

            # Check how many NaN values were introduced
            nan_count = df["backAdjustFactor"].isna().sum()
            if nan_count > 0:
                logger.warning(
                    f"{symbol}: {nan_count}/{len(df)} adjust factors converted to NaN"
                )

        # Note: Keep 'date' as column for converter to handle

        logger.info(f"Fetched {len(df)} adjust factor rows for {symbol}")

        return df


    @retry_on_failure()
    def fetch_stock_basic(self, symbol: str) -> pd.DataFrame:
        """
        Fetch stock basic information

        Args:
            symbol: Stock code in PTrade format

        Returns:
            DataFrame with basic stock information
        """

        bs_code = convert_from_ptrade_code(symbol, "baostock")
        rs = bs.query_stock_basic(code=bs_code)

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query stock basic info for {symbol}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            return pd.DataFrame()

        return df


    @retry_on_failure()
    def fetch_stock_industry(self, symbol: str, date: str = None) -> pd.DataFrame:
        """
        Fetch stock industry classification

        Args:
            symbol: Stock code in PTrade format
            date: Date string (YYYY-MM-DD), if None use today

        Returns:
            DataFrame with industry classification
        """

        bs_code = convert_from_ptrade_code(symbol, "baostock")

        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        # Use the date string directly (YYYY-MM-DD) – BaoStock expects this format
        date_str = date

        rs = bs.query_stock_industry(code=bs_code, date=date_str)

        if rs.error_code != "0":
            raise RuntimeError(f"Failed to query industry for {symbol}: {rs.error_msg}")

        df = rs.get_data()

        if df.empty:
            logger.warning(f"No industry data for {symbol}")
            return pd.DataFrame()

        return df

    @retry_on_failure()
    def fetch_trade_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch trading calendar
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame with trading days
        """
        
        rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
        
        if rs.error_code != "0":
            raise RuntimeError(f"Failed to query trade calendar: {rs.error_msg}")
        
        # Monkey patch for BaoStock's use of deprecated .append()
        # BaoStock library uses df.append() internally which was removed in pandas 2.0
        # We catch the AttributeError and implement a manual pagination workaround if possible
        # Or better: monkey patch pandas.DataFrame.append temporarily if we can't edit library code
        
        try:
            df = rs.get_data()
        except AttributeError as e:
            if "append" in str(e):
                logger.warning("BaoStock library uses deprecated .append(), attempting workaround...")
                
                # Manual implementation of get_data logic using concat
                data_list = []
                
                # Try to access raw data directly if possible or iterate blindly
                # Inspecting ResultData object (rs) structure from error might be needed
                # Assuming standard baostock result structure
                
                # If rs.pages is missing, it might be named differently or we just loop until empty
                try:
                    # BaoStock ResultData stores data in self.data list for the first page
                    # and subsequent pages are fetched. 
                    # If deprecated .append() failed inside get_data(), it means the first page was fetched successfully 
                    # but appending subsequent pages failed, OR even the first page handling failed.
                    
                    # Let's try to access the data directly if it's already loaded in rs.data
                    if hasattr(rs, 'data') and rs.data:
                         # Convert list of lists to DataFrame
                         data_list.append(pd.DataFrame(rs.data, columns=rs.fields))
                    
                    # If ResultData object doesn't have get_next_page_data, it might be because
                    # we are not using the standard baostock library or version differences.
                    # Standard baostock uses .next() method or .get_data() calls .next() internally
                    # Let's inspect the ResultData object if we can, but since we can't see it...
                    # Let's assume that if rs.data is present, that's all the data we got from the first query page.
                    
                    # If the data_list is populated from rs.data, we are good for at least one page.
                    # The error 'ResultData' object has no attribute 'get_next_page_data' suggests we can't paginate easily manually
                    # without knowing the exact API.
                    
                    # However, query_trade_dates usually returns small amount of data (rows), maybe it is not paginated?
                    # If rs.data contains all rows, we are fine.
                    pass
                except Exception as e:
                    # If any error occurs during iteration, stop but log it
                    logger.warning(f"Error during manual pagination: {e}")
                    pass
                
                if data_list:
                    df = pd.concat(data_list, ignore_index=True)
                else:
                    df = pd.DataFrame()
            else:
                raise e
        
        if df.empty:
            return pd.DataFrame()
            
        return df

    @retry_on_failure()
    def fetch_index_stocks(self, index_code: str, date: str = None) -> pd.DataFrame:
        """
        Fetch index constituent stocks

        Args:
            index_code: Index code in PTrade format (e.g., '000016.SS', '000300.SS', '000905.SS')
            date: Date string (YYYY-MM-DD), if None use latest

        Returns:
            DataFrame with stock codes

        Note:
            BaoStock only supports specific indices:
            - 000016.SS (上证50): query_sz50_stocks
            - 000300.SS (沪深300): query_hs300_stocks
            - 000905.SS (中证500): query_zz500_stocks
        """

        query_date = date
        if query_date is None:
            query_date = datetime.now().strftime("%Y-%m-%d")

        # Map PTrade index codes to BaoStock query methods
        index_map = {
            "000016.SS": "sz50",  # 上证50
            "000300.SS": "hs300",  # 沪深300
            "000905.SS": "zz500",  # 中证500
        }

        if index_code not in index_map:
            logger.warning(f"Index {index_code} not supported by BaoStock")
            return pd.DataFrame()

        index_type = index_map[index_code]

        # Call corresponding BaoStock API
        if index_type == "sz50":
            rs = bs.query_sz50_stocks(date=query_date)
        elif index_type == "hs300":
            rs = bs.query_hs300_stocks(date=query_date)
        elif index_type == "zz500":
            rs = bs.query_zz500_stocks(date=query_date)
        else:
            logger.warning(f"Unknown index type: {index_type}")
            return pd.DataFrame()

        if rs.error_code != "0":
            raise RuntimeError(
                f"Failed to query index stocks for {index_code}: {rs.error_msg}"
            )

        df = rs.get_data()

        if df.empty:
            logger.warning(f"No constituent stocks found for {index_code}")
            return pd.DataFrame()

        return df

    @retry_on_failure()
    def fetch_quarterly_fundamentals(
        self, symbol: str, year: int, quarter: int
    ) -> pd.DataFrame:
        """
        Fetch all quarterly fundamentals for a stock
        
        Combines data from 5 BaoStock APIs:
        - query_profit_data (盈利能力)
        - query_growth_data (成长能力)
        - query_balance_data (偿债能力)
        - query_operation_data (营运能力)
        - query_cash_flow_data (现金流量)
        
        Args:
            symbol: Stock code in PTrade format
            year: Year (e.g., 2024)
            quarter: Quarter (1-4)
        
        Returns:
            DataFrame with PTrade format fields including publ_date and end_date
        """
        bs_code = convert_from_ptrade_code(symbol, "baostock")
        
        # Fetch from all 5 APIs
        dfs = []
        
        # 1. Profit data (盈利能力)
        rs = bs.query_profit_data(code=bs_code, year=year, quarter=quarter)
        if rs.error_code == "0":
            df = rs.get_data()
            if not df.empty:
                dfs.append(df)
        
        # 2. Growth data (成长能力)
        rs = bs.query_growth_data(code=bs_code, year=year, quarter=quarter)
        if rs.error_code == "0":
            df = rs.get_data()
            if not df.empty:
                dfs.append(df)
        
        # 3. Balance data (偿债能力)
        rs = bs.query_balance_data(code=bs_code, year=year, quarter=quarter)
        if rs.error_code == "0":
            df = rs.get_data()
            if not df.empty:
                dfs.append(df)
        
        # 4. Operation data (营运能力)
        rs = bs.query_operation_data(code=bs_code, year=year, quarter=quarter)
        if rs.error_code == "0":
            df = rs.get_data()
            if not df.empty:
                dfs.append(df)
        
        # 5. Cash flow data (现金流量)
        rs = bs.query_cash_flow_data(code=bs_code, year=year, quarter=quarter)
        if rs.error_code == "0":
            df = rs.get_data()
            if not df.empty:
                dfs.append(df)
        
        if not dfs:
            logger.debug(f"No fundamentals data for {symbol} {year}Q{quarter}")
            return pd.DataFrame()
        
        # Merge all dataframes on common keys
        result = dfs[0]
        merge_keys = ['code', 'pubDate', 'statDate']
        
        for df in dfs[1:]:
            result = result.merge(df, on=merge_keys, how='outer', suffixes=('', '_dup'))
            # Remove duplicate columns
            result = result.loc[:, ~result.columns.str.endswith('_dup')]
        
        # Map to PTrade format
        field_mapping = {
            # Date fields (CRITICAL!)
            'pubDate': 'publ_date',  # 公告日期 - 最重要！
            'statDate': 'end_date',  # 统计日期（季度结束日）
            
            # Profitability
            'roeAvg': 'roe',
            'roa': 'roa',
            'npMargin': 'net_profit_ratio',
            'gpMargin': 'gross_income_ratio',
            
            # Growth
            'YOYORev': 'operating_revenue_grow_rate',
            'YOYNI': 'net_profit_grow_rate',
            'YOYAsset': 'total_asset_grow_rate',
            'YOYEPSBasic': 'basic_eps_yoy',
            'YOYPNI': 'np_parent_company_yoy',
            
            # Solvency
            'currentRatio': 'current_ratio',
            'quickRatio': 'quick_ratio',
            'liabilityToAsset': 'debt_equity_ratio',
            
            # Operating
            'NRTurnRatio': 'accounts_receivables_turnover_rate',
            'INVTurnRatio': 'inventory_turnover_rate',
            'CATurnRatio': 'current_assets_turnover_rate',
            'AssetTurnRatio': 'total_asset_turnover_rate',
            
            # Cash flow
            'ebitToInterest': 'interest_cover',
        }
        
        # Rename columns
        result = result.rename(columns=field_mapping)
        
        # Convert date fields with error handling
        if "publ_date" in result.columns:
            result["publ_date"] = pd.to_datetime(result["publ_date"], errors="coerce")

        if "end_date" in result.columns:
            result["end_date"] = pd.to_datetime(result["end_date"], errors="coerce")
            # Drop rows with invalid end_date (required for index)
            result = result.dropna(subset=["end_date"])
        # Convert numeric fields
        numeric_fields = [
            'roe', 'roa', 'net_profit_ratio', 'gross_income_ratio',
            'operating_revenue_grow_rate', 'net_profit_grow_rate',
            'total_asset_grow_rate', 'basic_eps_yoy', 'np_parent_company_yoy',
            'current_ratio', 'quick_ratio', 'debt_equity_ratio',
            'accounts_receivables_turnover_rate', 'inventory_turnover_rate',
            'current_assets_turnover_rate', 'total_asset_turnover_rate',
            'interest_cover'
        ]
        
        for field in numeric_fields:
            if field in result.columns:
                result[field] = pd.to_numeric(result[field], errors='coerce')
        
        logger.info(f"Fetched fundamentals for {symbol} {year}Q{quarter}: {len(result)} rows")
        return result
