"""
Mootdx Affair (financial data) fetcher implementation

This module handles batch financial data download using mootdx's Affair API.
Key advantage: one ZIP file contains all stocks' financial data for a quarter,
much more efficient than BaoStock's per-stock API approach.
"""

import logging
import tempfile
from pathlib import Path
from typing import List, Optional

import pandas as pd

from simtradedata.config.mootdx_finvalue_map import (
    CORE_FUNDAMENTAL_FIELDS,
    FINVALUE_TO_PTRADE,
    PTRADE_TO_CHINESE,
    PTRADE_TO_FINVALUE,
    parse_finvalue_date,
)

logger = logging.getLogger(__name__)


class MootdxAffairFetcher:
    """
    Fetch batch financial data via mootdx Affair API.

    This fetcher is stateless - it downloads ZIP files from TDX servers
    and parses them into DataFrames. No persistent connection needed.

    Data source: TDX gpcw (股票财务) ZIP files containing FINVALUE arrays
    for all listed stocks in a given quarter.
    """

    def __init__(self, download_dir: str | None = None):
        """
        Initialize MootdxAffairFetcher.

        Args:
            download_dir: Directory for downloading ZIP files.
                         Defaults to system temp directory.
        """
        if download_dir:
            self._download_dir = Path(download_dir)
            self._download_dir.mkdir(parents=True, exist_ok=True)
        else:
            self._download_dir = Path(tempfile.gettempdir()) / "mootdx_affair"
            self._download_dir.mkdir(parents=True, exist_ok=True)

    def list_available_reports(self) -> List[dict]:
        """
        List available financial report files on TDX server.

        Returns:
            List of dicts with keys: filename, hash, filesize
            Example: [{'filename': 'gpcw20231231.zip', 'hash': '...', 'filesize': 12345}]
        """
        from mootdx.affair import Affair

        try:
            files = Affair.files()
            if files:
                logger.info(f"Found {len(files)} available financial reports")
            return files or []
        except Exception as e:
            logger.error(f"Failed to list available reports: {e}")
            raise

    def fetch_and_parse(self, filename: str) -> pd.DataFrame:
        """
        Download and parse a financial data ZIP file.

        Affair.fetch() downloads the file and returns True/False.
        Affair.parse() reads the local file and returns a DataFrame.

        Args:
            filename: ZIP filename (e.g., 'gpcw20231231.zip')

        Returns:
            Raw DataFrame with FINVALUE array columns (0-indexed)
        """
        from mootdx.affair import Affair

        try:
            # Step 1: Download the file
            fetch_result = Affair.fetch(
                downdir=str(self._download_dir),
                filename=filename,
            )

            if not fetch_result:
                logger.warning(f"Failed to download {filename}")
                return pd.DataFrame()

            # Step 2: Parse the downloaded file
            df = Affair.parse(
                downdir=str(self._download_dir),
                filename=filename,
            )

            if df is None or not isinstance(df, pd.DataFrame) or df.empty:
                logger.warning(f"No data parsed from {filename}")
                return pd.DataFrame()

            logger.info(f"Parsed {filename}: {len(df)} rows")
            return df

        except Exception as e:
            logger.error(f"Failed to fetch and parse {filename}: {e}")
            raise

    def parse_local(self, filename: str) -> pd.DataFrame:
        """
        Parse a locally stored financial data file.

        Args:
            filename: ZIP or DAT filename in the download directory

        Returns:
            Raw DataFrame with FINVALUE array columns
        """
        from mootdx.affair import Affair

        try:
            df = Affair.parse(
                downdir=str(self._download_dir),
                filename=filename,
            )

            # Affair.parse may return False or None on failure
            if df is None or not isinstance(df, pd.DataFrame):
                return pd.DataFrame()

            if df.empty:
                return pd.DataFrame()

            return df

        except Exception as e:
            logger.error(f"Failed to parse local file {filename}: {e}")
            raise

    def fetch_fundamentals_for_quarter(
        self,
        year: int,
        quarter: int,
        fields: List[str] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch all stocks' financial data for a given quarter.

        This downloads the corresponding gpcw ZIP file and parses it
        into PTrade-compatible format.

        Args:
            year: Year (e.g., 2024)
            quarter: Quarter (1-4)
            fields: List of PTrade field names to include.
                   Defaults to CORE_FUNDAMENTAL_FIELDS.

        Returns:
            DataFrame indexed by stock code, with PTrade field names as columns.
            Includes 'end_date' and 'publ_date' columns.
        """
        # Determine quarter end date -> filename
        quarter_end = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}
        mmdd = quarter_end.get(quarter)
        if not mmdd:
            raise ValueError(f"Invalid quarter: {quarter}")

        filename = f"gpcw{year}{mmdd}.zip"

        # Download and parse
        raw_df = self.fetch_and_parse(filename)
        if raw_df.empty:
            logger.warning(f"No data for {year}Q{quarter}")
            return pd.DataFrame()

        return self._convert_to_ptrade_format(raw_df, fields)

    def _convert_to_ptrade_format(
        self,
        raw_df: pd.DataFrame,
        fields: List[str] | None = None,
    ) -> pd.DataFrame:
        """
        Convert raw FINVALUE DataFrame to PTrade-compatible format.

        Affair.parse() returns a DataFrame where:
        - Index = stock code (e.g., '000001')
        - Columns = Chinese field names, position matches FINVALUE index
        - Column 0 = 'report_date', Column 1 = '基本每股收益', etc.
        - Some column names are duplicated, so we must use iloc by position

        Args:
            raw_df: Raw DataFrame from Affair.parse()
            fields: Fields to include (PTrade names). None = CORE_FUNDAMENTAL_FIELDS

        Returns:
            DataFrame with PTrade field names
        """
        target_fields = fields or CORE_FUNDAMENTAL_FIELDS
        num_columns = len(raw_df.columns)
        raw_columns = [str(c) for c in raw_df.columns]

        result_data = {}
        
        # Build map of column name -> list of indices
        # This handles duplicate column names by keeping track of all positions
        col_indices = {}
        for i, col in enumerate(raw_columns):
            if col not in col_indices:
                col_indices[col] = []
            col_indices[col].append(i)
        
        def find_col_index_by_name(chinese_name):
            # 1. Try exact match
            if chinese_name in col_indices:
                return col_indices[chinese_name][0] # Return first match
            
            # 2. Try partial match ("ID | Name" format)
            for col, indices in col_indices.items():
                if chinese_name in col:
                    parts = col.split("|")
                    name_part = parts[-1].strip()
                    if chinese_name == name_part:
                        return indices[0]
            
            # 3. Try looser partial match
            for col, indices in col_indices.items():
                if chinese_name in col:
                     return indices[0]
            
            return None

        # 1. Process target fields
        for field in target_fields:
            found = False
            # Try Chinese name match first (more robust)
            if field in PTRADE_TO_CHINESE:
                chinese_name = PTRADE_TO_CHINESE[field]
                idx = find_col_index_by_name(chinese_name)
                if idx is not None:
                    result_data[field] = raw_df.iloc[:, idx].values
                    found = True
            
            # Fallback to index-based mapping
            if not found and field in PTRADE_TO_FINVALUE:
                idx = PTRADE_TO_FINVALUE[field]
                if idx < num_columns:
                    result_data[field] = raw_df.iloc[:, idx].values

        # 2. Process special fields (dates) always needed if not already present
        if "_report_date_raw" not in result_data:
             if num_columns > 0:
                 result_data["_report_date_raw"] = raw_df.iloc[:, 0].values

        if "_publ_date_raw" not in result_data:
             idx = find_col_index_by_name("财报公告日期")
             if idx is not None:
                 result_data["_publ_date_raw"] = raw_df.iloc[:, idx].values
             elif 314 in FINVALUE_TO_PTRADE and PTRADE_TO_FINVALUE.get("_publ_date_raw") == 314:
                 if 314 < num_columns:
                     result_data["_publ_date_raw"] = raw_df.iloc[:, 314].values

        if not result_data:
            logger.warning("No matching columns found in raw data")
            return pd.DataFrame()

        try:
            result = pd.DataFrame(result_data)
        except ValueError as e:
            logger.error(f"Failed to create DataFrame: {e}")
            # Debug column shapes
            for k, v in result_data.items():
                logger.error(f"Field {k} shape: {getattr(v, 'shape', 'unknown')}")
            return pd.DataFrame()

        # Preserve stock code
        # Logic to find code column: 'code', '代码', index, or column 0
        code_values = None
        if "code" in raw_df.columns:
            # Handle duplicate 'code' columns safely
            code_col = raw_df["code"]
            if isinstance(code_col, pd.DataFrame):
                code_values = code_col.iloc[:, 0].values
            else:
                code_values = code_col.values
        elif "代码" in raw_df.columns:
            code_col = raw_df["代码"]
            if isinstance(code_col, pd.DataFrame):
                code_values = code_col.iloc[:, 0].values
            else:
                code_values = code_col.values
        elif raw_df.index.name in ["code", "代码"]:
            code_values = raw_df.index.values
        else:
            # Fallback to column 0
            code_values = raw_df.iloc[:, 0].values
            
        result["code"] = [str(x).zfill(6) for x in code_values]
        result.set_index("code", inplace=True)

        # Parse report date (YYMMDD format)
        if "_report_date_raw" in result.columns:
            result["end_date"] = result["_report_date_raw"].apply(parse_finvalue_date)
            result["end_date"] = pd.to_datetime(result["end_date"], errors="coerce")
            result = result.drop(columns=["_report_date_raw"])

        # Parse publication date
        if "_publ_date_raw" in result.columns:
            result["publ_date"] = result["_publ_date_raw"].apply(parse_finvalue_date)
            result["publ_date"] = pd.to_datetime(result["publ_date"], errors="coerce")
            result = result.drop(columns=["_publ_date_raw"])

        # Convert numeric fields
        numeric_cols = [c for c in result.columns if c not in ("end_date", "publ_date", "code")]
        for col in numeric_cols:
            result[col] = pd.to_numeric(result[col], errors="coerce")

        logger.info(f"Converted to PTrade format: {len(result)} rows, {len(result.columns)} columns")
        return result

    def get_quarter_filename(self, year: int, quarter: int) -> str:
        """
        Get the expected filename for a given quarter.

        Args:
            year: Year
            quarter: Quarter (1-4)

        Returns:
            Filename string (e.g., 'gpcw20231231.zip')
        """
        quarter_end = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}
        return f"gpcw{year}{quarter_end[quarter]}.zip"

    def get_remote_file_hash(self, year: int, quarter: int) -> Optional[str]:
        """
        Get hash value of a quarter's financial data file from TDX server.

        Uses list_available_reports() to get file metadata including hash.

        Args:
            year: Year (e.g., 2024)
            quarter: Quarter (1-4)

        Returns:
            Hash string if file exists on server, None otherwise
        """
        filename = self.get_quarter_filename(year, quarter)

        try:
            files = self.list_available_reports()
            for file_info in files:
                if file_info.get("filename") == filename:
                    return file_info.get("hash")
            return None
        except Exception as e:
            logger.warning(f"Failed to get remote hash for {filename}: {e}")
            return None
