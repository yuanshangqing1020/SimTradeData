"""
HDF5 writer for PTrade-compatible format
"""

import logging
import warnings
from pathlib import Path
from typing import Dict, List

import pandas as pd
from tables import NaturalNameWarning

from simtradedata.validators import validate_before_write

warnings.filterwarnings("ignore", category=NaturalNameWarning)

logger = logging.getLogger(__name__)


class HDF5Writer:
    """
    Writer for HDF5 files in PTrade format

    Note: Thread locks removed as download_efficient.py uses sequential processing.
    BaoStock does not support concurrent access, so locks are unnecessary overhead.
    """

    # HDF5 storage format constants
    # "table": Supports queries, slower, good for time-series data
    # "fixed": Fast but no queries, good for metadata
    FORMAT_TABLE = "table"
    FORMAT_FIXED = "fixed"

    # Compression settings
    COMPRESSION_LEVEL = 9
    COMPRESSION_LIB = "blosc"

    def __init__(self, output_dir: str = "."):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # File paths
        self.ptrade_data_path = self.output_dir / "ptrade_data.h5"
        self.ptrade_fundamentals_path = self.output_dir / "ptrade_fundamentals.h5"
        self.ptrade_adj_pre_path = self.output_dir / "ptrade_adj_pre.h5"
        self.ptrade_dividend_cache_path = self.output_dir / "ptrade_dividend_cache.h5"

        logger.info(f"HDF5Writer initialized with output_dir: {self.output_dir}")

    def _ensure_datetime_index(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure DataFrame has DatetimeIndex

        Args:
            data: DataFrame to check

        Returns:
            DataFrame with DatetimeIndex
        """
        if not isinstance(data.index, pd.DatetimeIndex):
            data.index = pd.to_datetime(data.index)
        return data

    def _get_compression_kwargs(self) -> dict:
        """
        Get standard compression kwargs for HDF5 storage

        Returns:
            Dict with complevel and complib
        """
        return {
            'complevel': self.COMPRESSION_LEVEL,
            'complib': self.COMPRESSION_LIB
        }

    def write_market_data(
        self, symbol: str, data: pd.DataFrame, mode: str = "a"
    ) -> None:
        """
        Write market data to ptrade_data.h5/stock_data/{symbol}

        Args:
            symbol: Stock code in PTrade format (e.g., '000001.SZ')
            data: DataFrame with columns [open, high, low, close, volume, money]
            mode: 'a' for append, 'w' for overwrite
        """
        if data.empty:
            logger.info(f"No data to write for {symbol}")
            return

        # Validate data quality before writing
        try:
            validate_before_write(data, "market", symbol, strict=False)
        except Exception as e:
            logger.error(f"Validation failed for {symbol} market data: {e}")

        # Ensure datetime index
        data = self._ensure_datetime_index(data)

        key = f"stock_data/{symbol}"

        with pd.HDFStore(self.ptrade_data_path, mode=mode) as store:
            store.put(
                key,
                data,
                format=self.FORMAT_TABLE,
                **self._get_compression_kwargs()
            )

        logger.info(
            f"Wrote market data for {symbol} to {self.ptrade_data_path}: "
            f"{len(data)} rows"
        )

    def write_benchmark(self, data: pd.DataFrame, mode: str = "a") -> None:
        """
        Write benchmark index data to ptrade_data.h5/benchmark

        Args:
            data: DataFrame with columns [open, high, low, close, volume, money]
            mode: 'a' for append, 'w' for overwrite
        """
        if data.empty:
            logger.info("No benchmark data to write")
            return

        # Validate data quality before writing
        try:
            validate_before_write(data, "market", "benchmark", strict=False)
        except Exception as e:
            logger.error(f"Validation failed for benchmark data: {e}")

        data = self._ensure_datetime_index(data)

        with pd.HDFStore(self.ptrade_data_path, mode=mode) as store:
            store.put(
                "benchmark",
                data,
                format=self.FORMAT_TABLE,
                data_columns=True,
                **self._get_compression_kwargs()
            )

        logger.info(
            f"Wrote benchmark data to {self.ptrade_data_path}: {len(data)} rows"
        )

    def write_metadata(
        self,
        start_date: str,
        end_date: str,
        stock_count: int,
        mode: str = "a",
    ) -> None:
        """
        Write metadata Series to ptrade_data.h5/metadata

        The metadata contains essential information about the dataset,
        including download date, date range, stock count, etc.

        Args:
            start_date: Data start date (YYYY-MM-DD)
            end_date: Data end date (YYYY-MM-DD)
            stock_count: Number of stocks in the dataset
            mode: 'a' for append, 'w' for overwrite
        """
        import json
        from datetime import datetime

        # Create metadata Series matching simtradelab format
        metadata = pd.Series(
            {
                "download_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "start_date": start_date,
                "end_date": end_date,
                "stock_count": stock_count,
                "sample_count": 0,  # Will be updated if needed
                "format_version": 3,
                "index_constituents": json.dumps({}),  # Empty dict as placeholder
                "stock_status_history": json.dumps({}),  # Empty dict as placeholder
            }
        )

        with pd.HDFStore(self.ptrade_data_path, mode=mode) as store:
            store.put(
                "metadata",
                metadata,
                format=self.FORMAT_FIXED,
            )

        logger.info(
            f"Wrote metadata to {self.ptrade_data_path}: "
            f"start={start_date}, end={end_date}, stocks={stock_count}"
        )

    def write_exrights(self, symbol: str, data: pd.DataFrame, mode: str = "a") -> None:
        """
        Write exrights data to ptrade_data.h5/exrights/{symbol}

        Args:
            symbol: Stock code in PTrade format
            data: DataFrame with exrights fields
            mode: 'a' for append, 'w' for overwrite
        """
        if data.empty:
            logger.info(f"No data to write for {symbol}")
            return

        data = self._ensure_datetime_index(data)

        key = f"exrights/{symbol}"

        with pd.HDFStore(self.ptrade_data_path, mode=mode) as store:
            store.put(
                key,
                data,
                format=self.FORMAT_TABLE,
                **self._get_compression_kwargs()
            )

        logger.info(
            f"Wrote exrights data for {symbol} to {self.ptrade_data_path}: "
            f"{len(data)} rows"
        )

    def write_stock_metadata(self, metadata_df: pd.DataFrame, mode: str = "a") -> None:
        """
        Write stock metadata to ptrade_data.h5/stock_metadata

        Args:
            metadata_df: DataFrame with columns [blocks, de_listed_date, has_info, listed_date, stock_name]
                        and stock_code as index
            mode: 'a' for append, 'w' for overwrite
        """
        if metadata_df.empty:
            logger.info("No data to write")
            return

        # Convert all columns to string to avoid PyTables mixed-type warning
        metadata_clean = metadata_df.copy()
        for col in metadata_clean.columns:
            metadata_clean[col] = metadata_clean[col].astype(str)

        with pd.HDFStore(self.ptrade_data_path, mode=mode) as store:
            store.put(
                "stock_metadata",
                metadata_clean,
                format=self.FORMAT_TABLE,
                **self._get_compression_kwargs()
            )

        logger.info(
            f"Wrote stock metadata to {self.ptrade_data_path}: "
            f"{len(metadata_df)} stocks"
        )

    def write_fundamentals(
        self, symbol: str, data: pd.DataFrame, mode: str = "a"
    ) -> None:
        """
        Write fundamental data to ptrade_fundamentals.h5/fundamentals/{symbol}

        Args:
            symbol: Stock code in PTrade format
            data: DataFrame with 23 fundamental indicators
            mode: 'a' for append, 'w' for overwrite
        """
        if data.empty:
            logger.info(f"No data to write for {symbol}")
            return

        # Ensure end_date is the index
        if "end_date" in data.columns:
            data = data.set_index("end_date")
        data = self._ensure_datetime_index(data)

        key = f"fundamentals/{symbol}"

        with pd.HDFStore(self.ptrade_fundamentals_path, mode=mode) as store:
            store.put(
                key,
                data,
                format=self.FORMAT_TABLE,  # Changed from fixed to support compression
                **self._get_compression_kwargs()
            )

        logger.info(
            f"Wrote fundamentals for {symbol} to {self.ptrade_fundamentals_path}: "
            f"{len(data)} quarters"
        )

    def write_valuation(self, symbol: str, data: pd.DataFrame, mode: str = "a") -> None:
        """
        Write valuation data to ptrade_fundamentals.h5/valuation/{symbol}

        Args:
            symbol: Stock code in PTrade format
            data: DataFrame with valuation indicators
            mode: 'a' for append, 'w' for overwrite
        """
        if data.empty:
            logger.info(f"No data to write for {symbol}")
            return

        data = self._ensure_datetime_index(data)

        key = f"valuation/{symbol}"

        with pd.HDFStore(self.ptrade_fundamentals_path, mode=mode) as store:
            store.put(
                key,
                data,
                format=self.FORMAT_TABLE,
                **self._get_compression_kwargs()
            )

        logger.info(
            f"Wrote valuation for {symbol} to {self.ptrade_fundamentals_path}: "
            f"{len(data)} days"
        )

    def write_adjust_factor(
        self, symbol: str, data: pd.Series, mode: str = "a"
    ) -> None:
        """
        Write adjust factor to ptrade_adj_pre.h5/{symbol}

        Args:
            symbol: Stock code in PTrade format
            data: Series with backward adjust factor
            mode: 'a' for append, 'w' for overwrite
        """
        if data.empty:
            logger.info(f"No data to write for {symbol}")
            return

        data = self._ensure_datetime_index(data)

        # Ensure Series name is 'backward_a'
        data.name = "backward_a"

        with pd.HDFStore(self.ptrade_adj_pre_path, mode=mode) as store:
            store.put(
                symbol,
                data,
                format=self.FORMAT_TABLE,
                **self._get_compression_kwargs()
            )

        logger.info(
            f"Wrote adjust factor for {symbol} to {self.ptrade_adj_pre_path}: "
            f"{len(data)} days"
        )

    def write_trade_days(self, trade_days_df: pd.DataFrame, mode: str = "a") -> None:
        """
        Write trading days to ptrade_data.h5/trade_days

        Args:
            trade_days_df: DataFrame with trading dates
            mode: 'a' for append, 'w' for overwrite
        """
        if trade_days_df.empty:
            logger.warning("No trading days to write")
            return

        with pd.HDFStore(self.ptrade_data_path, mode=mode) as store:
            store.put(
                "trade_days",
                trade_days_df,
                format=self.FORMAT_TABLE,
                **self._get_compression_kwargs()
            )
        logger.info(
            f"Wrote {len(trade_days_df)} trading days to {self.ptrade_data_path}"
        )

    def write_global_metadata(self, metadata: pd.Series, mode: str = "a") -> None:
        """
        Write global metadata to ptrade_data.h5/metadata

        Args:
            metadata: Series containing global dataset info
            mode: 'a' for append, 'w' for overwrite
        """
        if metadata.empty:
            logger.info("No data to write")
            return

        with pd.HDFStore(self.ptrade_data_path, mode=mode) as store:
            store.put(
                "metadata",
                metadata,
                format=self.FORMAT_FIXED,
            )
        logger.info(f"Wrote global metadata to {self.ptrade_data_path}")

    def merge_and_write_global_data(
        self,
        key: str,
        new_data: pd.DataFrame,
        write_method: callable
    ) -> None:
        """
        Merge new global data with existing data and write to HDF5

        This method handles the common pattern of reading existing data,
        merging with new data, and writing back to avoid data loss.

        Args:
            key: HDF5 key (with leading slash, e.g., '/benchmark')
            new_data: New DataFrame to merge
            write_method: Method to call for writing (e.g., self.write_benchmark)
        """
        if new_data.empty:
            logger.info(f"No new data to merge for {key}")
            return

        merged_data = new_data.copy()

        # Try to read existing data
        try:
            with pd.HDFStore(self.ptrade_data_path, mode='r') as store:
                if key in store.keys():
                    existing_data = store[key]
                    # Combine and remove duplicates (keep='last' means new data overwrites old)
                    merged_data = pd.concat([existing_data, new_data])
                    merged_data = merged_data[~merged_data.index.duplicated(keep='last')]
                    merged_data = merged_data.sort_index()
                    logger.info(
                        f"Merged {key}: {len(existing_data)} existing + "
                        f"{len(new_data)} new = {len(merged_data)} total"
                    )
        except FileNotFoundError:
            logger.info(f"No existing file, will create new data for {key}")
        except KeyError:
            logger.info(f"No existing data for {key}, will create new")

        # Write merged data
        # IMPORTANT: Use mode='a' to preserve other keys in the file
        # mode='w' would truncate the entire file!
        write_method(merged_data, mode='a')

    def get_existing_stocks(self, file_type: str = "market") -> List[str]:
        """
        Get list of stocks already in HDF5 file

        Args:
            file_type: 'market', 'fundamentals', or 'adjust'

        Returns:
            List of stock codes
        """
        file_map = {
            "market": self.ptrade_data_path,
            "fundamentals": self.ptrade_fundamentals_path,
            "adjust": self.ptrade_adj_pre_path,
        }

        filepath = file_map.get(file_type)
        if not filepath or not filepath.exists():
            return []

        try:
            with pd.HDFStore(filepath, mode="r") as store:
                keys = store.keys()

                if file_type == "market":
                    # Extract stock codes from /stock_data/* keys
                    stocks = [
                        k.split("/")[-1] for k in keys if k.startswith("/stock_data/")
                    ]
                elif file_type == "fundamentals":
                    # Extract from /fundamentals/* or /valuation/* keys
                    stocks = [
                        k.split("/")[-1]
                        for k in keys
                        if k.startswith("/fundamentals/") or k.startswith("/valuation/")
                    ]
                else:
                    # For adjust factor, keys are stock codes directly
                    stocks = [k.lstrip("/") for k in keys]

                return list(set(stocks))
        except Exception as e:
            logger.error(f"Error reading existing stocks from {filepath}: {e}")
            return []

    def check_file_integrity(self, file_type: str = "market") -> bool:
        """
        Check if HDF5 file is valid and readable

        Args:
            file_type: 'market', 'fundamentals', or 'adjust'

        Returns:
            True if file is valid, False otherwise
        """
        file_map = {
            "market": self.ptrade_data_path,
            "fundamentals": self.ptrade_fundamentals_path,
            "adjust": self.ptrade_adj_pre_path,
        }

        filepath = file_map.get(file_type)
        if not filepath or not filepath.exists():
            return False

        try:
            with pd.HDFStore(filepath, mode="r") as store:
                keys = store.keys()
                return len(keys) > 0
        except Exception as e:
            logger.error(f"File integrity check failed for {filepath}: {e}")
            return False
