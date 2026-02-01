import pandas as pd
import numpy as np
import os
import json
import logging
from datetime import datetime
from tqdm import tqdm
from pathlib import Path
import warnings

# Suppress PyTables warnings
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)

# Configuration
DATA_DIR = Path("data")
MARKET_DATA_FILE = DATA_DIR / "ptrade_data.h5"
FUNDAMENTAL_FILE = DATA_DIR / "ptrade_fundamentals.h5"
ADJ_FACTOR_FILE = DATA_DIR / "ptrade_adj_pre.h5"
REPORT_FILE = DATA_DIR / "data_verification_report.json"
LOG_FILE = DATA_DIR / "data_verification.log"

# Logging setup
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)
logger = logging.getLogger(__name__)

class DataVerifier:
    def __init__(self):
        self.report = {
            "summary": {
                "total_stocks": 0,
                "fully_complete": 0,
                "with_missing_days": 0,
                "missing_market_data": 0,
                "missing_fundamentals": 0,
                "missing_adj_factor": 0,
                "total_missing_days": 0
            },
            "details": {
                "missing_market_data": [],
                "missing_fundamentals": [],
                "missing_adj_factor": [],
                "gaps": {}  # symbol: [missing_dates]
            }
        }
        self.trade_days = None
        self.stock_metadata = None

    def load_global_data(self):
        """Load trading calendar and stock metadata"""
        logger.info("Loading global data...")
        try:
            with pd.HDFStore(MARKET_DATA_FILE, mode='r') as store:
                # Load Trading Days
                if '/trade_days' in store:
                    self.trade_days = store['trade_days']
                    # Ensure index is datetime
                    if not isinstance(self.trade_days.index, pd.DatetimeIndex):
                        self.trade_days.index = pd.to_datetime(self.trade_days.index)
                    logger.info(f"Loaded trading calendar: {len(self.trade_days)} days")
                elif 'trade_days' in store: # Try without slash
                    self.trade_days = store['trade_days']
                    # Ensure index is datetime
                    if not isinstance(self.trade_days.index, pd.DatetimeIndex):
                        self.trade_days.index = pd.to_datetime(self.trade_days.index)
                    logger.info(f"Loaded trading calendar: {len(self.trade_days)} days")
                else:
                    logger.warning("Warning: /trade_days not found in market data file! Skipping gap analysis.")
                    self.report["summary"]["warnings"] = ["Trading calendar (/trade_days) is missing. Cannot perform gap analysis."]

                # Load Stock Metadata
                if '/stock_metadata' in store:
                    self.stock_metadata = store['stock_metadata']
                    logger.info(f"Loaded metadata for {len(self.stock_metadata)} stocks")
                else:
                    logger.error("Critical: /stock_metadata not found in market data file!")
                    return False
            return True
        except Exception as e:
            logger.error(f"Failed to load global data: {e}")
            return False

    def verify_stocks(self):
        """Verify data for each stock"""
        if self.stock_metadata is None:
            return

        stocks = self.stock_metadata.index.tolist()
        self.report["summary"]["total_stocks"] = len(stocks)

        logger.info("Opening data files for verification...")
        try:
            store_market = pd.HDFStore(MARKET_DATA_FILE, mode='r')
            store_fund = pd.HDFStore(FUNDAMENTAL_FILE, mode='r') if FUNDAMENTAL_FILE.exists() else None
            store_adj = pd.HDFStore(ADJ_FACTOR_FILE, mode='r') if ADJ_FACTOR_FILE.exists() else None
            
            logger.info("Verifying stocks...")
            
            # Cache keys for faster existence check
            market_keys = set(store_market.keys())
            fund_keys = set(store_fund.keys()) if store_fund else set()
            adj_keys = set(store_adj.keys()) if store_adj else set()

            for symbol in tqdm(stocks, desc="Verifying"):
                issues_found = False
                
                # 1. Market Data Check
                market_key = f'/stock_data/{symbol}'
                if market_key not in market_keys:
                    self.report["details"]["missing_market_data"].append(symbol)
                    self.report["summary"]["missing_market_data"] += 1
                    issues_found = True
                    continue # Cannot check gaps if no data
                else:
                    # Gap Analysis
                    if self.trade_days is not None:
                        try:
                            # Get IPO and Delist dates
                            meta = self.stock_metadata.loc[symbol]
                            
                            # Handle different field names (ipoDate vs listed_date)
                            if 'listed_date' in meta:
                                ipo_str = meta['listed_date']
                            elif 'ipoDate' in meta:
                                ipo_str = meta['ipoDate']
                            else:
                                ipo_str = None
                                
                            if 'de_listed_date' in meta:
                                out_str = meta['de_listed_date']
                            elif 'outDate' in meta:
                                out_str = meta['outDate']
                            else:
                                out_str = None
                            
                            # Validating date strings: must be non-empty and not '0' or 'None' or just whitespace
                            def parse_date(date_val, default_val):
                                if pd.isna(date_val) or str(date_val).strip() == '' or str(date_val) == '0' or str(date_val).lower() == 'none':
                                    return default_val
                                try:
                                    return pd.to_datetime(date_val)
                                except:
                                    return default_val

                            ipo_date = parse_date(ipo_str, None)
                            out_date = parse_date(out_str, pd.Timestamp.max)
                            
                            start_bound = self.trade_days.index.min()
                            end_bound = self.trade_days.index.max()
                            
                            has_valid_ipo = ipo_date is not None
                            
                            # Current check range
                            if has_valid_ipo:
                                check_start = max(ipo_date, start_bound)
                            else:
                                check_start = None
                                
                            check_end = min(out_date, end_bound)
                            
                            # If the stock has a specific delist date, it usually stops trading BEFORE or ON that date.
                            # The delist date itself is typically not a trading day (or at least not one we expect data for if it's the removal date).
                            # To avoid false positives on the delist date, we exclude it from the expected range.
                            if out_date != pd.Timestamp.max and check_end == out_date:
                                check_end = check_end - pd.Timedelta(days=1)
                            
                            if check_start is not None and check_start > check_end:
                                # Stock not listed in current calendar range
                                pass 
                            else:
                                # OPTIMIZATION: Try to read only the index to reduce I/O
                                try:
                                    # Try to use select_column if format allows (Table format)
                                    # This is much faster than reading the whole dataframe
                                    storer = store_market.get_storer(market_key)
                                    if storer.is_table:
                                        df_dates = pd.Index(store_market.select_column(market_key, 'index'))
                                        if not isinstance(df_dates, pd.DatetimeIndex):
                                            df_dates = pd.to_datetime(df_dates)
                                    else:
                                        # Fallback for Fixed format
                                        df = store_market.get(market_key)
                                        df_dates = df.index
                                except Exception:
                                    # Fallback if optimization fails
                                    df = store_market.get(market_key)
                                    df_dates = df.index
                                
                                if len(df_dates) == 0:
                                    self.report["details"]["missing_market_data"].append(symbol + " (Empty)")
                                    issues_found = True
                                else:
                                    # Refine check_start if it was None (missing IPO date)
                                    actual_start = df_dates.min()
                                    if check_start is None:
                                        # Assume data starts correctly if metadata is missing
                                        check_start = max(actual_start, start_bound)
                                    
                                    # Ensure check_start is not after check_end
                                    check_start = min(check_start, check_end)
                                    
                                    # Expected dates
                                    expected_dates = self.trade_days.loc[check_start:check_end].index
                                    
                                    # Find missing
                                    missing = expected_dates.difference(df_dates)
                                    
                                    if not missing.empty:
                                        # Ignore very recent dates (might not be updated yet today)
                                        missing_list = [d.strftime('%Y-%m-%d') for d in missing]
                                        self.report["details"]["gaps"][symbol] = missing_list
                                        self.report["summary"]["with_missing_days"] += 1
                                        self.report["summary"]["total_missing_days"] += len(missing)
                                        issues_found = True

                        except Exception as e:
                            logger.warning(f"Error checking market data for {symbol}: {e}")
                            issues_found = True

                # 2. Fundamentals Check
                fund_key = f'/valuation/{symbol}' # Assuming valuation is the main one
                if fund_key not in fund_keys:
                    self.report["details"]["missing_fundamentals"].append(symbol)
                    self.report["summary"]["missing_fundamentals"] += 1
                    issues_found = True

                # 3. Adj Factor Check
                adj_key_simple = f'/{symbol}'
                adj_key_prefix = f'/adjust_factor/{symbol}'
                
                if adj_key_simple in adj_keys:
                    pass
                elif adj_key_prefix in adj_keys:
                    pass
                else:
                    self.report["details"]["missing_adj_factor"].append(symbol)
                    self.report["summary"]["missing_adj_factor"] += 1
                    issues_found = True

                if not issues_found:
                    self.report["summary"]["fully_complete"] += 1

        except Exception as e:
            logger.error(f"Verification process failed: {e}")
        finally:
            if store_market: store_market.close()
            if store_fund: store_fund.close()
            if store_adj: store_adj.close()

    def generate_report(self):
        """Generate final report"""
        # Calculate completion rate
        total = self.report["summary"]["total_stocks"]
        if total > 0:
            rate = (self.report["summary"]["fully_complete"] / total) * 100
        else:
            rate = 0
            
        print("\n" + "=" * 60)
        print("DATA VERIFICATION REPORT")
        print("=" * 60)
        print(f"Total Stocks Checked: {total}")
        print(f"Fully Complete:       {self.report['summary']['fully_complete']} ({rate:.1f}%)")
        print("-" * 60)
        print("MISSING DATA SUMMARY:")
        print(f"  Missing Market Data:   {self.report['summary']['missing_market_data']}")
        print(f"  Missing Fundamentals:  {self.report['summary']['missing_fundamentals']}")
        print(f"  Missing Adjust Factor: {self.report['summary']['missing_adj_factor']}")
        print(f"  Stocks with Date Gaps: {self.report['summary']['with_missing_days']}")
        print(f"  Total Missing Days:    {self.report['summary']['total_missing_days']}")
        print("-" * 60)
        
        # Print Missing Data Details (if not too many)
        if self.report['details']['missing_market_data']:
            print(f"Missing Market Data ({len(self.report['details']['missing_market_data'])}):")
            print(f"  {self.report['details']['missing_market_data'][:20]} ...")
            
        if self.report['details']['missing_fundamentals']:
            print(f"Missing Fundamentals ({len(self.report['details']['missing_fundamentals'])}):")
            # Usually indices don't have fundamentals, so maybe just print first few
            print(f"  {self.report['details']['missing_fundamentals'][:10]} ...")

        print("-" * 60)
        
        # Top gap offenders
        if self.report["details"]["gaps"]:
            print("TOP 5 STOCKS WITH MOST MISSING DAYS:")
            sorted_gaps = sorted(self.report["details"]["gaps"].items(), key=lambda x: len(x[1]), reverse=True)
            for sym, dates in sorted_gaps[:5]:
                print(f"  {sym}: {len(dates)} days missing")
                if len(dates) <= 3:
                    print(f"    Dates: {dates}")
                else:
                    print(f"    Range: {dates[0]} ... {dates[-1]}")

        # Save detailed JSON
        with open(REPORT_FILE, 'w') as f:
            json.dump(self.report, f, indent=2)
        print(f"\nDetailed report saved to: {REPORT_FILE}")
        print(f"Log file saved to: {LOG_FILE}")
        print("=" * 60)

if __name__ == "__main__":
    verifier = DataVerifier()
    if verifier.load_global_data():
        verifier.verify_stocks()
        verifier.generate_report()
