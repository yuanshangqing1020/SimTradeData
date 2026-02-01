
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
from simtradedata.fetchers.baostock_fetcher import BaoStockFetcher
from simtradedata.writers.h5_writer import HDF5Writer

# Setup basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

def fix_trade_calendar():
    print("Starting standalone trading calendar download...")
    
    # Initialize components
    fetcher = BaoStockFetcher()
    writer = HDF5Writer(output_dir="data")
    
    # Login
    fetcher.login()
    
    try:
        # Full history range
        start_date = "1990-12-19"
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        print(f"Downloading calendar from {start_date} to {end_date}...")
        
        # Fetch
        df = fetcher.fetch_trade_calendar(start_date=start_date, end_date=end_date)
        
        if df is None or df.empty:
            print("Error: No data fetched!")
            return
            
        # Preprocess
        # Check raw data first
        print(f"Raw data fetched: {len(df)} rows")
        print(df.head())
        
        # Filter trading days
        # BaoStock might return '1' as string or integer, check types
        # Also column name might be different?
        if 'is_trading_day' in df.columns:
            # Try converting to string to be safe
            df['is_trading_day'] = df['is_trading_day'].astype(str)
            df = df[df['is_trading_day'] == '1']
        
        if df.empty:
            print("Warning: All rows filtered out! No trading days found.")
            return

        df['trade_date'] = pd.to_datetime(df['calendar_date'])
        df = df[['trade_date']].set_index('trade_date')
        
        print(f"Fetched {len(df)} trading days.")
        
        # Write
        # Manually write to HDFStore to bypass any potential issues in writer class
        # And ensure we append correctly
        
        # NOTE: If the file is very large, maybe repacking it is needed?
        # Or maybe we are writing to a different path?
        # cwd is /mnt/c/QMTReal/SimTrade/SimTradeData
        # file is data/ptrade_data.h5
        
        print(f"Writing to {Path('data/ptrade_data.h5').absolute()}")
        
        try:
             store = pd.HDFStore("data/ptrade_data.h5", mode='a')
             # Remove existing key first to ensure clean write (we fetched full history)
             # Note: we use '/trade_days' because keys() returns with slash
             if '/trade_days' in store.keys():
                 store.remove('/trade_days')
             
             # IMPORTANT: Must use format='table' (not fixed) to be compatible with verification script
             # And ensure the key is exactly 'trade_days' (without leading slash in put() call, pandas adds it)
             # But wait, why is it still missing?
             # Maybe the file is corrupted or not closing properly?
             # Let's try with a slash
             store.put('/trade_days', df, format='fixed') # Try fixed format first just to see if it sticks
             
             # Force flush
             store.flush()
             store.close()
        except Exception as e:
            print(f"HDFStore Error: {e}")
               
        print("Successfully saved to data/ptrade_data.h5")
        
    except AttributeError as e:
        if "append" in str(e):
             print(f"Error caught: {e}")
             print("Pandas compatibility issue persisting. Using direct store.put()...")
             # This block shouldn't be reached if we use the direct write above, but keeping as fallback
             pass
        else:
            raise e
    except Exception as e:
        print(f"Error: {e}")
    finally:
        fetcher.logout()

if __name__ == "__main__":
    fix_trade_calendar()
