
import logging
import sys
import pandas as pd
import baostock as bs
from datetime import datetime
from simtradedata.fetchers.unified_fetcher import UnifiedDataFetcher
from simtradedata.processors.data_splitter import DataSplitter
from simtradedata.writers.h5_writer import HDF5Writer

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("debug_etf")

def debug_etf_download(symbol="510050.SS"):
    print(f"\n=== Debugging download for {symbol} ===")
    
    # 1. 登录
    lg = bs.login()
    print(f"Login: {lg.error_msg}")
    
    try:
        # 2. 模拟下载流程
        start_date = "2024-01-01"
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        print(f"Fetching data from {start_date} to {end_date}...")
        
        fetcher = UnifiedDataFetcher()
        splitter = DataSplitter()
        writer = HDF5Writer(output_dir="debug_data")
        
        # 3. 获取数据
        unified_df = fetcher.fetch_unified_daily_data(symbol, start_date, end_date)
        
        if unified_df.empty:
            print("❌ fetch_unified_daily_data returned empty DataFrame!")
            return
            
        print(f"✅ Fetched {len(unified_df)} rows.")
        print("Columns:", unified_df.columns.tolist())
        print("Sample data:")
        print(unified_df.head(2))
        
        # 4. 检查字段是否包含估值数据
        valuation_cols = ['peTTM', 'pbMRQ']
        print("\nChecking valuation columns:")
        for col in valuation_cols:
            if col in unified_df.columns:
                print(f"{col} sample: {unified_df[col].head(2).tolist()}")
            else:
                print(f"❌ Missing {col}")
                
        # 5. 分割数据
        print("\nSplitting data...")
        split_data = splitter.split_data(unified_df)
        
        print(f"Split result keys: {list(split_data.keys())}")
        
        if 'market' in split_data:
            market_df = split_data['market']
            print(f"✅ Market data: {len(market_df)} rows")
            print("Market columns:", market_df.columns.tolist())
            
            # 6. 尝试写入
            print("\nWriting market data...")
            try:
                writer.write_market_data(symbol, market_df)
                print("✅ Write successful!")
            except Exception as e:
                print(f"❌ Write failed: {e}")
        else:
            print("❌ Market data missing in split result!")
            
    finally:
        bs.logout()
        print("\nLogged out.")

if __name__ == "__main__":
    debug_etf_download()
