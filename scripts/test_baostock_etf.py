
import baostock as bs
import pandas as pd
from datetime import datetime

# 登录
bs.login()

# 获取最近的一个交易日
date_str = datetime.now().strftime("%Y-%m-%d")

print(f"Querying all stocks for date: {date_str}...")
rs = bs.query_all_stock(day=date_str)

if rs.error_code == "0":
    data = rs.get_data()
    print(f"Total records: {len(data)}")
    
    # Check for ETFs
    # ETF codes usually start with 51 (SH), 56 (SH), 58 (SH), 15 (SZ)
    etfs = []
    for code in data['code']:
        # code format: sh.xxxxxx
        if "." in code:
            market, symbol = code.split('.')
            if symbol.startswith('51') or symbol.startswith('56') or symbol.startswith('58') or symbol.startswith('15'):
                etfs.append(code)
    
    if etfs:
        print(f"Found {len(etfs)} potential ETFs:")
        sample_etf = etfs[0]
        print(f"Testing data download for {sample_etf}...")
        
        # Test query_history_k_data_plus with unified fields
        UNIFIED_DAILY_FIELDS = [
            "date", "open", "high", "low", "close", "volume", "amount",
            "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM", "turn", "isST", "tradestatus"
        ]
        fields_str = ",".join(UNIFIED_DAILY_FIELDS)
        
        # Query last month
        end_date = date_str
        start_date = (datetime.strptime(date_str, "%Y-%m-%d").replace(year=datetime.strptime(date_str, "%Y-%m-%d").year - 1)).strftime("%Y-%m-%d") # 1 year ago just to be safe
        
        rs = bs.query_history_k_data_plus(
            sample_etf,
            fields_str,
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"
        )
        
        if rs.error_code == "0":
            df = rs.get_data()
            if not df.empty:
                print(f"Successfully downloaded {len(df)} rows for {sample_etf}")
                print(df.head())
            else:
                print(f"Downloaded empty data for {sample_etf}")
        else:
             print(f"Error downloading data for {sample_etf}: {rs.error_msg}")
             
    else:
        print("No ETFs found in query_all_stock result.")
else:
    print(f"Error querying stocks: {rs.error_msg}")

bs.logout()
