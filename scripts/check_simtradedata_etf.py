
import pandas as pd
import os
import glob

# 指定要检查的目录
data_dir = '/mnt/c/QMTReal/SimTrade/SimTradeData/data/'
h5_files = glob.glob(os.path.join(data_dir, '*.h5'))

print(f"Checking H5 files in: {data_dir}")
print(f"Found files: {[os.path.basename(f) for f in h5_files]}")

for file_path in h5_files:
    file_name = os.path.basename(file_path)
    print(f"\n" + "="*50)
    print(f"Checking {file_name}...")
    
    try:
        with pd.HDFStore(file_path, mode='r') as store:
            keys = store.keys()
            print(f"Total keys: {len(keys)}")
            
            etf_count = 0
            etf_examples = []
            
            # 遍历所有 key
            for key in keys:
                # key 格式通常为 /stock_data/000001.SS 或 /000001.SS
                parts = key.split('/')
                if not parts: continue
                
                # 获取最后一部分作为代码（包含后缀）
                code_with_suffix = parts[-1]
                
                # 去掉 .SS/.SZ 后缀获取纯数字代码
                if '.' in code_with_suffix:
                    code = code_with_suffix.split('.')[0]
                else:
                    code = code_with_suffix
                
                # ETF 代码判断规则：
                # 沪市 ETF：51xxxx, 56xxxx, 58xxxx
                # 深市 ETF：15xxxx
                if code.startswith('51') or code.startswith('56') or code.startswith('58') or code.startswith('15'):
                    etf_count += 1
                    if len(etf_examples) < 10:
                        etf_examples.append(code_with_suffix)
            
            if etf_count > 0:
                print(f"✅ Found {etf_count} ETF records!")
                print(f"Examples: {etf_examples}")
            else:
                print("❌ No ETF records found in keys.")
            
            # 额外检查元数据 (stock_metadata)
            if '/stock_metadata' in keys:
                print("\nChecking stock_metadata...")
                try:
                    metadata = store['stock_metadata']
                    # 检查 index 是否包含 ETF
                    meta_etf_count = 0
                    meta_etf_examples = []
                    
                    for idx in metadata.index:
                        idx_str = str(idx)
                        code = idx_str.split('.')[0]
                        if code.startswith('51') or code.startswith('56') or code.startswith('58') or code.startswith('15'):
                            meta_etf_count += 1
                            if len(meta_etf_examples) < 10:
                                meta_etf_examples.append(idx_str)
                    
                    if meta_etf_count > 0:
                        print(f"✅ Found {meta_etf_count} ETFs in metadata index!")
                        print(f"Examples: {meta_etf_examples}")
                    else:
                        print("❌ No ETFs found in metadata index.")
                        
                except Exception as e:
                    print(f"Error reading stock_metadata: {e}")

    except Exception as e:
        print(f"Error reading {file_name}: {e}")
