import pandas as pd
import numpy as np
from pathlib import Path  

def one_day(day, tbl, path):
    # 1日分の10秒間隔タイムスタンプを生成（8640個）
    start_time = pd.Timestamp(day)
    timestamps = pd.date_range(start=start_time, periods=8640, freq='10S')

    # ダミーデータ生成（例: センサー値やスコアなど）
    data = {
        '収集日時': timestamps,
        'sensor_a': np.random.normal(loc=50, scale=10, size=8640),
        'sensor_b': np.random.randint(0, 100, size=8640),
        'status': np.random.choice(['OK', 'WARN', 'FAIL'], size=8640, p=[0.8, 0.15, 0.05])
    }

    df = pd.DataFrame(data)

    # CSVとして保存
    daystr = start_time.strftime('%Y%m%d')
    df.to_csv(f'{path}/{tbl}_{daystr}.csv',encoding='utf_8_sig', index=False)


def days_one_month(year, mon):
    start = pd.Timestamp(f"{year}-{mon:02d}-01")
    end = start + pd.offsets.MonthEnd(0)

    return pd.date_range(start=start, end=end, freq="D")


year = 2025
month = 1

table_names = ['TBL01', 'TBL02', 'TEST']
base_dir = Path("output")
folder_path = base_dir / f"{year}" / f"{month:02d}"
folder_path.mkdir(parents=True, exist_ok=True)
days = days_one_month(year, month)

print(folder_path)

for tbl in table_names:
    for day in days:
        one_day(day, tbl, folder_path)

