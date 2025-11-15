import pandas as pd
import duckdb
import yaml
import os
import sys

#YAMLのデータを読み込み
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)
database = config["database"]
tables = config["tables"]

try:
    con = duckdb.connect(database) #現状例外にならずに強制終了する
except Exception as e:
    print("DuckDBを開けませんでした:", e)
    input()
    sys.exit(1)

import fnmatch
from datetime import datetime, time

def detect_pattern(str):
    for table in tables:
        if fnmatch.fnmatch(str, table["pattern"]):
            return table
    return None

def csv_to_db(file, table, filename, last_modded):

    df = pd.read_csv(file,
        skiprows=table.get("skip_rows", []),
        parse_dates=table.get("parse_dates", []),
        encoding="utf-8-sig")
    #ファイル名の列を追加
    df.insert(0,"source", filename)

    #ファイル更新日時が23時58分以降のものはログが完了しているものとする
    t = last_modded.time()
    complete = t >= time(23, 58)

    #ファイル名管理テーブルの定義
    con.execute("""CREATE TABLE IF NOT EXISTS files (
                source VARCHAR,
                complete BOOL)""")
    table_name = table["table_name"]
    #ファイルの登録データを探す
    exists = con.execute("SELECT complete FROM files WHERE source = ?", [filename]).fetchone()
    if exists is None: #ファイルが未登録の場合
        #ファイル登録
        con.execute("INSERT INTO files VALUES (?, ?)", [filename, complete])
        # テーブルがなければ作成（型は df から推論）
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0")
        #テーブルにレコード追加
        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        print(f"{filename}を追加しました")
    else:
        is_complete = exists[0]
        if is_complete == False: #ファイルが登録されているが、完了していない場合
            #テーブル内にある同ファイルのデータを消して入れなおす
            con.execute(f"DELETE FROM {table_name} WHERE source = ?", [filename])
            con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            #ファイル登録データを更新する
            con.execute("UPDATE files SET complete = ? WHERE source = ?", [complete, filename])
            print(f"{filename}を更新しました") 
        elif is_complete == True: #ファイルが登録されていて完了している場合
            print(f"{filename}はDB内に存在しています")


import argparse
from pathlib import Path
import zipfile


def main():
    parser = argparse.ArgumentParser(description="CSVファイルをDBに登録します")
    parser.add_argument("files", nargs="+", help="処理するCSVファイル")
 
    args = parser.parse_args()
    files = args.files

    for filepath in map(Path, files):
        match filepath.suffix:
            case '.csv':
                filename = filepath.name
                table = detect_pattern(filename)
                if table is not None:
                    ts = os.path.getmtime(filepath)
                    last_modded = datetime.fromtimestamp(ts)
                    
                    csv_to_db(filepath, table, filename, last_modded)
            case '.zip':
                with zipfile.ZipFile(filepath) as z:
                    print(filepath.name)
                    for info in z.infolist():
                        path = Path(info.filename)
                        filename = path.name
                        table = detect_pattern(filename)
                        if table is not None:
                            last_modded = datetime(*info.date_time)
                            csv_to_db(z.open(info), table, filename, last_modded)
            case _:
                pass

    
    print("処理が終了しました。")
    input()


if __name__ == "__main__":
    main()
 
