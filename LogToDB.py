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
    con = duckdb.connect(database)
except Exception as e:
    input("DuckDBを開けませんでした:", e)
    sys.exit(1)


def csv_to_db(filepath, table, skip, complete, parse_dates):
    df = pd.read_csv(filepath, skiprows=skip, parse_dates=parse_dates)
    #ファイル名の列を追加
    filename = os.path.basename(filepath)
    df.insert(0,"source", filename)

    #ファイル名管理テーブルの定義
    con.execute("""CREATE TABLE IF NOT EXISTS files (
                source VARCHAR,
                complete BOOL)""")
    #ファイルの登録データを探す
    exists = con.execute("SELECT complete FROM files WHERE source = ?", [filename]).fetchone()
    if exists is None: #ファイルが未登録の場合
        #ファイル登録
        con.execute("INSERT INTO files VALUES (?, ?)", [filename, complete])
        # テーブルがなければ作成（型は df から推論）
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")
        #テーブルにレコード追加
        con.execute(f"INSERT INTO {table} SELECT * FROM df")
        print(f"{filename}を追加しました")
    else:
        is_complete = exists[0]
        if is_complete == False: #ファイルが登録されているが、完了していない場合
            #テーブル内にある同ファイルのデータを消して入れなおす
            con.execute(f"DELETE FROM {table} WHERE source = ?", [filename])
            con.execute(f"INSERT INTO {table} SELECT * FROM df")
            print(f"{filename}を更新しました") 
        elif is_complete == True: #ファイルが登録されていて完了している場合
            print(f"{filename}はDB内に存在しています")


import argparse
import fnmatch
import datetime

def main():
    parser = argparse.ArgumentParser(description="CSVファイルをDBに登録します")
    parser.add_argument("files", nargs="+", help="処理するCSVファイル")
 
    args = parser.parse_args()
    files = args.files

    if files is None:
        input("ファイルを選択してください")
        return

    for table_data in tables:
        pattern = table_data["pattern"]
        table = table_data["table"]
        skip = table_data.get("skip_rows", [])
        parse_dates = table_data.get("parse_dates", [])
        print(parse_dates[0])

        for file in fnmatch.filter(files, f"*{pattern}"):
            #ファイル更新日時が23時58分以降のものはログが完了しているものとする
            ts = os.path.getmtime(file)
            time = datetime.datetime.fromtimestamp(ts).time()
            complete = time >= datetime.time(23, 58)
            
            csv_to_db(file, table, skip, complete, parse_dates)

    input("処理が終了しました。")


if __name__ == "__main__":
    main()
 
