import pandas as pd
import duckdb
import yaml
import os

#YAMLのデータを読み込み
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)
database = config["database"]
tables = config["tables"]

def csv_to_db(filepath, table, skip):
    df = pd.read_csv(filepath, skiprows=skip)
    #ファイル名の列を追加
    filename = os.path.basename(filepath)
    df.insert(0,"source", filename)
    
    con = duckdb.connect(database)

    #ファイル名管理テーブルの定義
    con.execute("""CREATE TABLE IF NOT EXISTS files (
                source VERCHAR,
                compete BOOL)""")
    #ファイルの登録データを探す
    exists = con.execute(f"SELECT complete FROM files WHERE source = {filename}").fetchone()[0]
    if exists is None: #ファイルが未登録の場合
        #ファイル登録
        con.execute(f"INSERT INTO files VALUES ({filename}, {True})")
        # テーブルがなければ作成（型は df から推論）
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")
        #テーブルにレコード追加
        con.execute(f"INSERT INTO {table} SELECT * FROM df")
        print(f"{filename}を追加しました")
    elif exists == False:pass #ファイルが登録されているが、完了していない場合
    elif exists == True: #ファイルが登録されていて完了している場合
        print(f"{filename}はDB内に存在しています")


import argparse
import fnmatch

def main():
    parser = argparse.ArgumentParser(description="CSVファイルをDBに登録します")
    parser.add_argument("files", nargs="+", help="処理するCSVファイル")
 
    args = parser.parse_args()
    files = args.files

    for table_data in tables:
        pattern = table_data["pattern"]
        table = table_data["table"]
        skip = table_data["skip"]

        for file in fnmatch.filter(files, f"*{pattern}"):
            csv_to_db(file, table, skip)

    input("処理が終了しました。")


if __name__ == "__main__":
    main()
 
