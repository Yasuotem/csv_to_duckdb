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
                source VARCHAR,
                complete BOOL)""")
    #ファイルの登録データを探す
    exists = con.execute("SELECT complete FROM files WHERE source = ?", [filename]).fetchone()
    if exists is None: #ファイルが未登録の場合
        #ファイル登録
        con.execute("INSERT INTO files VALUES (?, ?)", [filename, True])
        # テーブルがなければ作成（型は df から推論）
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")
        #テーブルにレコード追加
        con.execute(f"INSERT INTO {table} SELECT * FROM df")
        print(f"{filename}を追加しました")
    else:
        complete = exists[0]
        if complete == False:pass #ファイルが登録されているが、完了していない場合
        elif complete == True: #ファイルが登録されていて完了している場合
            print(f"{filename}はDB内に存在しています")


import argparse
import fnmatch

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
        skip = table_data["skip_rows"]
        print(pattern)

        for file in fnmatch.filter(files, f"*{pattern}"):
            print(file)
            csv_to_db(file, table, skip)

    input("処理が終了しました。")


if __name__ == "__main__":
    main()
 
