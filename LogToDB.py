import pandas as pd
import duckdb
import yaml
import os
import fnmatch
from datetime import datetime
import argparse
from pathlib import Path
import zipfile
from functools import partial
from typing import TypedDict
import dateutil

# YAML設定の型定義
class TableConfig(TypedDict, total=False):
    pattern: str                     # ファイル名パターン (例: "*.csv")
    table_name: str                  # DuckDB のテーブル名
    skip_rows: list[int] | None      # スキップする行番号リスト (例: [1])
    parse_dates: list[str] | None    # 日付として解釈する列名リスト
    log_completion_time: str

class Config(TypedDict):
    database: str                    # DuckDB データベースファイル名
    tables: list[TableConfig]        # 複数テーブルの設定リスト

#CSVをdfに変換してテーブルに追加, filesテーブルにファイル名を登録
def csv_to_db(con, file, table: TableConfig, filename, last_modded):
    #csvのデータをdfに読み込む
    df = pd.read_csv(file,
        skiprows=table.get("skip_rows", []),
        parse_dates=table.get("parse_dates", []),
        encoding="utf-8-sig")
    #ファイル名の列を先頭に追加
    df.insert(0,"source", filename)

    #ファイル更新日時からログが完了しているか判別する
    complete = last_modded.time() >= dateutil.parser.parse(table["log_completion_time"]).time()

    table_name = table["table_name"]
    #ファイルの登録データを探す
    match con.execute("SELECT complete FROM files WHERE source = ?", [filename]).fetchone():
        case None: #ファイルが未登録の場合
            # テーブルがなければ作成（型は df から推論）
            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0")
            #ファイル登録
            con.execute("INSERT INTO files VALUES (?, FALSE)", [filename])
            #テーブルにレコード追加
            con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            #ファイル登録データを更新する
            con.execute("UPDATE files SET complete = ? WHERE source = ?", [complete, filename])
            print(f"{filename}を追加しました")

        case (False,): # ファイルが登録されているが未完了の場合
            #テーブル内にある同ファイルのデータを消して入れなおす
            con.execute(f"DELETE FROM {table_name} WHERE source = ?", [filename])
            con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            #ファイル登録データを更新する
            con.execute("UPDATE files SET complete = ? WHERE source = ?", [complete, filename])
            print(f"{filename}を更新しました") 
            
        case (True,): #ファイルが登録されていて完了している場合
            print(f"{filename}はDB内に存在しています")

#ファイルをcsv/csvの入ったzipで分類して処理
def read_files(con, config, files):
    #yamlファイルからパターンの一致するtableを取得する
    def detect_pattern(str):
        for table in config["tables"]:
            if fnmatch.fnmatch(str, table["pattern"]):
                return table
        return None
    
    for filepath in files:
        match filepath.suffix:
            case '.csv':
                filename = filepath.name
                table = detect_pattern(filename)
                if table is not None:
                    #最終更新日時をdatetimeで取得
                    ts = os.path.getmtime(filepath)
                    last_modded = datetime.fromtimestamp(ts)
                    
                    csv_to_db(con, filepath, table, filename, last_modded)
            case '.zip':
                with zipfile.ZipFile(filepath) as z:
                    print(filepath.name)
                    for info in z.infolist():
                        path = Path(info.filename)
                        if path.suffix == '.csv':
                            filename = path.name
                            table = detect_pattern(filename)
                            if table is not None:
                                #最終更新日時をdatetimeで取得
                                last_modded = datetime(*info.date_time)

                                csv_to_db(con, z.open(info), table, filename, last_modded)
            case _:
                pass

def main():
    parser = argparse.ArgumentParser(description="CSVファイルをDBに登録します")
    parser.add_argument("files", nargs="+", type=Path, help="処理するCSVファイルまたはCSVファイルのZIPアーカイブ")
    parser.add_argument("-C", "--config", type=Path, default="config.yaml", help="設定のYAMLファイルの指定")
 
    args = parser.parse_args()

    #YAMLのデータを読み込み
    try:
        with open(args.config, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print("YAMLファイルを開けませんでした:", e)
        input()
        return
    # duckdbに接続
    try:
        con = duckdb.connect(config["database"]) 
    except Exception as e:
        print("DuckDBを開けませんでした:", e)
        input()
        return
    
    #ファイル名管理テーブルの定義
    con.execute("""CREATE TABLE IF NOT EXISTS files (
                source VARCHAR,
                complete BOOL)""")
  
    read_files(con, config, args.files)
    
    print("処理が終了しました。")
    input()
    
if __name__ == "__main__":
    main()
 
