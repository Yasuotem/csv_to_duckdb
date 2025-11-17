# CSV to DuckDB Ingestion Tool

このツールは、複数の CSV ファイルを DuckDB に取り込み、効率的にクエリできるようにするためのスクリプトです。  
設定は YAML ファイルで管理し、ユーザーが簡単にテーブル名やファイルパターンを変更できるようになっています。

---

## 🚀 特徴
- 複数 CSV ファイルを一括で DuckDB にロード
- ZIPファイル内のCSVファイルもロードできます
- YAML 設定でテーブル名やファイル名のパターンを指定可能
- 読み込んだCSVファイル名を記録することで、同じデータを重複して記録しないようにしています
- CLIから柔軟に実行可能（drag & drop, positional arguments 対応）


## 動作環境
スクリプトの実行にはPython 3.10以上が必要です



## 📦 インストール
```bash
git clone https://github.com/Yasuotem/csv_to_duckdb.git
cd csv_to_duckdb
pip install -r requirements.txt
```
もしくは
1. https://github.com/Yasuotem/csv_to_duckdb/archive/refs/heads/main.zip
からダウンロードして任意の場所に展開して下さい
2. 展開先のフォルダで右クリックメニューから「ターミナルで開く」を選択し、以下のコマンドを実行してください
```bash
pip install -r requirements.txt
```



## ⚙️ 使い方
1. 設定ファイル(config.yaml)を編集
```config.yaml
database: "output/testdb.duckdb" # 出力するduckdbファイル名
tables:
  - pattern: "TBL01_*.csv" # csvファイル名のパターン
    table_name: "TBL01" # データベース内のデーブル名を指定する
    skip_rows: [1]  # スキップしたい行を指定する。最初の行は0。3行目までスキップしたいなら[0, 1, 2]のようにする。省略可
    parse_dates: [収集日時] # 日時として記録したい列の列名を指定する。省略可
  - pattern: "TEST_*.csv" 
    table_name: "TEST"
    skip_rows: [1, 2]  
    parse_dates: [収集日時]
```

2. 実行例
```bash
python ingest.py --config config.yaml
```

3. DuckDB でのクエリ例
```duckdb
SELECT COUNT(*) FROM orders;
SELECT DISTINCT source_file FROM orders;
```

