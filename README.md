# CSV to DuckDB Ingestion Tool

このツールは、複数の CSV ファイルを DuckDB に取り込み、効率的にクエリできるようにするためのスクリプトです。  
設定は YAML ファイルで管理し、ユーザーが簡単にテーブル名やファイルパターンを変更できるようになっています。

---

## 🚀 特徴
- 複数 CSV ファイルを一括で DuckDB にロード
- YAML 設定でテーブル名や glob パターンを指定可能
- `source_file` カラムを追加してファイルの由来を追跡
- CLI から柔軟に実行可能（drag & drop, positional arguments 対応）

---

## 📦 インストール
```bash
git clone https://github.com/Yasuotem/csv_to_duckdb.git
cd csv_to_duckdb
pip install -r requirements.txt
```

## ⚙️ 使い方
1. 設定ファイル(YAML)
```config.yaml
input_pattern: "data/*.csv"
table_name: "orders"
add_source_file: true
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

