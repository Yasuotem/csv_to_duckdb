#!/bin/bash

# 使用方法: ./run_year.sh 2025
YEAR=$1
OUTPUT_DIR="output"

# 月ごとにループ
for MONTH in $(seq 1 12); do
    echo "=== ${YEAR}-${MONTH} を処理中 ==="
    python GenerateDummy.py --year $YEAR --month $MONTH --output $OUTPUT_DIR
done

echo "✅ $YEAR 年分のCSV出力が完了しました！"
