#!/bin/bash

PROJECT_DIR="$HOME/Jai-Codes/Flexi.SOH_Mobility"

PYTHON_EXE="python"
SCRIPT_PATH="$PROJECT_DIR/scripts/estimate_SOH.py"

START_DATE="2024-11-04"
CHUNK_DAYS=30

LOG_DIR="$PROJECT_DIR/logs"

mkdir -p "$LOG_DIR"
export PYTHONUTF8=1

TODAY=$(date +%Y-%m-%d)
current="$START_DATE"

echo "=============================================="
echo " Batch run: $START_DATE --> $TODAY"
echo "=============================================="

while [[ "$current" < "$TODAY" ]]; do

    chunk_end=$(
        "$PYTHON_EXE" -c "
from datetime import datetime, timedelta

start = datetime.strptime('$current', '%Y-%m-%d')
today = datetime.strptime('$TODAY', '%Y-%m-%d')

end = start + timedelta(days=$CHUNK_DAYS)

if end > today:
    end = today

print(end.strftime('%Y-%m-%d'))
"
    )

    echo ""
    echo "=============================================="
    echo " Running chunk: $current --> $chunk_end"
    echo "=============================================="

    LOG_FILE="$LOG_DIR/chunk_${current}_${chunk_end}.log"

    "$PYTHON_EXE" "$SCRIPT_PATH" "$current" "$chunk_end" \
        2>&1 | tee "$LOG_FILE"

    EXIT_CODE=${PIPESTATUS[0]}

    if [ $EXIT_CODE -eq 139 ]; then

        echo ""
        echo "Segfault detected. Retrying chunk once..."

        rm -rf "$PROJECT_DIR/target"

        find "$PROJECT_DIR" \
            -name "partial_parse.msgpack" \
            -delete

        sync
        sleep 2

        "$PYTHON_EXE" "$SCRIPT_PATH" "$current" "$chunk_end" \
            2>&1 | tee -a "$LOG_FILE"

        EXIT_CODE=${PIPESTATUS[0]}
    fi

    if [ $EXIT_CODE -ne 0 ]; then

        echo ""
        echo "Chunk failed with exit code $EXIT_CODE"
        echo "Check log: $LOG_FILE"

        exit $EXIT_CODE

    fi

    echo ""
    echo "Chunk completed successfully."

    rm -rf "$PROJECT_DIR/target"

    find "$PROJECT_DIR" \
        -name "partial_parse.msgpack" \
        -delete

    sync
    sleep 2

    current="$chunk_end"

done

echo ""
echo "=============================================="
echo "All chunks completed through $TODAY"
echo "=============================================="