#!/bin/bash

PYTHON_EXE="python"
SCRIPT_PATH="C:/EOL/SOH/Flexi.SOH_Mobility/scripts/estimate_soh.py"

START_DATE="2024-11-09"

LOG_DIR="C:/EOL/SOH/Flexi.SOH_Mobility/logs"
WINDOW_DAYS=15

mkdir -p "$LOG_DIR"
export PYTHONUTF8=1

TODAY=$(date +%Y-%m-%d)
current="$START_DATE"

echo "=============================================="
echo " Batch run: $START_DATE --> $TODAY"
echo "=============================================="

while [[ "$current" < "$TODAY" ]]; do

    echo ""
    echo "----------------------------------------------"
    echo " Running for window starting: $current"
    echo "----------------------------------------------"

    LOG_FILE="$LOG_DIR/run_${current}.log"

    "$PYTHON_EXE" "$SCRIPT_PATH" "$current" 2>&1 | tee "$LOG_FILE"
    EXIT_CODE=${PIPESTATUS[0]}

    if [ $EXIT_CODE -eq 139 ]; then
        echo ""
        echo "  Segfault detected. Retrying window $current once..."
        "$PYTHON_EXE" "$SCRIPT_PATH" "$current" 2>&1 | tee "$LOG_FILE"
        EXIT_CODE=${PIPESTATUS[0]}
    fi

    if [ $EXIT_CODE -ne 0 ]; then
        echo ""
        echo "WARNING: Script exited with code $EXIT_CODE for window $current."
        echo "Check log: $LOG_FILE"

        if [ $EXIT_CODE -eq 139 ]; then
            echo "  Segfault again after retry. Skipping window $current and continuing..."

        elif grep -q "Refitting constants" "$LOG_FILE" && ! grep -q "Running q_loss" "$LOG_FILE"; then
            echo "  Refit-only failure. Continuing to next window..."

        else
            echo "  Critical failure during processing. Stopping."
            echo "  To resume, set START_DATE=$current in the script."
            exit 1
        fi

    else
        echo ""
        echo "  Window $current completed OK."
    fi

    current=$("$PYTHON_EXE" -c "
from datetime import datetime, timedelta
print((datetime.strptime('$current', '%Y-%m-%d') + timedelta(days=$WINDOW_DAYS)).strftime('%Y-%m-%d'))
")

done

echo ""
echo "=============================================="
echo " All windows completed through $TODAY."
echo "=============================================="