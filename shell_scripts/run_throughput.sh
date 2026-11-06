#!/bin/bash

START_DATE="$1"
END_DATE=$(date +%Y-%m-%d)

PROJECT_DIR="$HOME/Jai-Codes/Flexi.SOH_Mobility"

LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

current_date="$START_DATE"

while [[ "$current_date" < "$END_DATE" || "$current_date" == "$END_DATE" ]]; do

    LOG_FILE="$LOG_DIR/dbt_run_${current_date}.log"

    echo "========================================" | tee -a "$LOG_FILE"
    echo "Running for: $current_date" | tee -a "$LOG_FILE"
    echo "========================================" | tee -a "$LOG_FILE"

    while true; do

        dbt run \
            --select stg_battery_raw_parquet_data inter_battery_variable_mapping mart_daily_cyclic_aging_bucket \
            --vars "{\"process_date\":\"$current_date\"}" \
            2>&1 | tee -a "$LOG_FILE"

        DBT_EXIT=${PIPESTATUS[0]}

        if [ $DBT_EXIT -eq 0 ]; then
            echo "Completed: $current_date" | tee -a "$LOG_FILE"
            break
        fi

        echo "Failed for: $current_date" | tee -a "$LOG_FILE"
        echo "Retrying same day in 3 seconds..." | tee -a "$LOG_FILE"
        sleep 3

    done

    current_date=$(date -I -d "$current_date + 1 day")

done

echo "========================================"
echo "   All dates completed successfully!"
echo "========================================"