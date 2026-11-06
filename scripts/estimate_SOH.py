#SOH_Mobility

import subprocess
from datetime import datetime, timedelta
import duckdb
import numpy as np
import pandas as pd
import os
import traceback
import sys
import gc
import resource
import shutil
import time

NUM_DAYS = 15
DBT_PROJECT_DIR = "/home/tec-ubuntu/Jai-Codes/Flexi.SOH_Mobility"
DB_FILE = "dev.duckdb"

SOC_START_THRESHOLD   = 30.0
SOC_END_THRESHOLD     = 99.0
MIN_CHARGE_CURRENT    = 5.0
MIN_IDLE_DURATION_S   = 2400   # 40 minutes
MIN_CHARGE_DURATION_S = 2400   # 40 minutes
MIN_CHARGE_ROWS       = 2
MAX_CAPACITY_AH       = 54

MIN_EVENTS_FOR_SOH_UPDATE = 2

IMBALANCE_TABLE_MV = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110]
IMBALANCE_TABLE_AH = [0, 0.18, 0.32, 0.46, 0.56, 0.66, 0.80, 1.02, 1.34, 1.84, 2.60, 3.68]
VREF_IMBALANCE     = 3.58
SOH_INITIAL        = 1.0


def _get_current_version(con, device_id):
    result = con.execute("""
        SELECT model_version 
        FROM pack_capacity_measurements 
        WHERE device_id = ? 
        ORDER BY event_date DESC 
        LIMIT 1
    """, [device_id]).fetchone()
    
    if result and result[0]:
        return str(result[0])
    
    return f"V1_{device_id}"


def _get_next_version(current_version, device_id):
    if current_version.startswith("V") and "_" in current_version:
        parts = current_version.split("_", 1)
        version_num = int(parts[0][1:])  
        return f"V{version_num + 1}_{device_id}"
    
    return f"V1_{device_id}"


def _get_next_capacity_id(con):
    return con.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM pack_capacity_measurements").fetchone()[0]

def get_imbalance_ah(delta_v_mv):
    return float(np.interp(delta_v_mv, IMBALANCE_TABLE_MV, IMBALANCE_TABLE_AH))

def voltage_to_soc(voltage, ocv_voltages, ocv_socs):
    return float(np.interp(voltage, ocv_voltages, ocv_socs))

def load_ocv_table(con):
    df = con.execute("""
        SELECT ocv_voltage, soc_percent FROM ocv_table ORDER BY ocv_voltage ASC
    """).df()
    if df.empty:
        raise ValueError("OCV table is empty.")
    return df["ocv_voltage"].to_numpy(), df["soc_percent"].to_numpy()

def parse_args():
    if len(sys.argv) > 1:
        try:
            return datetime.strptime(sys.argv[1], "%Y-%m-%d")
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD.")
            exit(1)
    else:
        date_input = input("Enter start date (YYYY-MM-DD): ").strip()
        try:
            return datetime.strptime(date_input, "%Y-%m-%d")
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD.")
            exit(1)

def get_preceding_idle(seg, segments, idx):

    preceding_idles = [s for s in segments[:idx] if s["type"] == "idle"]
    if not preceding_idles:
        return None

    idle_seg = preceding_idles[-1]

    segments_between = segments[segments.index(idle_seg) + 1 : idx]
    if any(s["type"] == "charge" for s in segments_between):
        return None

    if idle_seg["duration_s"] < MIN_IDLE_DURATION_S:
        return None

    return idle_seg

def get_eligible_charge_segments(segments, device_id):

    eligible = []
    skipped_soc = 0
    skipped_no_idle = 0
    skipped_idle_short = 0
    skipped_too_short = 0

    for idx, seg in enumerate(segments):
        if seg["type"] != "charge":
            continue

        charge_segment = seg["rows"]
        start_soc = float(charge_segment.iloc[0]["soc"])

        if start_soc >= SOC_START_THRESHOLD:
            skipped_soc += 1
            continue

        idle_seg = get_preceding_idle(seg, segments, idx)

        if idle_seg is None:
            preceding = [s for s in segments[:idx] if s["type"] == "idle"]

            if not preceding:
                skipped_no_idle += 1
                continue

            candidate = preceding[-1]
            candidate_idx = segments.index(candidate)

            between = segments[candidate_idx + 1:idx]

            if any(s["type"] == "charge" for s in between):
                skipped_no_idle += 1
                continue

            candidate_end = pd.to_datetime(candidate["end_ts"])
            charge_start = pd.to_datetime(seg["start_ts"])

            gap_before_charge = (
                charge_start - candidate_end
            ).total_seconds()

            effective_idle = (
                candidate["duration_s"] + gap_before_charge
            )

            if effective_idle >= 40 * 60:
                idle_seg = candidate.copy()
                idle_seg["effective_idle_duration_s"] = effective_idle
                idle_seg["pre_charge_gap_s"] = gap_before_charge
            else:
                skipped_idle_short += 1
                continue

        charge_time = (
            pd.to_datetime(charge_segment.iloc[-1]["ts"]) -
            pd.to_datetime(charge_segment.iloc[0]["ts"])
        ).total_seconds()

        if charge_time < MIN_CHARGE_DURATION_S or len(charge_segment) < MIN_CHARGE_ROWS:
            skipped_too_short += 1
            continue

        eligible.append((idx, seg, idle_seg))

    print(
        f"[PRE-FILTER] {device_id} | "
        f"Eligible: {len(eligible)} | "
        f"Skipped(SOC too high): {skipped_soc} | "
        f"Skipped(no valid idle): {skipped_no_idle} | "
        f"Skipped(idle too short <40min): {skipped_idle_short} | "
        f"Skipped(charge too short): {skipped_too_short}"
    )

    return eligible


def detect_and_estimate_capacity(con, start_date, end_date, ocv_voltages, ocv_socs):
    MAX_ALLOWED_GAP_SECONDS = 60

    con.execute("ALTER TABLE pack_capacity_measurements ADD COLUMN IF NOT EXISTS soh DOUBLE")
    con.execute("ALTER TABLE pack_capacity_measurements ADD COLUMN IF NOT EXISTS actual_capacity DOUBLE")

    soh_df = con.execute("""
        SELECT device_id, soh, actual_capacity
        FROM (
            SELECT device_id, soh, actual_capacity,
                   ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY event_date DESC) AS rn
            FROM pack_capacity_measurements
            WHERE soh IS NOT NULL AND actual_capacity IS NOT NULL
        ) t
        WHERE rn = 1
    """).df()

    latest_soh = dict(zip(soh_df["device_id"], soh_df["soh"]))
    actual_capacity = dict(zip(soh_df["device_id"], soh_df["actual_capacity"]))

    print(f"[INIT] Loaded SOH for {len(latest_soh)} devices | Date range: {start_date} → {end_date}")

    raw_df = con.execute("""
        SELECT device_id, ts, v_min, current, soc, temp01, nominal_capacity,
               v1, v2, v3, v4, v5, v6, v7, v8,
               v9, v10, v11, v12, v13, v14, v15, v16
        FROM inter_battery_variable_mapping
        WHERE ts >= ?::timestamp AND ts < ?::timestamp
        ORDER BY device_id, ts
    """, [start_date, end_date]).df()

    if raw_df.empty:
        print("[WARN] No telemetry data found for the given date range. Exiting.")
        return 0

    print(f"[DATA] Fetched {len(raw_df)} rows across {raw_df['device_id'].nunique()} devices")

    cell_cols = [c for c in raw_df.columns if c.startswith("v") and c[1:].isdigit()]

    events_saved = 0
    next_id = _get_next_capacity_id(con)
    batch_qs = {}

    for device_id, group in raw_df.groupby("device_id"):
        group = group.sort_values("ts").reset_index(drop=True)

        device_soh = latest_soh.get(device_id, 1.0)
        device_act_cap = actual_capacity.get(device_id, None)
        is_new_device = device_act_cap is None

        current_version = _get_current_version(con, device_id)

        print(f"\n[DEVICE] {device_id} | {'NEW' if is_new_device else 'KNOWN'} | SOH: {device_soh:.4f} | ActualCap: {device_act_cap} | Version: {current_version}")

        group["seg_type"] = group["current"].apply(
            lambda c: "idle" if float(c) == 0.0 else ("charge" if float(c) >= MIN_CHARGE_CURRENT else "other")
        )

        group["seg_id"] = (group["seg_type"] != group["seg_type"].shift()).cumsum()

        segments = []

        for seg_id, seg in group.groupby("seg_id"):
            seg = seg.reset_index(drop=True)
            start_ts = pd.to_datetime(seg.iloc[0]["ts"])
            end_ts = pd.to_datetime(seg.iloc[-1]["ts"])

            segments.append({
                "seg_id": seg_id,
                "type": seg.iloc[0]["seg_type"],
                "start_ts": start_ts,
                "end_ts": end_ts,
                "duration_s": (end_ts - start_ts).total_seconds(),
                "rows": seg,
            })

        charge_segs = [s for s in segments if s["type"] == "charge"]
        idle_segs = [s for s in segments if s["type"] == "idle"]

        print(f"[SEGMENTS] {device_id} | Total: {len(segments)} | Charge: {len(charge_segs)} | Idle: {len(idle_segs)}")

        eligible = get_eligible_charge_segments(segments, device_id)

        if not eligible:
            print(f"[SKIP DEVICE] {device_id} | No eligible charge segments after pre-filter")
            continue

        for idx, seg, idle_seg in eligible:
            charge_segment = seg["rows"]
            idle_rows = idle_seg["rows"]

            if len(idle_rows) < 2:
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | Preceding idle segment has fewer than 2 rows")
                continue

            last_idle_row = idle_rows.iloc[-2]

            cell_v = pd.to_numeric(last_idle_row[cell_cols], errors="coerce").dropna()
            cell_v = cell_v[cell_v > 0]

            if not cell_v.empty:
                ocv_voltage = float(cell_v.min())
            elif pd.notna(last_idle_row.get("v_min")) and float(last_idle_row["v_min"]) > 0:
                ocv_voltage = float(last_idle_row["v_min"])
            else:
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | Could not extract valid OCV from idle row")
                continue

            print(f"[OCV] {device_id} | seg {seg['seg_id']} | OCV: {ocv_voltage:.4f}V from idle row at {last_idle_row['ts']}")

            ts_vals = pd.to_datetime(charge_segment["ts"], errors="coerce")

            if ts_vals.isna().any():
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | Invalid timestamp found")
                continue

            dt_seconds = ts_vals.diff().dt.total_seconds().fillna(0)

            if (dt_seconds < 0).any():
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | Negative timestamp difference detected")
                continue

            max_gap = float(dt_seconds.max())

            if max_gap > MAX_ALLOWED_GAP_SECONDS:
                print(
                    f"[SKIP] {device_id} | seg {seg['seg_id']} | "
                    f"Data gap detected: {max_gap:.1f}s > {MAX_ALLOWED_GAP_SECONDS}s"
                )
                continue

            current_vals = pd.to_numeric(charge_segment["current"], errors="coerce")

            if current_vals.isna().any():
                print(
                    f"[SKIP] {device_id} | seg {seg['seg_id']} | "
                    f"Invalid current value found"
                )
                continue

            max_raw_current = float(current_vals.max())

            if max_raw_current > 60.0:
                print(
                    f"[CURRENT CAP] {device_id} | seg {seg['seg_id']} | "
                    f"Max raw current: {max_raw_current:.2f}A | Capped to 60A"
                )

            current_vals = current_vals.clip(upper=60.0)

            # dt_hours = dt_seconds / 3600.0

            # coulombs_counted = float((current_vals * dt_hours).sum())

            # current_vals = pd.to_numeric(charge_segment["current"], errors="coerce")

            # if current_vals.isna().any():
            #     print(f"[SKIP] {device_id} | seg {seg['seg_id']} | Invalid current value found")
            #     continue

            dt_hours = dt_seconds / 3600.0

            coulombs_counted = float((current_vals * dt_hours).sum())

            if coulombs_counted <= 0:
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | Coulombs counted zero or negative: {coulombs_counted:.4f}")
                continue

            delta_v_mv = 0.0

            if cell_cols:
                for _, seg_row in charge_segment.iterrows():
                    cell_v = pd.to_numeric(seg_row[cell_cols], errors="coerce").dropna()
                    cell_v = cell_v[cell_v > 0]

                    if cell_v.empty:
                        continue

                    if abs(cell_v.max() - VREF_IMBALANCE) <= 0.01:
                        delta_v_mv = (cell_v.max() - cell_v.min()) * 1000.0
                        break

            if delta_v_mv == 0:
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | Imbalance threshold never hit, charge incomplete")
                continue

            imbalance_ah = get_imbalance_ah(max(0.0, delta_v_mv))

            nominal_cap_val = pd.to_numeric(
                charge_segment["nominal_capacity"].iloc[0],
                errors="coerce"
            )

            if pd.isna(nominal_cap_val) or float(nominal_cap_val) <= 0:
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | Invalid nominal capacity")
                continue

            nominal_cap = float(nominal_cap_val)

            start_soc_ocv = voltage_to_soc(ocv_voltage, ocv_voltages, ocv_socs)

            if start_soc_ocv <= 0 or start_soc_ocv > SOC_START_THRESHOLD:
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | OCV-derived SOC out of range: {start_soc_ocv:.2f}%")
                continue

            actual_end_soc_val = pd.to_numeric(charge_segment.iloc[-1]["soc"], errors="coerce")

            if pd.isna(actual_end_soc_val):
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | Invalid end SOC")
                continue

            actual_end_soc = float(actual_end_soc_val)
            ocv_soc_rise = actual_end_soc - start_soc_ocv

            if ocv_soc_rise < 10.0:
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | SOC rise too small: {ocv_soc_rise:.2f}%")
                continue

            start_soc_frac = start_soc_ocv / 100.0
            base_cap       = device_act_cap if not is_new_device else nominal_cap
            leftover_ah    = start_soc_frac * device_soh * base_cap
            total_q_ah     = leftover_ah + coulombs_counted + imbalance_ah

            temp_vals = pd.to_numeric(
                charge_segment["temp01"],
                errors="coerce"
            )

            avg_temp = float(temp_vals.mean()) if temp_vals.notna().any() else None

            if total_q_ah < MAX_CAPACITY_AH:
                print(
                    f"[EVENT] {device_id} | seg {seg['seg_id']} | "
                    f"StartSOC: {start_soc_ocv:.2f}% | EndSOC: {actual_end_soc:.2f}% | "
                    f"Remaining: {leftover_ah:.4f} | Coulombs: {coulombs_counted:.4f}Ah | "
                    f"Imbalance: {imbalance_ah:.4f}Ah | TotalQ: {total_q_ah:.4f}Ah"
                )

                con.execute("""
                    INSERT INTO pack_capacity_measurements
                        (id, device_id, event_date,
                         charge_start_ts, charge_end_ts,
                         start_voltage, start_soc_ocv, end_soc, leftover_ah,
                         coulombs_counted, imbalance_q_ah, total_q_ah,
                         avg_temp, model_version, actual_capacity, soh)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    next_id,
                    device_id,
                    pd.to_datetime(charge_segment.iloc[0]["ts"]).date(),
                    str(charge_segment.iloc[0]["ts"]),
                    str(charge_segment.iloc[-1]["ts"]),
                    ocv_voltage,
                    start_soc_ocv,
                    actual_end_soc,
                    leftover_ah,
                    coulombs_counted,
                    imbalance_ah,
                    total_q_ah,
                    avg_temp,
                    current_version,
                    device_act_cap if not is_new_device else None,
                    device_soh
                ])

                batch_qs.setdefault(device_id, []).append((total_q_ah, nominal_cap, ocv_soc_rise))

                next_id += 1
                events_saved += 1

            else:
                print(f"[SKIP] {device_id} | seg {seg['seg_id']} | Total Q exceeds {MAX_CAPACITY_AH}Ah: {total_q_ah:.4f}Ah")
                continue

        print(f"\n[SUMMARY] Total charge events saved this run: {events_saved}")

    if batch_qs:
        for device_id, events in batch_qs.items():
            qs = [e[0] for e in events]
            nominal = events[0][1]
            weights = [e[2] for e in events]
            prior_soh_value = latest_soh.get(device_id, None)

            current_version = _get_current_version(con, device_id)

            print(f"\n[SOH UPDATE] {device_id} | Events this run: {len(events)} | Prior SOH: {prior_soh_value} | Current Version: {current_version}")

            act_cap_row = con.execute("""
                SELECT actual_capacity
                FROM pack_capacity_measurements
                WHERE device_id = ?
                  AND actual_capacity IS NOT NULL
                ORDER BY event_date ASC
                LIMIT 1
            """, [device_id]).fetchone()

            if prior_soh_value is None:
                if len(qs) < 2:
                    print(f"[SKIP SOH] {device_id} | New device but only {len(qs)} event(s) this run — need at least 2 to fix actual capacity")
                    continue

                weight_sum = sum(weights)

                if weight_sum <= 0:
                    print(f"[SKIP SOH] {device_id} | Invalid weight sum: {weight_sum}")
                    continue

                dev_actual_cap = sum(
                    q * w for q, w in zip(qs, weights)
                ) / weight_sum

                print(f"[NEW DEVICE] {device_id} | Fixing actual_capacity from {len(qs)} events → {dev_actual_cap:.4f}Ah")

                con.execute("""
                    UPDATE pack_capacity_measurements
                    SET actual_capacity = ?
                    WHERE device_id = ?
                """, [dev_actual_cap, device_id])

                new_soh = 1.0
                final_version = current_version

                print(f"[VERSION] {device_id} | First calculation → {final_version} | SOH: {new_soh:.6f}")

            else:
                if act_cap_row is None or act_cap_row[0] is None:
                    print(f"[SKIP SOH] {device_id} | No baseline actual_capacity found in DB")
                    continue

                dev_actual_cap = float(act_cap_row[0])

                if dev_actual_cap <= 0:
                    print(f"[SKIP SOH] {device_id} | Invalid baseline actual_capacity: {dev_actual_cap}")
                    continue

                if len(qs) < MIN_EVENTS_FOR_SOH_UPDATE:
                    print(f"[SKIP SOH] {device_id} | Not enough events for SOH update: {len(qs)} < {MIN_EVENTS_FOR_SOH_UPDATE}")

                    con.execute("""
                        UPDATE pack_capacity_measurements
                        SET actual_capacity = ?
                        WHERE device_id = ?
                          AND event_date >= ?::date
                          AND event_date < ?::date
                    """, [
                        dev_actual_cap,
                        device_id,
                        start_date,
                        end_date
                    ])

                    continue

                weight_sum = sum(weights)

                if weight_sum <= 0:
                    print(f"[SKIP SOH] {device_id} | Invalid weight sum: {weight_sum}")
                    continue

                weighted_q = sum(q * w for q, w in zip(qs, weights)) / weight_sum

                new_soh = round(min(max(weighted_q / dev_actual_cap, 0.0), 1.0), 6)

                soh_changed = abs(new_soh - prior_soh_value) > 1e-6

                if soh_changed:
                    final_version = _get_next_version(current_version,device_id)

                    print(f"[KNOWN DEVICE] {device_id} | WeightedQ: {weighted_q:.4f}Ah | BaselineCap: {dev_actual_cap:.4f}Ah | NewSOH: {new_soh:.6f}")
                    print(f"[VERSION] {device_id} | SOH changed {prior_soh_value:.6f} → {new_soh:.6f} | {current_version} → {final_version}")

                else:
                    final_version = current_version

                    print(f"[VERSION] {device_id} | SOH unchanged {new_soh:.6f} | Stays at {final_version}")

            con.execute("""
                UPDATE pack_capacity_measurements
                SET actual_capacity = ?,
                    soh = ?,
                    model_version = ?
                WHERE device_id = ?
                  AND event_date >= ?::date
                  AND event_date < ?::date
            """, [
                dev_actual_cap,
                new_soh,
                final_version,
                device_id,
                start_date,
                end_date
            ])

            print(f"[WRITTEN] {device_id} | actual_capacity: {dev_actual_cap:.4f}Ah | soh: {new_soh:.6f} | version: {final_version}")

    print(f"\n[DONE] detect_and_estimate_capacity complete | Events saved: {events_saved}")

    return events_saved

def run_dbt_command(command_args):
    cmd = (
        ["dbt"]
        + command_args[:1]
        + ["--no-partial-parse", "--no-write-json"]
        + command_args[1:]
    )

    for attempt in range(2):

        result = subprocess.run(
            cmd,
            cwd=DBT_PROJECT_DIR,
            shell=False
        )

        if result.returncode == 0:
            return

        print(f"RETURN CODE: {result.returncode}")

        if result.returncode == -11:

            if attempt == 0:

                print("Segmentation fault detected. Retrying once...")

                shutil.rmtree(
                    os.path.join(DBT_PROJECT_DIR, "target"),
                    ignore_errors=True
                )

                for root, dirs, files in os.walk(DBT_PROJECT_DIR):
                    for file in files:
                        if file == "partial_parse.msgpack":
                            try:
                                os.remove(
                                    os.path.join(root, file)
                                )
                            except Exception:
                                pass

                gc.collect()
                time.sleep(5)

                continue

            raise RuntimeError("dbt segfaulted twice")

        raise RuntimeError(
            f"dbt failed with return code {result.returncode}"
        )
        
def main():
    start_date = parse_args()
    end_date = start_date + timedelta(days=NUM_DAYS)
    db_path = os.path.join(DBT_PROJECT_DIR, DB_FILE)

    con = duckdb.connect(db_path)

    con.execute("""
    DROP TABLE IF EXISTS inter_battery_variable_mapping
    """)

    con.close()

    for i in range(NUM_DAYS):
        date_str = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")

        print(f"\n--- Day {i + 1}: {date_str} ---")

        try:

            run_dbt_command(
                [
                    "run",
                    "--select",
                    "stg_battery_raw_parquet_data inter_battery_variable_mapping",
                    "--vars",
                    f'{{"process_date":"{date_str}"}}'
                ]
            )

        except Exception as e:

            print(f"Skipping {date_str}: {e}")
            continue

    con = duckdb.connect(db_path)

    con.execute("""
        CREATE TABLE IF NOT EXISTS pack_capacity_measurements (
            id                INTEGER,
            device_id         VARCHAR,
            event_date        DATE,
            charge_start_ts   VARCHAR,
            charge_end_ts     VARCHAR,
            start_voltage     DOUBLE,
            start_soc_ocv     DOUBLE,
            end_soc           DOUBLE,
            leftover_ah       DOUBLE,
            coulombs_counted  DOUBLE,
            imbalance_q_ah    DOUBLE,
            total_q_ah        DOUBLE,
            avg_temp          DOUBLE,
            model_version     VARCHAR,
            actual_capacity   DOUBLE,
            soh               DOUBLE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS ocv_table (
            soc_percent  DOUBLE PRIMARY KEY,
            ocv_voltage  DOUBLE
        )
    """)

    count = con.execute(
        "SELECT COUNT(*) FROM ocv_table"
    ).fetchone()[0]

    if count == 0:
        con.execute("""
            INSERT INTO ocv_table VALUES
                (0,   2.973),
                (5,   3.183),
                (10,  3.202),
                (15,  3.222),
                (20,  3.246),
                (25,  3.262),
                (30,  3.278),
                (35,  3.284),
                (40,  3.285),
                (45,  3.285),
                (50,  3.288),
                (55,  3.289),
                (60,  3.291),
                (65,  3.312),
                (70,  3.325),
                (75,  3.326),
                (80,  3.326),
                (85,  3.326),
                (90,  3.327),
                (95,  3.329),
                (100, 3.339)
        """)

    try:
        ocv_voltages, ocv_socs = load_ocv_table(con)
        print(f"[CHECK] Devices in table: {con.execute('SELECT DISTINCT device_id FROM inter_battery_variable_mapping').fetchall()}")

        events = detect_and_estimate_capacity(
            con,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            ocv_voltages,
            ocv_socs
        )

        if events == 0:
            print("No charge events detected.")

    except BaseException as e:
        print(f"Error: {type(e).__name__}: {e}")
        traceback.print_exc()

    finally:
        con.close()

    print("\nDone.")

if __name__ == "__main__":
    main()