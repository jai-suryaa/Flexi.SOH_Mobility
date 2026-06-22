import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

start = datetime(2026, 2, 27)
today = datetime.today()
WINDOW_DAYS = 15

log_dir = Path("logs/master")
log_dir.mkdir(parents=True, exist_ok=True)

while start + timedelta(days=WINDOW_DAYS) < today:

    date_str = start.strftime("%Y-%m-%d")

    print(f"STARTING WINDOW: {date_str}")
    print("=" * 80)

    log_file = log_dir / f"{date_str}.log"

    with open(log_file, "w", encoding="utf-8") as f:

        process = subprocess.Popen(
            [
                sys.executable,
                "scripts/estimate_SOH.py",
                date_str
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        for line in process.stdout:
            print(line, end="")
            f.write(line)
            f.flush()

        process.wait()

    print(f"\nWINDOW {date_str} EXIT CODE: {process.returncode}")

    if process.returncode != 0:
        print(f"FAILED AT {date_str}")
        print(f"CHECK LOG: {log_file}")
        break

    time.sleep(30)

    start += timedelta(days=15)

print("\nDone")