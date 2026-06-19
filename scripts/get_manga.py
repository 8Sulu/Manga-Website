"""
Fetch manga batches from the MAL ranking API.

Usage:
    python get_manga.py <offset>           one batch starting at <offset>
    python get_manga.py <runs>,<step>      <runs> batches stepping by <step>

Examples:
    python get_manga.py 0          # offset 0  (ranks 1-500)
    python get_manga.py 500        # offset 500 (ranks 501-1000)
    python get_manga.py 6,10       # offsets 0,10,20,30,40,50
    python get_manga.py 3,500      # offsets 0,500,1000  (ranks 1-1500)
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from services.api_service import process_manga_batch, is_stop_requested
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def parse_args(raw: str) -> list[int]:
    """
    "0"      → [0]
    "500"    → [500]
    "6,10"   → [0, 10, 20, 30, 40, 50]   (6 runs, step 10)
    "3,500"  → [0, 500, 1000]             (3 runs, step 500)
    """
    raw = raw.strip()
    if "," in raw:
        left, right = raw.split(",", 1)
        try:
            runs = int(left)
            step = int(right)
        except ValueError:
            raise ValueError(f"Expected 'runs,step' (e.g. 3,500 or 6,10), got: {raw!r}")
        if runs < 1 or step < 1:
            raise ValueError("runs and step must both be positive integers")
        return [i * step for i in range(runs)]
    else:
        try:
            return [int(raw)]
        except ValueError:
            raise ValueError(f"Expected an integer offset or 'runs,step', got: {raw!r}")


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)

    try:
        offsets = parse_args(sys.argv[1])
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    total = len(offsets)
    for n, offset in enumerate(offsets, start=1):
        if is_stop_requested():
            print(f"[{n}/{total}] Stop requested — halting", flush=True)
            break
        print(f"\n[Run {n}/{total}] offset={offset}", flush=True)
        if not process_manga_batch(offset):
            print(f"[Run {n}/{total}] Failed at offset {offset}")
            sys.exit(1)

    print("\nAll runs complete.", flush=True)


if __name__ == "__main__":
    main()
