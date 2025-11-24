"""
Ashmore RMP Data Orchestrator
Runs the scraper and mapper in sequence.
"""

import subprocess
import sys

def main():
    print("=" * 60)
    print("ASHMORE RMP DATA ORCHESTRATOR")
    print("=" * 60)
    print()

    # Step 1: Run scraper
    print("[1/2] Running scraper...")
    result = subprocess.run([sys.executable, "scraper.py"], cwd=".")
    if result.returncode != 0:
        print("[FAIL] Scraper failed")
        return 1

    print()

    # Step 2: Run mapper
    print("[2/2] Running mapper...")
    result = subprocess.run([sys.executable, "map.py"], cwd=".")
    if result.returncode != 0:
        print("[FAIL] Mapper failed")
        return 1

    print()
    print("=" * 60)
    print("[OK] ORCHESTRATION COMPLETE")
    print("=" * 60)
    return 0

if __name__ == "__main__":
    sys.exit(main())
