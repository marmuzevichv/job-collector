"""
merge_jobs.py  —  Merge latest_jobs.csv + latest_jobs_ddg.csv into latest_jobs_combined.csv
Run after both collectors have finished.
"""

import csv
import os
from datetime import datetime, timezone
from typing import Dict, List, Any

FILE_A = "latest_jobs.csv"
FILE_B = "latest_jobs_ddg.csv"
OUTPUT = "latest_jobs_combined.csv"

FIELDS = [
    "collected_at_utc", "source_type", "company", "title",
    "location", "team", "url", "external_id", "posted_at",
    "description_snippet",
]


def read_csv(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        print(f"  File not found: {path}")
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main() -> None:
    jobs_a = read_csv(FILE_A)
    jobs_b = read_csv(FILE_B)

    print(f"{FILE_A}: {len(jobs_a)} jobs")
    print(f"{FILE_B}: {len(jobs_b)} jobs")

    seen_urls: set = set()
    unique: List[Dict[str, Any]] = []

    for job in sorted(
        jobs_a + jobs_b,
        key=lambda x: (x.get("company", "").lower(), x.get("title", "").lower()),
    ):
        url = job.get("url", "").strip()
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique.append(job)

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(unique)

    duplicates = len(jobs_a) + len(jobs_b) - len(unique)
    print(f"Duplicates removed: {duplicates}")
    print(f"Unique combined:    {len(unique)}")
    print(f"Saved to:           {OUTPUT}")


if __name__ == "__main__":
    main()
