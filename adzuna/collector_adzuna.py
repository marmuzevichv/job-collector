"""
adzuna/collector_adzuna.py  —  Job search via Adzuna API

Requires env vars: ADZUNA_APP_ID, ADZUNA_APP_KEY
Free tier: 1000 requests/day

Outputs to adzuna/ folder:
  adzuna/latest_jobs_adzuna.csv
  adzuna/latest_jobs_adzuna.md
  adzuna/latest_jobs_ranked_adzuna.md
  adzuna/jobs_seen_adzuna.json
  adzuna/current_jobs_adzuna.json
"""

import csv
import json
import os
import re
import time
import requests
from datetime import datetime, timezone
from typing import Any, Dict, List

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SEEN_FILE    = os.path.join(BASE_DIR, "jobs_seen_adzuna.json")
CURRENT_FILE = os.path.join(BASE_DIR, "current_jobs_adzuna.json")
OUTPUT_MD    = os.path.join(BASE_DIR, "latest_jobs_adzuna.md")
OUTPUT_CSV   = os.path.join(BASE_DIR, "latest_jobs_adzuna.csv")
WINDOW_HOURS = 24

ADZUNA_APP_ID  = os.environ.get("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY", "")

# Search keywords
SEARCH_QUERIES = [
    "DevOps Engineer remote",
    "Site Reliability Engineer remote",
    "Platform Engineer remote",
    "Cloud Engineer remote",
    "Infrastructure Engineer remote",
    "DevSecOps remote",
    "SRE Engineer remote",
    "Kubernetes Engineer remote",
]

RESULTS_PER_QUERY = 50  # Adzuna supports up to 50 per page
PAGES_PER_QUERY   = 2   # fetch 2 pages = 100 results per query

CSV_FIELDS = [
    "collected_at_utc", "source_type", "company", "title",
    "location", "team", "url", "external_id", "posted_at",
    "description_snippet",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EXCLUDED_TITLE_WORDS = [
    "architect", "staff ", "staff/", "(staff",
    "principal", "manager", "director", "vp ", "vp,", "vp/",
    "vice president", "head of", "intern", "internship",
    "distinguished", "fellow", "executive",
]

_NON_US_MARKERS = [
    "europe", "eu only", "uk only", "united kingdom", "england", "london",
    "germany", "berlin", "france", "paris", "netherlands", "amsterdam",
    "spain", "madrid", "poland", "warsaw", "sweden", "stockholm",
    "denmark", "copenhagen", "norway", "oslo", "finland", "helsinki",
    "austria", "vienna", "switzerland", "zurich", "belgium", "brussels",
    "portugal", "lisbon", "italy", "milan", "rome", "romania", "bucharest",
    "ukraine", "kyiv", "russia", "moscow", "turkey", "istanbul",
    "israel", "tel aviv", "dubai", "uae", "india", "bangalore", "mumbai",
    "delhi", "hyderabad", "latam", "australia", "sydney", "melbourne",
    "new zealand", "canada only", "brazil", "mexico only", "asia",
    "singapore", "japan", "tokyo", "south korea", "seoul", "china",
    "beijing", "shanghai", "remoto", "relocation to",
]

_HYBRID_MARKERS    = ["hybrid", "on-site", "onsite", "in office", "in-office", "office"]
_MINNESOTA_MARKERS = ["minnesota", "minneapolis", "mn,", " mn ", "saint paul", "st. paul", "st paul"]

_HQ_RE = re.compile(r"headquarters?\s*[:：]\s*([^\n.]+)", re.IGNORECASE)


def load_json_file(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json_file(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def is_excluded_title(title: str) -> bool:
    t = normalize(title)
    return any(w in t for w in _EXCLUDED_TITLE_WORDS)


def is_us_eligible(location: str, description: str = "") -> bool:
    loc = normalize(location)
    for marker in _NON_US_MARKERS:
        if marker in loc:
            return False
    if any(m in loc for m in _HYBRID_MARKERS):
        return any(m in loc for m in _MINNESOTA_MARKERS)
    if description:
        m = _HQ_RE.search(description)
        if m:
            hq = normalize(m.group(1))
            for marker in _NON_US_MARKERS:
                if marker in hq:
                    return False
    return True


# ---------------------------------------------------------------------------
# Adzuna API
# ---------------------------------------------------------------------------

def search_adzuna(query: str, page: int = 1) -> List[Dict]:
    """Search Adzuna US jobs API."""
    results = []
    try:
        resp = requests.get(
            f"https://api.adzuna.com/v1/api/jobs/us/search/{page}",
            params={
                "app_id":           ADZUNA_APP_ID,
                "app_key":          ADZUNA_APP_KEY,
                "what":             query,
                "content-type":     "application/json",
                "results_per_page": RESULTS_PER_QUERY,
                "what_and":         "remote",
                "sort_by":          "date",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        for job in data.get("results", []):
            results.append({
                "url":     job.get("redirect_url", ""),
                "title":   job.get("title", ""),
                "company": job.get("company", {}).get("display_name", ""),
                "location": job.get("location", {}).get("display_name", ""),
                "snippet": job.get("description", "")[:300],
                "posted_at": job.get("created", "")[:10],
                "id":      str(job.get("id", "")),
            })

        time.sleep(0.3)

    except requests.exceptions.HTTPError as e:
        print(f"  Adzuna error [{query}]: {e} — {resp.text[:200]}")
    except Exception as e:
        print(f"  Adzuna search failed [{query}]: {e}")

    return results


def collect_all() -> List[Dict[str, Any]]:
    jobs = []
    seen_urls: set = set()

    for query in SEARCH_QUERIES:
        print(f"\nSearching: {query}")
        total = 0
        for page in range(1, PAGES_PER_QUERY + 1):
            results = search_adzuna(query, page)
            for r in results:
                url = r.get("url", "").strip()
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                jobs.append({
                    "source_type":         "adzuna",
                    "company":             r.get("company", ""),
                    "title":               r.get("title", ""),
                    "location":            r.get("location", ""),
                    "team":                "",
                    "categories":          [],
                    "url":                 url,
                    "external_id":         f"adzuna::{r.get('id', url)}",
                    "description_snippet": r.get("snippet", ""),
                    "posted_at":           r.get("posted_at", ""),
                })
                total += 1

        print(f"  → {total} results")

    return jobs


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_csv(jobs: List[Dict[str, Any]]) -> None:
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(jobs)


def write_markdown(jobs: List[Dict[str, Any]]) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Jobs via Adzuna API",
        "",
        f"Generated: {now}",
        f"Total jobs (24h window): {len(jobs)}",
        "",
    ]
    if not jobs:
        lines.append("No matching jobs found.")
    else:
        for job in jobs:
            lines.append(f"- [{job['title'] or 'Untitled'}]({job['url']})")
            lines.append(f"  - Company: {job['company'] or 'Unknown'} | {job.get('location','')}")
            if job.get("description_snippet"):
                lines.append(f"  - {job['description_snippet'][:120]}...")
            lines.append("")
    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        print("ERROR: Set ADZUNA_APP_ID and ADZUNA_APP_KEY environment variables.")
        return

    seen = load_json_file(SEEN_FILE, {})
    now  = datetime.now(timezone.utc)
    cutoff = now.timestamp() - WINDOW_HOURS * 3600

    seen = {
        k: v for k, v in seen.items()
        if datetime.fromisoformat(v["first_seen_utc"].replace("Z", "+00:00")).timestamp() >= cutoff
    }

    current_jobs: List[Dict[str, Any]] = load_json_file(CURRENT_FILE, [])
    current_jobs = [
        j for j in current_jobs
        if datetime.fromisoformat(j["collected_at_utc"].replace("Z", "+00:00")).timestamp() >= cutoff
    ]
    current_ids = {j["external_id"] for j in current_jobs}

    print("Collecting jobs via Adzuna...")
    raw_jobs = collect_all()
    print(f"\nTotal fetched: {len(raw_jobs)}")

    new_jobs = []
    for job in raw_jobs:
        if job["external_id"] in seen:
            continue
        if is_excluded_title(job.get("title", "")):
            continue
        if not is_us_eligible(job.get("location", ""), job.get("description_snippet", "")):
            continue

        job["collected_at_utc"] = now.strftime("%Y-%m-%d %H:%M:%S")
        new_jobs.append(job)
        seen[job["external_id"]] = {
            "company":        job["company"],
            "title":          job["title"],
            "url":            job["url"],
            "first_seen_utc": job["collected_at_utc"],
        }

    print(f"New jobs (not seen before): {len(new_jobs)}")

    current_jobs = current_jobs + [j for j in new_jobs if j["external_id"] not in current_ids]

    write_csv(new_jobs)
    write_markdown(current_jobs)
    save_json_file(CURRENT_FILE, current_jobs)
    save_json_file(SEEN_FILE, seen)

    print(json.dumps({"new_from_adzuna": len(new_jobs), "total_in_window": len(current_jobs)}, indent=2))


if __name__ == "__main__":
    main()
