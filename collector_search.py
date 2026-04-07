"""
collector_search.py  —  Job search via DuckDuckGo (no API key, no 403)

Searches ATS sites (Greenhouse, Lever, Workday, etc.) using DuckDuckGo
site: queries and appends results to the same latest_jobs.csv / latest_jobs.md
files that collector.py uses, so both workflows can run together.
"""

import csv
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from ddgs import DDGS

# ---------------------------------------------------------------------------
# Config — shared with collector.py
# ---------------------------------------------------------------------------

SEEN_FILE = "jobs_seen_ddg.json"
CURRENT_FILE = "current_jobs_ddg.json"
OUTPUT_MD = "latest_jobs_ddg.md"
OUTPUT_CSV = "latest_jobs_ddg.csv"
WINDOW_HOURS = 24

SEARCH_QUERIES = [
    '"DevOps Engineer"',
    '"SRE" OR "Site Reliability Engineer"',
    '"Platform Engineer"',
    '"Cloud Engineer"',
    '"Infrastructure Engineer"',
    '"DevSecOps"',
    '"Cloud Infrastructure"',
    '"Reliability Engineer"',
]

# ATS sites to search
SITES = [
    "job-boards.greenhouse.io",
    "boards.greenhouse.io",
    "jobs.lever.co",
    "apply.workable.com",
    "jobs.ashbyhq.com",
    "jobs.smartrecruiters.com",
]

# How many DDG results per (site + query) combination
RESULTS_PER_QUERY = 10

# ---------------------------------------------------------------------------
# Shared helpers (duplicated from collector.py to keep files independent)
# ---------------------------------------------------------------------------

_EXCLUDED_TITLE_WORDS = [
    "architect", "staff ", "staff/", "(staff",
    "principal", "manager", "director", "vp ", "vp,", "vp/",
    "vice president", "head of", "intern", "internship",
    "distinguished", "fellow", "executive",
]

_NON_US_MARKERS = [
    "europe", "eu only", "uk only", "united kingdom", "england", "london",
    "germany", "berlin", "munich", "münchen", "hamburg", "frankfurt", "cologne",
    "köln", "dusseldorf", "düsseldorf", "stuttgart", "dortmund", "bremen",
    "bochum", "potsdam", "darmstadt", "nuremberg", "nürnberg", "hannover",
    "france", "paris", "lyon", "marseille",
    "netherlands", "amsterdam", "rotterdam",
    "spain", "madrid", "barcelona", "seville",
    "poland", "warsaw", "krakow",
    "sweden", "stockholm", "gothenburg",
    "denmark", "copenhagen",
    "norway", "oslo",
    "finland", "helsinki",
    "austria", "vienna",
    "switzerland", "zurich", "zürich", "geneva",
    "belgium", "brussels",
    "portugal", "lisbon",
    "italy", "milan", "rome", "turin",
    "czechia", "prague", "czech republic",
    "romania", "bucharest",
    "hungary", "budapest",
    "ukraine", "kyiv",
    "russia", "moscow",
    "serbia", "belgrade", "slovenia", "ljubljana",
    "croatia", "zagreb", "slovakia", "bratislava", "bulgaria", "sofia",
    "greece", "athens", "cyprus", "nicosia", "estonia", "tallinn",
    "latvia", "riga", "lithuania", "vilnius", "iceland", "reykjavik",
    "turkey", "istanbul", "ankara", "israel", "tel aviv", "dubai", "uae",
    "egypt", "cairo", "south africa", "nigeria", "kenya",
    "india", "bangalore", "bengaluru", "mumbai", "delhi", "hyderabad", "pune", "chennai",
    "latam", "latin america",
    "apac", "australia", "sydney", "melbourne", "brisbane",
    "new zealand", "auckland",
    "canada only",
    "brazil", "são paulo", "sao paulo",
    "mexico only", "mexico city",
    "asia", "singapore", "japan", "tokyo", "osaka",
    "south korea", "seoul",
    "china", "beijing", "shanghai",
    "taiwan", "taipei",
    "indonesia", "jakarta",
    "thailand", "bangkok",
    "vietnam", "ho chi minh",
    "philippines", "manila",
    "montenegro", "remoto", "relocation to",
]

_HYBRID_MARKERS = ["hybrid", "on-site", "onsite", "in office", "in-office", "office"]
_MINNESOTA_MARKERS = ["minnesota", "minneapolis", "mn,", " mn ", "saint paul", "st. paul", "st paul"]

CSV_FIELDS = [
    "collected_at_utc", "source_type", "company", "title",
    "location", "team", "url", "external_id", "posted_at",
    "description_snippet",
]


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


_HQ_RE = re.compile(r"headquarters?\s*[:：]\s*([^\n.]+)", re.IGNORECASE)


def is_us_eligible(location: str, description: str = "") -> bool:
    loc = normalize(location)

    # Block non-US locations
    for marker in _NON_US_MARKERS:
        if marker in loc:
            return False

    # Hybrid/on-site only if Minnesota
    if any(m in loc for m in _HYBRID_MARKERS):
        return any(m in loc for m in _MINNESOTA_MARKERS)

    # Check description for "Headquarters: <city>" pattern
    if description:
        m = _HQ_RE.search(description)
        if m:
            hq = normalize(m.group(1))
            for marker in _NON_US_MARKERS:
                if marker in hq:
                    return False

    # Block if description snippet reveals non-US location (for empty-location DDG results)
    if description and not loc:
        desc_norm = normalize(description)
        for marker in _NON_US_MARKERS:
            if marker in desc_norm:
                return False

    return True


def clean_title(raw: str) -> str:
    for sep in [" | ", " - ", " – ", " — "]:
        if sep in raw:
            parts = raw.split(sep)
            return max(parts, key=len).strip()
    return raw.strip()


def extract_company(url: str, site: str) -> str:
    try:
        after = url.lower().split(site.lower())[-1].strip("/")
        part = after.split("/")[0]
        return part if part and part not in ("jobs", "postings", "apply") else ""
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# DuckDuckGo search
# ---------------------------------------------------------------------------

def search_ddg(site: str, query: str, max_results: int = RESULTS_PER_QUERY) -> List[Dict]:
    full_query = f"site:{site} {query}"
    results = []
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(full_query, max_results=max_results, timelimit='d'):
                results.append(r)
        time.sleep(0.5)  # be polite
    except Exception as e:
        print(f"  DDG [{site}] query failed: {e}")
    return results


def collect_all() -> List[Dict[str, Any]]:
    jobs = []
    seen_urls: set = set()

    for site in SITES:
        print(f"\nSearching: {site}")
        for query in SEARCH_QUERIES:
            results = search_ddg(site, query)
            print(f"  [{query[:40]}] → {len(results)} results")

            for r in results:
                url = (r.get("href") or r.get("url") or "").strip()
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                raw_title = r.get("title", "")
                title = clean_title(raw_title)
                snippet = re.sub(r"\s+", " ", r.get("body", "")).strip()[:300]
                company = extract_company(url, site)

                jobs.append({
                    "source_type": f"ddg:{site}",
                    "company": company,
                    "title": title,
                    "location": "",
                    "team": "",
                    "categories": [],
                    "url": url,
                    "external_id": f"ddg::{url}",
                    "description_snippet": snippet,
                    "posted_at": "",
                })

    return jobs


# ---------------------------------------------------------------------------
# Output — append to shared files
# ---------------------------------------------------------------------------

def write_csv(jobs: List[Dict[str, Any]]) -> None:
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(jobs)


def write_markdown(jobs: List[Dict[str, Any]]) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Jobs via DuckDuckGo Search",
        "",
        f"Generated: {now}",
        f"Total jobs (24h window): {len(jobs)}",
        "",
    ]

    if not jobs:
        lines.append("No matching jobs found.")
    else:
        grouped: Dict[str, List] = {}
        for job in jobs:
            grouped.setdefault(job["source_type"], []).append(job)

        for source in sorted(grouped.keys()):
            lines.append(f"## {source}")
            lines.append("")
            for job in grouped[source]:
                lines.append(f"- [{job['title'] or 'Untitled'}]({job['url']})")
                lines.append(f"  - Company: {job['company'] or 'Unknown'}")
                if job.get("description_snippet"):
                    lines.append(f"  - {job['description_snippet'][:120]}...")
                lines.append("")

    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    seen = load_json_file(SEEN_FILE, {})
    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - WINDOW_HOURS * 3600

    seen = {
        k: v for k, v in seen.items()
        if datetime.fromisoformat(
            v["first_seen_utc"].replace("Z", "+00:00")
        ).timestamp() >= cutoff
    }

    current_jobs: List[Dict[str, Any]] = load_json_file(CURRENT_FILE, [])
    current_jobs = [
        j for j in current_jobs
        if datetime.fromisoformat(
            j["collected_at_utc"].replace("Z", "+00:00")
        ).timestamp() >= cutoff
    ]
    current_ids = {j["external_id"] for j in current_jobs}

    print("Collecting jobs via DuckDuckGo...")
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
            "company": job["company"],
            "title": job["title"],
            "url": job["url"],
            "first_seen_utc": job["collected_at_utc"],
        }

    print(f"New jobs (not seen before): {len(new_jobs)}")

    # Merge into rolling window
    current_jobs = current_jobs + [j for j in new_jobs if j["external_id"] not in current_ids]

    write_csv(new_jobs)
    write_markdown(current_jobs)
    save_json_file(CURRENT_FILE, current_jobs)
    save_json_file(SEEN_FILE, seen)

    print(json.dumps({
        "new_from_ddg": len(new_jobs),
        "total_in_window": len(current_jobs),
    }, indent=2))


if __name__ == "__main__":
    main()
