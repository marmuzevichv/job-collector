"""
collector_search_google.py  —  Job search via Google Custom Search API

Mirrors collector_search.py (DuckDuckGo) but uses Google CSE.
Requires env vars: GOOGLE_API_KEY, GOOGLE_CSE_ID

Outputs to separate files (never mixed with DDG results):
  latest_jobs_google.csv
  latest_jobs_google.md
  jobs_seen_google.json
  current_jobs_google.json
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

OUTPUT_DIR = "google"
os.makedirs(OUTPUT_DIR, exist_ok=True)

SEEN_FILE = os.path.join(OUTPUT_DIR, "jobs_seen_google.json")
CURRENT_FILE = os.path.join(OUTPUT_DIR, "current_jobs_google.json")
OUTPUT_MD = os.path.join(OUTPUT_DIR, "latest_jobs_google.md")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "latest_jobs_google.csv")
WINDOW_HOURS = 24

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")
GOOGLE_CSE_ID = os.environ.get("GOOGLE_CSE_ID", "")

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

# ATS sites to search (same as DDG pipeline)
SITES = [
    "jobs.lever.co",
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
    "jobs.ashbyhq.com",
    "apply.workable.com",
    "jobs.smartrecruiters.com",
    "wellfound.com/jobs",
]

# Google CSE returns max 10 per request; we fetch 1 page per (site + query)
RESULTS_PER_QUERY = 10

# ---------------------------------------------------------------------------
# Shared helpers (mirrored from collector_search.py)
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
# Google Custom Search
# ---------------------------------------------------------------------------

def search_google(site: str, query: str, max_results: int = RESULTS_PER_QUERY) -> List[Dict]:
    """Call Google Custom Search API for site:X query."""
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        print("  ERROR: GOOGLE_API_KEY or GOOGLE_CSE_ID not set")
        return []

    full_query = f'site:{site} {query} ("remote" OR "united states" OR "US")'
    results = []

    try:
        # Google CSE returns max 10 per page; fetch up to max_results (1 page)
        params = {
            "key": GOOGLE_API_KEY,
            "cx": GOOGLE_CSE_ID,
            "q": full_query,
            "num": min(max_results, 10),
        }
        resp = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("items", []):
            results.append({
                "url": item.get("link", ""),
                "title": item.get("title", ""),
                "body": item.get("snippet", ""),
            })

        time.sleep(0.3)  # stay within quota

    except requests.exceptions.HTTPError as e:
        print(f"  Google API error [{site}]: {e}")
    except Exception as e:
        print(f"  Google search failed [{site}]: {e}")

    return results


def collect_all() -> List[Dict[str, Any]]:
    jobs = []
    seen_urls: set = set()

    for site in SITES:
        print(f"\nSearching: {site}")
        for query in SEARCH_QUERIES:
            results = search_google(site, query)
            print(f"  [{query[:40]}] → {len(results)} results")

            for r in results:
                url = r.get("url", "").strip()
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                raw_title = r.get("title", "")
                title = clean_title(raw_title)
                snippet = re.sub(r"\s+", " ", r.get("body", "")).strip()[:300]
                company = extract_company(url, site)

                jobs.append({
                    "source_type": f"google:{site}",
                    "company": company,
                    "title": title,
                    "location": "",
                    "team": "",
                    "categories": [],
                    "url": url,
                    "external_id": f"google::{url}",
                    "description_snippet": snippet,
                    "posted_at": "",
                })

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
        "# Jobs via Google Custom Search",
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
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        print("ERROR: Set GOOGLE_API_KEY and GOOGLE_CSE_ID environment variables.")
        return

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

    print("Collecting jobs via Google Custom Search...")
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

    current_jobs = current_jobs + [j for j in new_jobs if j["external_id"] not in current_ids]

    write_csv(new_jobs)
    write_markdown(current_jobs)
    save_json_file(CURRENT_FILE, current_jobs)
    save_json_file(SEEN_FILE, seen)

    print(json.dumps({
        "new_from_google": len(new_jobs),
        "total_in_window": len(current_jobs),
    }, indent=2))


if __name__ == "__main__":
    main()
