"""
serpSearch/collector_search_serp.py  —  Job search via SerpAPI (Google Search)

Requires env var: SERP_API_KEY
Free tier: 100 searches/month at serpapi.com

Outputs to serpSearch/ folder:
  serpSearch/latest_jobs_serp.csv
  serpSearch/latest_jobs_serp.md
  serpSearch/jobs_seen_serp.json
  serpSearch/current_jobs_serp.json
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

SEEN_FILE    = os.path.join(BASE_DIR, "jobs_seen_serp.json")
CURRENT_FILE = os.path.join(BASE_DIR, "current_jobs_serp.json")
OUTPUT_MD    = os.path.join(BASE_DIR, "latest_jobs_serp.md")
OUTPUT_CSV   = os.path.join(BASE_DIR, "latest_jobs_serp.csv")
WINDOW_HOURS = 24

SERP_API_KEY = os.environ.get("SERP_API_KEY", "")

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

SITES = [
    "jobs.lever.co",
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
    "jobs.ashbyhq.com",
    "apply.workable.com",
    "jobs.smartrecruiters.com",
]

RESULTS_PER_QUERY = 10

# ---------------------------------------------------------------------------
# Helpers (same as ducksearch)
# ---------------------------------------------------------------------------

_EXCLUDED_TITLE_WORDS = [
    "architect", "staff ", "staff/", "(staff",
    "principal", "manager", "director", "vp ", "vp,", "vp/",
    "vice president", "head of", "intern", "internship",
    "distinguished", "fellow", "executive",
]

_NON_US_MARKERS = [
    "europe", "eu only", "uk only", "united kingdom", "england", "london",
    "germany", "berlin", "munich", "münchen", "hamburg", "frankfurt",
    "france", "paris", "netherlands", "amsterdam", "spain", "madrid",
    "poland", "warsaw", "sweden", "stockholm", "denmark", "copenhagen",
    "norway", "oslo", "finland", "helsinki", "austria", "vienna",
    "switzerland", "zurich", "belgium", "brussels", "portugal", "lisbon",
    "italy", "milan", "rome", "romania", "bucharest", "hungary", "budapest",
    "ukraine", "kyiv", "russia", "moscow", "turkey", "istanbul",
    "israel", "tel aviv", "dubai", "uae", "india", "bangalore", "mumbai",
    "delhi", "hyderabad", "latam", "latin america", "apac", "australia",
    "sydney", "melbourne", "new zealand", "canada only", "brazil",
    "mexico only", "asia", "singapore", "japan", "tokyo", "south korea",
    "seoul", "china", "beijing", "shanghai", "remoto", "relocation to",
]

_HYBRID_MARKERS    = ["hybrid", "on-site", "onsite", "in office", "in-office", "office"]
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
# SerpAPI search
# ---------------------------------------------------------------------------

def search_serp(site: str, query: str, max_results: int = RESULTS_PER_QUERY) -> List[Dict]:
    """Search via SerpAPI Google Search endpoint."""
    if not SERP_API_KEY:
        print("  ERROR: SERP_API_KEY not set")
        return []

    full_query = f'site:{site} {query} ("remote" OR "united states")'
    results = []

    try:
        resp = requests.get(
            "https://serpapi.com/search",
            params={
                "api_key": SERP_API_KEY,
                "engine":  "google",
                "q":       full_query,
                "num":     max_results,
                "gl":      "us",
                "hl":      "en",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("organic_results", []):
            results.append({
                "url":   item.get("link", ""),
                "title": item.get("title", ""),
                "body":  item.get("snippet", ""),
            })

        time.sleep(0.5)

    except requests.exceptions.HTTPError as e:
        print(f"  SerpAPI error [{site}]: {e} — {resp.text[:200]}")
    except Exception as e:
        print(f"  SerpAPI search failed [{site}]: {e}")

    return results


def collect_all() -> List[Dict[str, Any]]:
    jobs = []
    seen_urls: set = set()

    for site in SITES:
        print(f"\nSearching: {site}")
        for query in SEARCH_QUERIES:
            results = search_serp(site, query)
            print(f"  [{query[:40]}] → {len(results)} results")

            for r in results:
                url = r.get("url", "").strip()
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                title   = clean_title(r.get("title", ""))
                snippet = re.sub(r"\s+", " ", r.get("body", "")).strip()[:300]
                company = extract_company(url, site)

                jobs.append({
                    "source_type":         f"serp:{site}",
                    "company":             company,
                    "title":               title,
                    "location":            "",
                    "team":                "",
                    "categories":          [],
                    "url":                 url,
                    "external_id":         f"serp::{url}",
                    "description_snippet": snippet,
                    "posted_at":           "",
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
        "# Jobs via SerpAPI (Google Search)",
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
    if not SERP_API_KEY:
        print("ERROR: Set SERP_API_KEY environment variable.")
        print("Get a free key at: https://serpapi.com/manage-api-key")
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

    print("Collecting jobs via SerpAPI...")
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

    print(json.dumps({"new_from_serp": len(new_jobs), "total_in_window": len(current_jobs)}, indent=2))


if __name__ == "__main__":
    main()
