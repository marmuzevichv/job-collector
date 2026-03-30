import csv
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests

CONFIG_FILE = "sources.json"
SEEN_FILE = "jobs_seen.json"
CURRENT_FILE = "current_jobs.json"
OUTPUT_MD = "latest_jobs.md"
OUTPUT_CSV = "latest_jobs.csv"
TIMEOUT = 30
WINDOW_HOURS = 24


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


def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", text).strip()


def matches_keywords(job: Dict[str, Any], keywords: List[str]) -> bool:
    haystack = " ".join([
        normalize(str(job.get("title", ""))),
        normalize(str(job.get("team", ""))),
        normalize(" ".join(job.get("categories", []))),
    ])
    return any(normalize(k) in haystack for k in keywords)


# Locations that clearly exclude US candidates
_EXCLUDED_TITLE_WORDS = [
    "architect", "staff ", "staff/", "(staff",
    "principal", "manager", "director", "vp ", "vp,", "vp/",
    "vice president", "head of", "intern", "internship",
    "distinguished", "fellow", "executive",
]

def is_excluded_title(title: str) -> bool:
    t = normalize(title)
    return any(w in t for w in _EXCLUDED_TITLE_WORDS)


_NON_US_MARKERS = [
    "europe", "eu only", "uk only", "germany", "berlin", "munich", "hamburg",
    "bochum", "potsdam", "france", "paris", "netherlands", "amsterdam",
    "spain", "madrid", "barcelona", "poland", "warsaw", "sweden", "stockholm",
    "denmark", "norway", "finland", "austria", "vienna", "switzerland",
    "belgium", "portugal", "italy", "milan", "rome", "czechia", "prague",
    "czech republic", "romania", "hungary", "india", "bangalore", "mumbai",
    "latam", "latin america", "apac", "australia", "sydney", "melbourne",
    "new zealand", "canada only", "brazil", "mexico only", "mexico city",
    "asia", "singapore", "japan", "tokyo", "south korea", "seoul",
    "montenegro", "relocation to",
]

_US_MARKERS = [
    "united states", "usa", " us ", "u.s.", "us-", "remote us",
    "us remote", "north america", "worldwide", "global", "anywhere",
    "remote", "",  # empty location = assume open
]


def is_us_eligible(location: str) -> bool:
    loc = normalize(location)
    if not loc:
        return True  # no location = assume open/remote
    for marker in _NON_US_MARKERS:
        if marker in loc:
            return False
    return True


def safe_get(session: requests.Session, url: str) -> Any:
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# RemoteOK  https://remoteok.com/api
# ---------------------------------------------------------------------------

def collect_remoteok(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    time.sleep(1)  # RemoteOK rate-limit courtesy
    data = safe_get(session, "https://remoteok.com/api")
    jobs = []
    for item in data:
        if not isinstance(item, dict) or "id" not in item or "legal" in item:
            continue
        tags = item.get("tags", []) or []
        desc = strip_html(item.get("description", ""))[:300]
        jobs.append({
            "source_type": "remoteok",
            "company": (item.get("company") or "").strip(),
            "title": (item.get("position") or "").strip(),
            "location": (item.get("location") or "Worldwide").strip(),
            "team": "",
            "categories": tags,
            "url": (item.get("url") or "").strip(),
            "external_id": f"remoteok::{item['id']}",
            "description_snippet": desc,
            "posted_at": item.get("date", ""),
        })
    return jobs


# ---------------------------------------------------------------------------
# We Work Remotely  https://weworkremotely.com  (RSS)
# ---------------------------------------------------------------------------

WWR_FEEDS = [
    "https://weworkremotely.com/categories/remote-devops-sysadmin-jobs.rss",
    "https://weworkremotely.com/categories/remote-back-end-programming-jobs.rss",
]


def collect_weworkremotely(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    jobs = []
    seen_urls: set = set()

    for feed_url in WWR_FEEDS:
        resp = session.get(feed_url, timeout=TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        for item in root.findall(".//item"):
            def text(tag: str) -> str:
                el = item.find(tag)
                return (el.text or "").strip() if el is not None else ""

            raw_title = text("title")
            guid = text("guid")
            pub_date = text("pubDate")
            desc = strip_html(text("description"))[:300]

            # WWR title format: "Region: Company: Job Title"
            parts = raw_title.split(": ", 2)
            if len(parts) == 3:
                _, company, title = parts
            elif len(parts) == 2:
                company, title = parts
            else:
                company, title = "", raw_title

            url = guid or ""
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            jobs.append({
                "source_type": "weworkremotely",
                "company": company.strip(),
                "title": title.strip(),
                "location": "Remote",
                "team": "",
                "categories": [],
                "url": url,
                "external_id": f"wwr::{url}",
                "description_snippet": desc,
                "posted_at": pub_date,
            })

    return jobs


# ---------------------------------------------------------------------------
# Jobicy  https://jobicy.com/api/v2/remote-jobs
# ---------------------------------------------------------------------------

def collect_jobicy(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    tags = source.get("tags", ["devops"])
    seen_ids: set = set()
    jobs = []

    for tag in tags:
        url = f"https://jobicy.com/api/v2/remote-jobs?count=50&tag={tag}"
        try:
            data = safe_get(session, url)
        except Exception as e:
            print(f"  Jobicy tag '{tag}' failed: {e}")
            continue

        for item in data.get("jobs", []):
            job_id = str(item.get("id", ""))
            if not job_id or job_id in seen_ids:
                continue
            seen_ids.add(job_id)

            industry = item.get("jobIndustry") or ""
            if isinstance(industry, list):
                industry = ", ".join(str(i) for i in industry)

            jobs.append({
                "source_type": "jobicy",
                "company": (item.get("companyName") or "").strip(),
                "title": (item.get("jobTitle") or "").strip(),
                "location": (item.get("jobGeo") or "Remote").strip(),
                "team": industry.strip(),
                "categories": [str(item.get("jobType", ""))],
                "url": (item.get("url") or "").strip(),
                "external_id": f"jobicy::{job_id}",
                "description_snippet": strip_html(item.get("jobExcerpt", ""))[:300],
                "posted_at": item.get("pubDate", ""),
            })

    return jobs


# ---------------------------------------------------------------------------
# arbeitnow  https://www.arbeitnow.com/api/job-board-api
# ---------------------------------------------------------------------------

def collect_arbeitnow(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = safe_get(session, "https://www.arbeitnow.com/api/job-board-api")
    jobs = []

    for item in data.get("data", []):
        tags = item.get("tags", []) or []
        location = "Remote" if item.get("remote") else (item.get("location") or "").strip()
        jobs.append({
            "source_type": "arbeitnow",
            "company": (item.get("company_name") or "").strip(),
            "title": (item.get("title") or "").strip(),
            "location": location,
            "team": ", ".join(tags[:3]),
            "categories": tags,
            "url": (item.get("url") or "").strip(),
            "external_id": f"arbeitnow::{item.get('slug', '')}",
            "description_snippet": strip_html(item.get("description", ""))[:300],
            "posted_at": str(item.get("created_at", "")),
        })

    return jobs


# ---------------------------------------------------------------------------
# Lever  https://api.lever.co/v0/postings/{company}
# ---------------------------------------------------------------------------

def collect_lever(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    companies = source.get("companies", [])
    jobs = []

    for company in companies:
        url = f"https://api.lever.co/v0/postings/{company}?mode=json"
        try:
            data = safe_get(session, url)
        except Exception:
            continue
        if not isinstance(data, list):
            continue

        for item in data:
            cats = item.get("categories") or {}
            location = cats.get("location") or cats.get("allLocations", [""])[0] if isinstance(cats.get("allLocations"), list) else ""
            jobs.append({
                "source_type": "lever",
                "company": item.get("company", company).strip(),
                "title": (item.get("text") or "").strip(),
                "location": (location or "").strip(),
                "team": (cats.get("team") or "").strip(),
                "categories": [cats.get("department", "")],
                "url": (item.get("hostedUrl") or "").strip(),
                "external_id": f"lever::{item.get('id', '')}",
                "description_snippet": strip_html(item.get("descriptionPlain", ""))[:300],
                "posted_at": str(item.get("createdAt", "")),
            })

    return jobs


# ---------------------------------------------------------------------------
# Greenhouse  https://boards-api.greenhouse.io/v1/boards/{company}/jobs
# ---------------------------------------------------------------------------

def collect_greenhouse(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    companies = source.get("companies", [])
    jobs = []

    for company in companies:
        url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
        try:
            data = safe_get(session, url)
        except Exception:
            continue

        for item in data.get("jobs", []):
            offices = item.get("offices") or []
            location = offices[0].get("name", "") if offices else ""
            departments = item.get("departments") or []
            team = departments[0].get("name", "") if departments else ""
            jobs.append({
                "source_type": "greenhouse",
                "company": company,
                "title": (item.get("title") or "").strip(),
                "location": location.strip(),
                "team": team.strip(),
                "categories": [],
                "url": (item.get("absolute_url") or "").strip(),
                "external_id": f"greenhouse::{item.get('id', '')}",
                "description_snippet": "",
                "posted_at": item.get("updated_at", ""),
            })

    return jobs


# ---------------------------------------------------------------------------
# Remotive  https://remotive.com/api/remote-jobs
# ---------------------------------------------------------------------------

def collect_remotive(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    categories = source.get("categories", ["devops-sysadmin"])
    seen_ids: set = set()
    jobs = []

    for cat in categories:
        url = f"https://remotive.com/api/remote-jobs?category={cat}"
        try:
            data = safe_get(session, url)
        except Exception as e:
            print(f"  Remotive category '{cat}' failed: {e}")
            continue

        for item in data.get("jobs", []):
            job_id = str(item.get("id", ""))
            if not job_id or job_id in seen_ids:
                continue
            seen_ids.add(job_id)

            jobs.append({
                "source_type": "remotive",
                "company": (item.get("company_name") or "").strip(),
                "title": (item.get("title") or "").strip(),
                "location": (item.get("candidate_required_location") or "Remote").strip(),
                "team": (item.get("category") or "").strip(),
                "categories": [item.get("job_type", "")],
                "url": (item.get("url") or "").strip(),
                "external_id": f"remotive::{job_id}",
                "description_snippet": strip_html(item.get("description", ""))[:300],
                "posted_at": item.get("publication_date", ""),
            })

    return jobs


# ---------------------------------------------------------------------------
# Bing Web Search API  →  any ATS site
# ---------------------------------------------------------------------------

_cse_globally_failed = False


def collect_bing_search(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    global _cse_globally_failed
    if _cse_globally_failed:
        print("  Skipping: Bing search failed earlier")
        return []

    api_key = os.environ.get("BING_API_KEY", "")
    if not api_key:
        print("  Skipping: BING_API_KEY not set")
        return []

    site = source["site"]
    pages = source.get("pages", 3)
    query = f"site:{site} ({_GOOGLE_TITLE_KEYWORDS})"

    jobs = []
    seen_urls: set = set()

    for page in range(pages):
        offset = page * 10
        try:
            resp = session.get(
                "https://api.bing.microsoft.com/v7.0/search",
                headers={"Ocp-Apim-Subscription-Key": api_key},
                params={"q": query, "count": 10, "offset": offset, "mkt": "en-US"},
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (401, 403):
                _cse_globally_failed = True
                print(f"  Bing [{site}] auth failed — stopping all Bing sources")
            else:
                print(f"  Bing [{site}] page {page + 1} failed: {e}")
            break

        items = data.get("webPages", {}).get("value", [])
        if not items:
            break

        for item in items:
            job_url = (item.get("url") or "").strip()
            if not job_url or job_url in seen_urls:
                continue
            seen_urls.add(job_url)

            title = _clean_google_title(item.get("name", ""))
            snippet = re.sub(r"\s+", " ", item.get("snippet", "")).strip()
            company = _extract_company(job_url, site)

            jobs.append({
                "source_type": f"bing:{site}",
                "company": company,
                "title": title,
                "location": "",
                "team": "",
                "categories": [],
                "url": job_url,
                "external_id": f"bing::{job_url}",
                "description_snippet": snippet[:300],
                "posted_at": "",
                "_pre_filtered": True,
            })

        time.sleep(0.3)

    return jobs


# ---------------------------------------------------------------------------
# Google Custom Search API  →  any ATS site
# ---------------------------------------------------------------------------

# One combined query per site to save quota (100 free calls/day)
_GOOGLE_TITLE_KEYWORDS = (
    '"DevOps" OR "SRE" OR "Site Reliability" OR "Platform Engineer" '
    'OR "Cloud Engineer" OR "Infrastructure Engineer" OR "DevSecOps" '
    'OR "MLOps" OR "Cloud Architect" OR "Reliability Engineer"'
)


def _extract_company(url: str, site: str) -> str:
    """Pull company slug from ATS URL path."""
    try:
        after = url.lower().split(site.lower())[-1].strip("/")
        part = after.split("/")[0]
        return part if part and part != "jobs" else ""
    except Exception:
        return ""


def _clean_google_title(raw: str) -> str:
    """'Company - Job Title | Lever' → 'Job Title'"""
    for sep in [" | ", " - ", " – "]:
        if sep in raw:
            parts = raw.split(sep)
            # longest part is usually the actual title
            return max(parts, key=len).strip()
    return raw.strip()


def collect_google_cse(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    api_key = os.environ.get("GOOGLE_API_KEY", "")
    cx = os.environ.get("GOOGLE_CX", "")
    if not api_key or not cx:
        print("  Skipping: GOOGLE_API_KEY or GOOGLE_CX not set")
        return []

    site = source["site"]
    pages = source.get("pages", 3)
    query = f"site:{site} ({_GOOGLE_TITLE_KEYWORDS})"

    jobs = []
    seen_urls: set = set()

    for page in range(pages):
        start = page * 10 + 1
        endpoint = (
            "https://www.googleapis.com/customsearch/v1"
            f"?key={api_key}&cx={cx}"
            f"&q={requests.utils.quote(query)}&start={start}"
        )
        try:
            data = safe_get(session, endpoint)
        except Exception as e:
            print(f"  Google CSE [{site}] page {page + 1} failed: {e}")
            break

        items = data.get("items", [])
        if not items:
            break

        for item in items:
            job_url = (item.get("link") or "").strip()
            if not job_url or job_url in seen_urls:
                continue
            seen_urls.add(job_url)

            raw_title = item.get("title", "")
            title = _clean_google_title(raw_title)
            snippet = re.sub(r"\s+", " ", item.get("snippet", "")).strip()
            company = _extract_company(job_url, site)

            jobs.append({
                "source_type": f"google:{site}",
                "company": company,
                "title": title,
                "location": "",
                "team": "",
                "categories": [],
                "url": job_url,
                "external_id": f"google::{job_url}",
                "description_snippet": snippet[:300],
                "posted_at": "",
                "_pre_filtered": True,  # Google already matched keywords
            })

        time.sleep(0.3)

    return jobs


# ---------------------------------------------------------------------------
# The Muse  https://www.themuse.com/api/public/jobs
# ---------------------------------------------------------------------------

def collect_themuse(session: requests.Session, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    url = "https://www.themuse.com/api/public/jobs?category=Engineering&page=0"
    data = safe_get(session, url)
    jobs = []

    for item in data.get("results", []):
        locations = item.get("locations", []) or []
        location = ", ".join(loc.get("name", "") for loc in locations if loc.get("name"))
        levels = item.get("levels", []) or []
        level = ", ".join(lv.get("name", "") for lv in levels if lv.get("name"))
        company = (item.get("company") or {}).get("name", "")
        refs = item.get("refs", {}) or {}
        job_url = refs.get("landing_page", "")

        jobs.append({
            "source_type": "themuse",
            "company": company.strip(),
            "title": (item.get("name") or "").strip(),
            "location": location or "Remote",
            "team": level,
            "categories": [],
            "url": job_url.strip(),
            "external_id": f"themuse::{item.get('id', '')}",
            "description_snippet": strip_html(item.get("contents", ""))[:300],
            "posted_at": item.get("publication_date", ""),
        })

    return jobs


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def sort_jobs(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        jobs,
        key=lambda x: (
            normalize(x.get("company", "")),
            normalize(x.get("title", "")),
        ),
    )


CSV_FIELDS = [
    "collected_at_utc",
    "source_type",
    "company",
    "title",
    "location",
    "team",
    "url",
    "external_id",
    "posted_at",
    "description_snippet",
]


def write_csv(jobs: List[Dict[str, Any]]) -> None:
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(jobs)


def write_markdown(jobs: List[Dict[str, Any]]) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# New jobs",
        "",
        f"Generated: {now}",
        "",
        f"Total new jobs: {len(jobs)}",
        "",
    ]

    if not jobs:
        lines.append("No new matching jobs found.")
    else:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for job in jobs:
            grouped.setdefault(job["source_type"], []).append(job)

        for source in sorted(grouped.keys()):
            lines.append(f"## {source}")
            lines.append("")
            for job in grouped[source]:
                title = job["title"] or "Untitled"
                location = job["location"] or "Remote"
                company = job["company"] or "Unknown company"
                lines.append(f"- [{title}]({job['url']})")
                lines.append(f"  - Company: {company}")
                lines.append(f"  - Location: {location}")
                if job.get("team"):
                    lines.append(f"  - Level/Team: {job['team']}")
                lines.append("")

    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

COLLECTORS = {
    "remoteok": collect_remoteok,
    "weworkremotely": collect_weworkremotely,
    "jobicy": collect_jobicy,
    "arbeitnow": collect_arbeitnow,
    "themuse": collect_themuse,
    "lever": collect_lever,
    "greenhouse": collect_greenhouse,
    "remotive": collect_remotive,
    "bing_search": collect_bing_search,
    "google_cse": collect_google_cse,
}


def main() -> None:
    config = load_json_file(CONFIG_FILE, {"keywords": [], "sources": []})
    seen = load_json_file(SEEN_FILE, {})
    keywords = config.get("keywords", [])
    sources = config.get("sources", [])

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; JobCollector/1.0; +https://github.com/)"
    })

    us_only = config.get("us_only", False)
    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - WINDOW_HOURS * 3600

    # Drop seen entries older than WINDOW_HOURS so jobs reappear after 24h
    seen = {
        k: v for k, v in seen.items()
        if datetime.fromisoformat(v["first_seen_utc"].replace("Z", "+00:00")).timestamp() >= cutoff
    }

    # Load rolling window and drop jobs older than WINDOW_HOURS
    current_jobs: List[Dict[str, Any]] = load_json_file(CURRENT_FILE, [])
    current_jobs = [
        j for j in current_jobs
        if datetime.fromisoformat(j["collected_at_utc"].replace("Z", "+00:00")).timestamp() >= cutoff
    ]
    current_ids = {j["external_id"] for j in current_jobs}

    all_new_jobs: List[Dict[str, Any]] = []
    stats = {"checked_sources": 0, "found_total": 0, "new_total": 0}

    for source in sources:
        source_type = source.get("type")
        collector = COLLECTORS.get(source_type)
        if not collector:
            print(f"Skipping unsupported source: {source_type}")
            continue

        print(f"Collecting: {source.get('name', source_type)} ...")
        stats["checked_sources"] += 1

        try:
            jobs = collector(session, source)
        except Exception as e:
            print(f"  Failed: {e}")
            continue

        stats["found_total"] += len(jobs)
        print(f"  Fetched {len(jobs)} jobs")

        for job in jobs:
            if not job.get("url") or not job.get("external_id"):
                continue
            if not job.pop("_pre_filtered", False) and not matches_keywords(job, keywords):
                continue
            if is_excluded_title(job.get("title", "")):
                continue
            if us_only and not is_us_eligible(job.get("location", "")):
                continue
            if job["external_id"] in seen:
                continue

            job["collected_at_utc"] = now.strftime("%Y-%m-%d %H:%M:%S")
            all_new_jobs.append(job)
            seen[job["external_id"]] = {
                "company": job["company"],
                "title": job["title"],
                "url": job["url"],
                "first_seen_utc": job["collected_at_utc"],
            }

    all_new_jobs = sort_jobs(all_new_jobs)
    stats["new_total"] = len(all_new_jobs)

    # Merge new jobs into rolling window and write full 24h list
    current_jobs = sort_jobs(current_jobs + [j for j in all_new_jobs if j["external_id"] not in current_ids])
    stats["window_total"] = len(current_jobs)

    write_csv(current_jobs)
    write_markdown(current_jobs)
    save_json_file(CURRENT_FILE, current_jobs)
    save_json_file(SEEN_FILE, seen)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
