"""
rank_jobs.py  —  Rank jobs from latest_jobs_combined.csv using Claude AI
Outputs latest_jobs_ranked.md with top matches for the resume.
"""

import csv
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List

import anthropic
import requests

import sys
INPUT_FILE  = sys.argv[1] if len(sys.argv) > 1 else "latest_jobs_combined.csv"
OUTPUT_FILE = sys.argv[2] if len(sys.argv) > 2 else "latest_jobs_ranked.md"
TOP_CANDIDATES = 200  # pre-filter before sending to Claude
TOP_RANKED = 100      # how many to show in final output

RESUME = """
Slava Marmuzevich — DevOps Engineer, Minneapolis MN, Green Card Holder.

6+ years DevOps experience at Fortune 100 companies (Walgreens, CHS Inc., DTN).

KEY SKILLS:
- Cloud: AWS (EKS, EC2, RDS, S3, Lambda, IAM, CloudWatch, Route53), Azure (AKS, Key Vault, DevOps), GCP basic
- Containers: Kubernetes, Docker, Helm, Argo CD, OpenShift, KEDA, Istio
- IaC: Terraform, Ansible, CloudFormation, GitOps (Argo CD, Flux)
- CI/CD: GitHub Actions, Jenkins, Azure DevOps, GitLab CI/CD, Bitbucket Pipelines
- Monitoring: Prometheus, Grafana, Loki, EFK/ELK, Datadog, New Relic
- Security: FedRAMP, Vault, Secrets Manager, IAM, RBAC, SSO, OIDC, Trivy, Checkov
- Languages: Python, Bash, PowerShell, YAML, SQL
- Certifications: CKA, CKAD, AWS SAA, Terraform Associate, AZ-104

TARGET ROLES: DevOps Engineer, SRE, Platform Engineer, Cloud Engineer, Infrastructure Engineer, DevSecOps
LOCATION: Remote (US only) or Hybrid in Minnesota
NOT interested in: Lead, Manager, Director, GRC, QA Automation, Test Engineer roles
"""

# Keywords for pre-filtering — must match at least one
_INCLUDE_KEYWORDS = [
    "devops", "site reliability", "sre", "platform engineer", "cloud engineer",
    "infrastructure engineer", "devsecops", "cloud infrastructure", "reliability engineer",
    "kubernetes", "terraform", "aws", "azure", "ci/cd", "gitops", "helm",
    "ansible", "github actions", "argo", "prometheus", "grafana",
]

# Titles to skip before sending to Claude
_EXCLUDE_TITLES = [
    "lead", "manager", "director", "vp", "vice president", "head of",
    "principal", "staff", "intern", "grc", "test engineer", "qa ",
    "automation engineer", "security engineer", "data engineer",
    "machine learning", "ml engineer", "software engineer",
    "frontend", "backend", "fullstack", "full stack", "full-stack",
]


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def is_relevant(job: Dict[str, Any]) -> bool:
    title = normalize(job.get("title", ""))
    if any(w in title for w in _EXCLUDE_TITLES):
        return False
    haystack = title + " " + normalize(job.get("description_snippet", "")) + " " + normalize(job.get("team", ""))
    return any(k in haystack for k in _INCLUDE_KEYWORDS)


def score_simple(job: Dict[str, Any]) -> int:
    """Quick keyword score for pre-ranking before sending to Claude."""
    haystack = normalize(" ".join([
        job.get("title", ""),
        job.get("description_snippet", ""),
        job.get("team", ""),
    ]))
    score = 0
    high_value = ["kubernetes", "terraform", "aws", "azure", "github actions",
                  "sre", "platform engineer", "devops engineer", "argo", "helm"]
    med_value = ["ci/cd", "prometheus", "grafana", "ansible", "vault",
                 "cloud", "infrastructure", "devsecops"]
    for k in high_value:
        if k in haystack:
            score += 3
    for k in med_value:
        if k in haystack:
            score += 1
    return score


_CLOSED_PHRASES = [
    "no longer open", "no longer available", "position has been filled",
    "job has been closed", "this job is closed", "posting has expired",
    "requisition is closed", "role has been filled", "not accepting applications",
    "position is no longer", "job is no longer",
    # Ashby "Job not found" page
    "job not found", "the job you requested was not found",
    # Workable not_found redirect
    "not_found=true",
]

# URL patterns that indicate a closed/error page
_CLOSED_URL_PATTERNS = [
    "error=true", "not_found=true", "job_not_found",
]

# Phrases that indicate the role requires specific location (non-US country or office-required city)
_NON_US_REQUIRED_PHRASES = [
    "must be based in", "must reside in", "must live in", "must be located in",
    "only considering candidates in", "only considering applicants in",
    "candidates must be in", "applicants must be in",
    "fully living and resident in",
    "resident in romania", "resident in spain", "resident in the uk",
    "based in the uk", "based in romania", "based in spain",
    "based in germany", "based in france", "based in netherlands",
    "based in poland", "based in india", "based in canada",
    "located in the uk", "located in romania", "located in spain",
    # Office-required roles in specific US cities (candidate is in Minnesota, remote only)
    "nyc-based role", "new york city-based", "must be based in new york",
    "must be in new york", "required to work in our new york",
    "san francisco-based", "must be based in san francisco",
    "must be in the bay area", "bay area-based role",
    "must be based in seattle", "must be based in austin",
    "must be based in chicago", "must be based in los angeles",
    "in-person interview required",
]

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "Mozilla/5.0 (compatible; JobChecker/1.0)"})


def is_job_open(url: str) -> bool:
    """Return True if the job URL is reachable and not showing a closed message."""
    if not url:
        return False
    try:
        resp = _SESSION.get(url, timeout=10, allow_redirects=True)
        if resp.status_code >= 400:
            return False
        # Check final URL for error patterns (e.g. Greenhouse ?error=true)
        final_url = resp.url.lower()
        if any(p in final_url for p in _CLOSED_URL_PATTERNS):
            return False
        text = resp.text.lower()
        if any(phrase in text for phrase in _CLOSED_PHRASES):
            return False
        if any(phrase in text for phrase in _NON_US_REQUIRED_PHRASES):
            return False
        return True
    except Exception:
        return False


def verify_jobs(jobs: List[Dict[str, Any]], workers: int = 10) -> List[Dict[str, Any]]:
    """Filter out closed/unreachable jobs using parallel HTTP checks."""
    print(f"Verifying {len(jobs)} jobs (checking if still open)...")
    open_jobs = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        future_to_job = {ex.submit(is_job_open, j["url"]): j for j in jobs}
        for future in as_completed(future_to_job):
            job = future_to_job[future]
            if future.result():
                open_jobs.append(job)
    print(f"Open jobs after verification: {len(open_jobs)} (removed {len(jobs) - len(open_jobs)} closed)")
    return open_jobs


def read_jobs() -> List[Dict[str, Any]]:
    if not os.path.exists(INPUT_FILE):
        print(f"File not found: {INPUT_FILE}")
        return []
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


BATCH_SIZE = 50  # jobs per Claude API call


def rank_batch(client: anthropic.Anthropic, jobs: List[Dict[str, Any]], batch_num: int) -> List[Dict[str, Any]]:
    jobs_text = ""
    for i, job in enumerate(jobs, 1):
        jobs_text += (
            f"{i}. [{job.get('title', 'N/A')}] at {job.get('company', 'N/A')}\n"
            f"   Location: {job.get('location') or 'Remote'}\n"
            f"   URL: {job.get('url', '')}\n"
            f"   Info: {job.get('description_snippet', '')[:200]}\n\n"
        )

    prompt = f"""You are a job matching assistant. Score each job for fit with this candidate's resume.

RESUME:
{RESUME}

JOB POSTINGS:
{jobs_text}

TASK:
- Score each job 1-10 (10 = perfect match for DevOps/SRE/Platform/Cloud/Infrastructure)
- Skip jobs clearly unrelated (GRC, QA, software dev, data engineering, etc.)
- SKIP any job that is not explicitly Remote (US) or based in the United States. If location mentions any non-US country (UK, Romania, Spain, Germany, India, Canada, Netherlands, Australia, etc.) — skip it.
- SKIP any job that requires residency or physical presence outside the US.
- If location is unclear or not mentioned, assume it's OK only if the snippet or title mentions "remote", "US", "United States", or nothing at all.
- For each relevant job return score, title, company, location, URL, and 1 sentence why it fits

Format EXACTLY like this (one per job, no extra text):
### [SCORE/10] Job Title — Company
- Location: ...
- URL: ...
- Why: ..."""

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=6000,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def rank_with_claude(jobs: List[Dict[str, Any]]) -> str:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    all_results = []
    batches = [jobs[i:i + BATCH_SIZE] for i in range(0, len(jobs), BATCH_SIZE)]

    for i, batch in enumerate(batches, 1):
        print(f"  Ranking batch {i}/{len(batches)} ({len(batch)} jobs)...")
        result = rank_batch(client, batch, i)
        all_results.append(result)

    # Combine all batch results
    combined = "\n\n".join(all_results)

    # Extract scored jobs and sort by score
    entries = re.findall(r"(### \[(\d+)/10\].*?)(?=### \[|\Z)", combined, re.DOTALL)
    scored = [(int(score), block.strip()) for block, score in entries]
    scored.sort(key=lambda x: x[0], reverse=True)

    top = scored[:TOP_RANKED]
    print(f"  Total relevant jobs found: {len(scored)}, showing top {len(top)}")

    return "\n\n".join(block for _, block in top)


def write_markdown(ranked_text: str, total_input: int, candidates_sent: int) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    content = f"""# Top DevOps Jobs — Ranked by AI

Generated: {now}
Total jobs analyzed: {total_input}
Candidates sent to AI: {candidates_sent}

---

{ranked_text}
"""
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Saved to: {OUTPUT_FILE}")


def main() -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("Error: ANTHROPIC_API_KEY not set")
        return

    print(f"Reading {INPUT_FILE}...")
    all_jobs = read_jobs()
    print(f"Total jobs: {len(all_jobs)}")

    # Pre-filter by relevance
    relevant = [j for j in all_jobs if is_relevant(j)]
    print(f"After keyword filter: {len(relevant)}")

    # Sort by simple score and take top N
    relevant.sort(key=score_simple, reverse=True)
    candidates = relevant[:TOP_CANDIDATES]

    # Verify jobs are still open before sending to Claude
    candidates = verify_jobs(candidates)
    print(f"Sending top {len(candidates)} to Claude for ranking...")

    ranked_text = rank_with_claude(candidates)
    write_markdown(ranked_text, len(all_jobs), len(candidates))

    print("Done!")


if __name__ == "__main__":
    main()
