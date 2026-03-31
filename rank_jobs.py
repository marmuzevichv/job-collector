"""
rank_jobs.py  —  Rank jobs from latest_jobs_combined.csv using Claude AI
Outputs latest_jobs_ranked.md with top matches for the resume.
"""

import csv
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List

import anthropic

INPUT_FILE = "latest_jobs_combined.csv"
OUTPUT_FILE = "latest_jobs_ranked.md"
TOP_CANDIDATES = 80   # pre-filter before sending to Claude
TOP_RANKED = 30       # how many to show in final output

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


def read_jobs() -> List[Dict[str, Any]]:
    if not os.path.exists(INPUT_FILE):
        print(f"File not found: {INPUT_FILE}")
        return []
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def rank_with_claude(jobs: List[Dict[str, Any]]) -> str:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    jobs_text = ""
    for i, job in enumerate(jobs, 1):
        jobs_text += (
            f"{i}. [{job.get('title', 'N/A')}] at {job.get('company', 'N/A')}\n"
            f"   Location: {job.get('location') or 'Remote'}\n"
            f"   URL: {job.get('url', '')}\n"
            f"   Info: {job.get('description_snippet', '')[:200]}\n\n"
        )

    prompt = f"""You are a job matching assistant. Below is a candidate's resume summary and a list of job postings.

RESUME:
{RESUME}

JOB POSTINGS:
{jobs_text}

TASK:
1. Score each job 1-10 based on fit with the resume (10 = perfect match)
2. Return TOP {TOP_RANKED} jobs sorted by score (highest first)
3. For each job include: score, title, company, location, URL, and 1 sentence why it fits
4. Skip any job that is clearly not DevOps/SRE/Platform/Cloud/Infrastructure

Format EXACTLY like this for each job:
### [SCORE/10] Job Title — Company
- Location: ...
- URL: ...
- Why: ...

Only return the ranked list, no intro text."""

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


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
    print(f"Sending top {len(candidates)} to Claude for ranking...")

    ranked_text = rank_with_claude(candidates)
    write_markdown(ranked_text, len(all_jobs), len(candidates))

    print("Done!")


if __name__ == "__main__":
    main()
