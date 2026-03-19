#!/usr/bin/env python3
"""Scrape student information from the Tsinghua Storage Research Group team page."""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString, Tag

BASE_URL = "https://storage.cs.tsinghua.edu.cn/"
PEOPLE_URL = urljoin(BASE_URL, "team/")
TARGET_GROUPS = {"Students"}
PROFILE_SECTIONS = {
    "bio": "bio",
    "education": "education",
    "selected awards": "selected_awards",
    "teaching": "teaching",
}
USER_AGENT = (
    "Mozilla/5.0 (compatible; TsinghuaStorageStudentScraper/1.0; "
    "+https://storage.cs.tsinghua.edu.cn/)"
)
SEARCH_TARGETS = ("scholar", "github", "linkedin", "personal")
SEARCH_EXCLUDED_DOMAINS = (
    "google.com",
    "scholar.google.com",
    "github.com",
    "linkedin.com",
    "madsys.cs.tsinghua.edu.cn",
    "cs.tsinghua.edu.cn",
    "tsinghua.edu.cn",
)
DEFAULT_SCHOOL_ALIASES = {
    "THU": ["THU", "Tsinghua", "Tsinghua University"],
}
COMPETING_SCHOOL_TOKENS = (
    "sjtu",
    "shanghai jiao tong",
    "pku",
    "peking university",
    "zju",
    "zhejiang university",
    "fudan",
    "ustc",
    "university of science and technology of china",
    "nju",
    "nanjing university",
    "bit",
    "beijing institute of technology",
)


@dataclass
class StudentRecord:
    name: str
    student_type: str
    profile_url: str = ""
    lab: str = ""
    bio: str = ""
    education: str = ""
    selected_awards: str = ""
    teaching: str = ""
    email: str = ""
    google_scholar_url: str = ""
    github_url: str = ""
    linkedin_url: str = ""
    personal_website_url: str = ""
    official_external_links: str = ""
    google_search_query: str = ""
    google_search_url: str = ""
    scholar_search_url: str = ""
    github_search_url: str = ""
    linkedin_search_url: str = ""
    personal_search_url: str = ""
    external_search_status: str = ""
    external_search_provider: str = ""

    def as_row(self) -> dict[str, str]:
        return {
            "name": self.name,
            "student_type": self.student_type,
            "profile_url": self.profile_url,
            "lab": self.lab,
            "bio": self.bio,
            "education": self.education,
            "selected_awards": self.selected_awards,
            "teaching": self.teaching,
            "email": self.email,
            "google_scholar_url": self.google_scholar_url,
            "github_url": self.github_url,
            "linkedin_url": self.linkedin_url,
            "personal_website_url": self.personal_website_url,
            "official_external_links": self.official_external_links,
            "google_search_query": self.google_search_query,
            "google_search_url": self.google_search_url,
            "scholar_search_url": self.scholar_search_url,
            "github_search_url": self.github_search_url,
            "linkedin_search_url": self.linkedin_search_url,
            "personal_search_url": self.personal_search_url,
            "external_search_status": self.external_search_status,
            "external_search_provider": self.external_search_provider,
        }


def normalize_text(value: str) -> str:
    return " ".join(value.split())


def contains_term(haystack: str, needle: str) -> bool:
    escaped = re.escape(needle.lower())
    return re.search(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", haystack.lower()) is not None


def text_from_node(node: Tag | NavigableString | None) -> str:
    if node is None:
        return ""
    if isinstance(node, NavigableString):
        return normalize_text(str(node))
    return normalize_text(node.get_text(" ", strip=True))


def fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=20)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding
    return response.text


def iter_section_blocks(heading: Tag) -> Iterable[Tag]:
    for sibling in heading.next_siblings:
        if isinstance(sibling, NavigableString):
            continue
        if sibling.name in {"h1", "h2", "h3", "h4"}:
            break
        yield sibling


def flatten_blocks(blocks: Iterable[Tag]) -> str:
    pieces: list[str] = []
    for block in blocks:
        if block.name in {"ul", "ol"}:
            items = block.find_all("li", recursive=False)
            if not items:
                text = text_from_node(block)
                if text:
                    pieces.append(text)
                continue

            for item in items:
                text = text_from_node(item)
                if text:
                    pieces.append(text)
            continue

        text = text_from_node(block)
        if text:
            pieces.append(text)

    return " | ".join(dict.fromkeys(pieces))


def decode_obfuscated_email(text: str) -> str:
    candidate = normalize_text(text)
    candidate = re.sub(r"(?i)email\s*:\s*", "", candidate)
    candidate = re.sub(r"(?i)\bat\b", "@", candidate)
    candidate = re.sub(r"(?i)\bdot\b", ".", candidate)
    candidate = candidate.replace(" ", "")
    match = re.search(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})", candidate)
    return match.group(1) if match else ""


def parse_profile(session: requests.Session, profile_url: str) -> dict[str, str]:
    soup = BeautifulSoup(fetch_html(session, profile_url), "lxml")
    result = {
        "lab": "",
        "bio": "",
        "education": "",
        "selected_awards": "",
        "teaching": "",
        "email": "",
        "google_scholar_url": "",
        "github_url": "",
        "linkedin_url": "",
        "personal_website_url": profile_url,
        "official_external_links": profile_url,
    }
    
    lab_tag = soup.select_one('a[href="/"]')
    if lab_tag:
        result["lab"] = text_from_node(lab_tag)
    
    email_p = None
    for p in soup.select('p'):
        text = text_from_node(p)
        if 'email' in text.lower() or 'mail' in text.lower() or '@' in text:
            email_p = p
            break
    
    if email_p:
        result["email"] = decode_obfuscated_email(text_from_node(email_p))
    
    external_links: list[str] = [profile_url]
    for link in soup.select('a[href]'):
        href = (link.get("href") or "").strip()
        if not href:
            continue
        if href.startswith("mailto:"):
            result["email"] = href.removeprefix("mailto:")
            continue

        if href.startswith("#") or href == "/" or href.startswith("/"):
            continue

        absolute_url = urljoin(profile_url, href)
        lowered = absolute_url.lower()
        if lowered.rstrip("/") == profile_url.lower().rstrip("/"):
            continue
        if "storage.cs.tsinghua.edu.cn" in lowered and "/~" not in lowered:
            continue
        if "scholar.google." in lowered and not result["google_scholar_url"]:
            result["google_scholar_url"] = absolute_url
            external_links.append(absolute_url)
        elif "github.com" in lowered and not result["github_url"]:
            result["github_url"] = absolute_url
            external_links.append(absolute_url)
        elif "linkedin.com" in lowered and not result["linkedin_url"]:
            result["linkedin_url"] = absolute_url
            external_links.append(absolute_url)
        elif "storage.cs.tsinghua.edu.cn" not in lowered and "tsinghua.edu.cn" not in lowered:
            result["personal_website_url"] = absolute_url
            external_links.append(absolute_url)

    result["official_external_links"] = " | ".join(dict.fromkeys(external_links))
    
    section_mapping = {
        "about me": "bio",
        "education": "education",
        "honors awarded": "selected_awards",
        "awards": "selected_awards",
        "teaching": "teaching",
    }
    
    for heading in soup.find_all('h3'):
        title = text_from_node(heading).lower()
        if title not in section_mapping:
            continue
        
        field = section_mapping[title]
        current_elem = heading.next_sibling
        content_pieces: list[str] = []
        
        while current_elem is not None:
            if isinstance(current_elem, Tag) and current_elem.name == 'h3':
                break
            
            if isinstance(current_elem, Tag) and current_elem.name == 'hr':
                current_elem = current_elem.next_sibling
                continue
            
            text = text_from_node(current_elem)
            if text:
                content_pieces.append(text)
            
            current_elem = current_elem.next_sibling
        
        result[field] = " ".join(content_pieces)
    
    return result


def build_search_urls(name: str, school_abbr: str) -> dict[str, str]:
    base_query = f'"{name}" "{school_abbr}"'
    scholar_query = f'{base_query} site:scholar.google.com/citations'
    github_query = f'{base_query} site:github.com'
    linkedin_query = f'{base_query} site:linkedin.com/in'
    personal_query = f'{base_query} ("homepage" OR "personal website" OR "cv")'
    return {
        "google_search_query": base_query,
        "google_search_url": f"https://www.google.com/search?q={quote_plus(base_query)}",
        "scholar_search_url": f"https://www.google.com/search?q={quote_plus(scholar_query)}",
        "github_search_url": f"https://www.google.com/search?q={quote_plus(github_query)}",
        "linkedin_search_url": f"https://www.google.com/search?q={quote_plus(linkedin_query)}",
        "personal_search_url": f"https://www.google.com/search?q={quote_plus(personal_query)}",
    }


def get_email_handle(email: str) -> str:
    if "@" not in email:
        return ""
    return email.split("@", 1)[0].strip().lower()


def get_school_aliases(school_abbr: str) -> list[str]:
    aliases = DEFAULT_SCHOOL_ALIASES.get(school_abbr.upper(), [school_abbr])
    return list(dict.fromkeys(alias.strip() for alias in aliases if alias.strip()))


def get_name_variants(name: str) -> list[str]:
    parts = [part for part in name.split() if part]
    variants = [name]
    if len(parts) >= 2:
        variants.append(f"{parts[0]} {parts[-1]}")
        variants.append(" ".join(parts[::-1]))
    return list(dict.fromkeys(variants))


def detect_search_provider(requested_provider: str) -> str:
    if requested_provider != "auto":
        return requested_provider
    if os.getenv("SERPAPI_API_KEY"):
        return "serpapi"
    if os.getenv("GOOGLE_CSE_API_KEY") and os.getenv("GOOGLE_CSE_CX"):
        return "google_cse"
    return "none"


def fetch_search_results(
    session: requests.Session,
    provider: str,
    query: str,
    num_results: int,
) -> list[dict[str, str]]:
    if provider == "serpapi":
        response = session.get(
            "https://serpapi.com/search",
            params={
                "engine": "google",
                "q": query,
                "api_key": os.environ["SERPAPI_API_KEY"],
                "num": num_results,
                "hl": "en",
                "google_domain": "google.com",
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        return [
            {
                "title": item.get("title", ""),
                "link": item.get("link", ""),
                "snippet": item.get("snippet", ""),
            }
            for item in payload.get("organic_results", [])
            if item.get("link")
        ]

    if provider == "google_cse":
        response = session.get(
            "https://customsearch.googleapis.com/customsearch/v1",
            params={
                "key": os.environ["GOOGLE_CSE_API_KEY"],
                "cx": os.environ["GOOGLE_CSE_CX"],
                "q": query,
                "num": min(num_results, 10),
                "hl": "en",
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        return [
            {
                "title": item.get("title", ""),
                "link": item.get("link", ""),
                "snippet": item.get("snippet", ""),
            }
            for item in payload.get("items", [])
            if item.get("link")
        ]

    return []


def fetch_github_api_candidates(
    session: requests.Session,
    student_name: str,
    email_handle: str,
) -> list[dict[str, str]]:
    queries = [f'{student_name} in:fullname type:user']
    if email_handle:
        queries.append(f'{email_handle} in:login type:user')

    candidates: list[dict[str, str]] = []
    seen_links: set[str] = set()
    for query in queries:
        headers = {"Accept": "application/vnd.github+json"}
        if os.getenv("GITHUB_TOKEN"):
            headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"
        response = session.get(
            "https://api.github.com/search/users",
            params={"q": query, "per_page": 5},
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        for item in payload.get("items", []):
            link = item.get("html_url", "")
            if not link or link in seen_links:
                continue
            seen_links.add(link)
            candidates.append(
                {
                    "title": item.get("login", ""),
                    "link": link,
                    "snippet": item.get("type", ""),
                }
            )
    return candidates


def result_matches_name(result: dict[str, str], student_name: str) -> bool:
    haystack = normalize_text(
        " ".join([result.get("title", ""), result.get("snippet", ""), result.get("link", "")]).lower()
    )
    return all(part.lower() in haystack for part in student_name.split())


def normalize_candidate_url(target: str, url: str) -> str:
    if target == "github":
        trimmed = url.split("?", 1)[0].rstrip("/")
        if "github.com/" not in trimmed:
            return trimmed
        path = trimmed.split("github.com/", 1)[1]
        login = path.split("/", 1)[0]
        if not login:
            return ""
        if login.lower() in {"features", "topics", "orgs", "organizations", "search", "login", "join"}:
            return ""
        return f"https://github.com/{login}"
    return url


def score_candidate(
    result: dict[str, str],
    student_name: str,
    school_aliases: list[str],
    target: str,
    email_handle: str,
) -> int:
    link = normalize_candidate_url(target, result.get("link", ""))
    if not link:
        return -10_000

    title = normalize_text(result.get("title", "")).lower()
    snippet = normalize_text(result.get("snippet", "")).lower()
    lowered = f"{title} {snippet} {link.lower()}"

    score = 0
    parts = [part.lower() for part in student_name.split() if part]
    if all(part in lowered for part in parts):
        score += 60
    elif any(part in lowered for part in parts):
        score += 15

    if student_name.lower() in lowered:
        score += 40

    for alias in school_aliases:
        if contains_term(lowered, alias):
            score += 20

    has_school_alias = any(contains_term(lowered, alias) for alias in school_aliases)
    if not has_school_alias:
        for token in COMPETING_SCHOOL_TOKENS:
            if contains_term(lowered, token):
                score -= 80 if target == "github" else 45
                break

    if email_handle and email_handle in lowered:
        score += 35

    if target == "scholar":
        if "scholar.google.com/citations" in link.lower():
            score += 80
        if "user=" in link.lower():
            score += 20
    elif target == "github":
        if link.count("/") == 3:
            score += 80
        if "/issues/" in link.lower() or "/pull/" in link.lower() or "/commit/" in link.lower():
            score -= 80
        if link.lower().count("/") > 3:
            score -= 25
    elif target == "linkedin":
        if "linkedin.com/in/" in link.lower():
            score += 70
        if "linkedin.com/company/" in link.lower():
            score -= 60
    elif target == "personal":
        if any(domain in link.lower() for domain in SEARCH_EXCLUDED_DOMAINS):
            score -= 100
        if any(host in link.lower() for host in ("github.io", ".me/", ".dev/", ".page/", ".pages.dev", ".site/")):
            score += 25
        if any(token in link.lower() for token in ("homepage", "home", "about", "bio", "cv")):
            score += 10

    return score


def choose_candidate_url(
    results: list[dict[str, str]],
    student_name: str,
    target: str,
    school_aliases: list[str],
    email_handle: str,
) -> str:
    best_url = ""
    best_score = -10_000

    for result in results:
        normalized_url = normalize_candidate_url(target, result.get("link", ""))
        if not normalized_url:
            continue
        result = {**result, "link": normalized_url}
        score = score_candidate(result, student_name, school_aliases, target, email_handle)
        if score > best_score:
            best_score = score
            best_url = normalized_url

    minimum_scores = {
        "scholar": 80,
        "github": 85,
        "linkedin": 80,
        "personal": 70,
    }
    if best_score < minimum_scores.get(target, 60):
        return ""
    return best_url


def build_target_queries(student: StudentRecord, target: str, school_aliases: list[str]) -> list[str]:
    name_variants = get_name_variants(student.name)
    email_handle = get_email_handle(student.email)
    queries: list[str] = []

    for alias in school_aliases:
        for name in name_variants:
            if target == "scholar":
                queries.append(f'"{name}" "{alias}" site:scholar.google.com/citations')
                queries.append(f'"{name}" "{alias}" "Google Scholar"')
            elif target == "github":
                queries.append(f'"{name}" "{alias}" site:github.com')
                queries.append(f'"{name}" "{alias}" GitHub')
            elif target == "linkedin":
                queries.append(f'"{name}" "{alias}" site:linkedin.com/in')
                queries.append(f'"{name}" "{alias}" LinkedIn')
            elif target == "personal":
                queries.append(f'"{name}" "{alias}" ("homepage" OR "personal website" OR "cv")')
                queries.append(f'"{name}" "{alias}" ("bio" OR "about me")')

    if email_handle:
        if target == "github":
            queries.append(f'"{email_handle}" site:github.com')
        elif target == "personal":
            queries.append(f'"{email_handle}" ("homepage" OR "cv")')

    return list(dict.fromkeys(query for query in queries if query.strip()))


def enrich_external_links(
    session: requests.Session,
    students: list[StudentRecord],
    provider: str,
    school_abbr: str,
    num_results: int,
    delay_seconds: float,
) -> None:
    if provider == "none":
        return

    for student in students:
        school_aliases = get_school_aliases(school_abbr)
        email_handle = get_email_handle(student.email)
        current_values = {
            "scholar": student.google_scholar_url,
            "github": student.github_url,
            "linkedin": student.linkedin_url,
            "personal": student.personal_website_url,
        }

        found_any = False
        for target in SEARCH_TARGETS:
            if current_values[target]:
                continue
            aggregated_results: list[dict[str, str]] = []
            if target == "github":
                try:
                    aggregated_results.extend(
                        fetch_github_api_candidates(session, student.name, email_handle)
                    )
                except requests.RequestException:
                    pass

            if provider != "none":
                for query in build_target_queries(student, target, school_aliases):
                    try:
                        results = fetch_search_results(session, provider, query, num_results)
                    except (requests.RequestException, KeyError, ValueError) as exc:
                        student.external_search_provider = provider
                        student.external_search_status = f"api_error:{type(exc).__name__}"
                        aggregated_results = []
                        break
                    aggregated_results.extend(results)
                    time.sleep(delay_seconds)

            if not aggregated_results and provider == "none":
                continue
            try:
                candidate = choose_candidate_url(
                    aggregated_results,
                    student.name,
                    target,
                    school_aliases,
                    email_handle,
                )
            except ValueError as exc:
                student.external_search_provider = provider
                student.external_search_status = f"api_error:{type(exc).__name__}"
                break
            if not candidate:
                continue

            if target == "scholar":
                student.google_scholar_url = candidate
            elif target == "github":
                student.github_url = candidate
            elif target == "linkedin":
                student.linkedin_url = candidate
            elif target == "personal":
                student.personal_website_url = candidate

            found_any = True

        resolved_provider = provider
        if provider == "none" and (student.github_url or student.google_scholar_url or student.linkedin_url or student.personal_website_url):
            resolved_provider = "github_api"
        student.external_search_provider = resolved_provider
        if found_any:
            merged_links = []
            if student.official_external_links:
                merged_links.extend(student.official_external_links.split(" | "))
            merged_links.extend(
                [
                    link
                    for link in [
                        student.google_scholar_url,
                        student.github_url,
                        student.linkedin_url,
                        student.personal_website_url,
                    ]
                    if link
                ]
            )
            if merged_links:
                student.official_external_links = " | ".join(dict.fromkeys(merged_links))
            if student.external_search_status == "official_links_found":
                student.external_search_status = "official_links_found+api_links_found"
            else:
                student.external_search_status = "api_links_found"
        elif student.external_search_status == "search_urls_generated":
            student.external_search_status = "api_no_match"


def parse_people_page(session: requests.Session) -> list[StudentRecord]:
    soup = BeautifulSoup(fetch_html(session, PEOPLE_URL), "lxml")
    students: list[StudentRecord] = []
    students_h2 = None
    for h2 in soup.find_all('h2'):
        if 'students' in h2.get_text().lower():
            students_h2 = h2
            break

    if students_h2 is None:
        raise RuntimeError("Could not locate the Students section.")

    current_elem = students_h2.next_sibling
    while current_elem is not None:
        if isinstance(current_elem, Tag) and current_elem.name == 'h2':
            break

        if isinstance(current_elem, Tag) and current_elem.name == 'div' and 'row' in current_elem.get('class', []):
            student_cards = current_elem.select('.col-sm-3')
            for card in student_cards:
                name_tag = card.select_one('h4 a.memberlink')
                if name_tag:
                    name = text_from_node(name_tag)
                    profile_url = urljoin(BASE_URL, name_tag.get('href', ''))
                    student_type = ""
                    for paragraph in card.select("p"):
                        text = text_from_node(paragraph)
                        if text:
                            student_type = text
                            break

                    students.append(
                        StudentRecord(
                            name=name,
                            student_type=student_type,
                            profile_url=profile_url,
                            lab="Storage Research Group",
                        )
                    )

        current_elem = current_elem.next_sibling

    return students


def build_dataset(
    session: requests.Session,
    school_abbr: str,
    search_provider: str,
    search_num_results: int,
    search_delay: float,
) -> list[StudentRecord]:
    students = parse_people_page(session)
    for student in students:
        search_urls = build_search_urls(student.name, school_abbr)
        student.google_search_query = search_urls["google_search_query"]
        student.google_search_url = search_urls["google_search_url"]
        student.scholar_search_url = search_urls["scholar_search_url"]
        student.github_search_url = search_urls["github_search_url"]
        student.linkedin_search_url = search_urls["linkedin_search_url"]
        student.personal_search_url = search_urls["personal_search_url"]
        student.external_search_status = "search_urls_generated"

        if not student.profile_url:
            continue
        try:
            details = parse_profile(session, student.profile_url)
        except requests.RequestException as exc:
            print(f"Warning: failed to fetch {student.profile_url}: {exc}", file=sys.stderr)
            continue

        student.lab = details["lab"]
        student.bio = details["bio"]
        student.education = details["education"]
        student.selected_awards = details["selected_awards"]
        student.teaching = details["teaching"]
        student.email = details["email"]
        student.google_scholar_url = details["google_scholar_url"]
        student.github_url = details["github_url"]
        student.linkedin_url = details["linkedin_url"]
        student.personal_website_url = details["personal_website_url"]
        student.official_external_links = details["official_external_links"]
        if any(
            [
                student.google_scholar_url,
                student.github_url,
                student.linkedin_url,
                student.personal_website_url,
            ]
        ):
            student.external_search_status = "official_links_found"

    enrich_external_links(
        session=session,
        students=students,
        provider=search_provider,
        school_abbr=school_abbr,
        num_results=search_num_results,
        delay_seconds=search_delay,
    )

    return students


def export_csv(records: list[StudentRecord], output_path: Path) -> None:
    rows = [record.as_row() for record in records]
    with output_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def export_xlsx(records: list[StudentRecord], output_path: Path) -> bool:
    try:
        import openpyxl  # noqa: F401
    except ImportError:
        return False

    dataframe = pd.DataFrame([record.as_row() for record in records])
    dataframe.to_excel(output_path, index=False)
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--school-abbr",
        default="THU",
        help='School abbreviation used in generated search queries. Default: %(default)s',
    )
    parser.add_argument(
        "--output",
        default="storage_students.csv",
        help="Path to the CSV output file. Default: %(default)s",
    )
    parser.add_argument(
        "--xlsx",
        default="storage_students.xlsx",
        help="Optional XLSX output file. Default: %(default)s",
    )
    parser.add_argument(
        "--skip-xlsx",
        action="store_true",
        help="Only export CSV, even if openpyxl is installed.",
    )
    parser.add_argument(
        "--search-provider",
        choices=("auto", "none", "serpapi", "google_cse"),
        default="auto",
        help="External search backend. Default: %(default)s",
    )
    parser.add_argument(
        "--search-num-results",
        type=int,
        default=5,
        help="Max results fetched per external search query. Default: %(default)s",
    )
    parser.add_argument(
        "--search-delay",
        type=float,
        default=0.2,
        help="Delay between external search requests in seconds. Default: %(default)s",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    provider = detect_search_provider(args.search_provider)
    records = build_dataset(
        session,
        args.school_abbr,
        provider,
        args.search_num_results,
        args.search_delay,
    )
    if not records:
        raise RuntimeError("No students were scraped from the Storage Research Group team page.")

    csv_path = Path(args.output).resolve()
    export_csv(records, csv_path)
    print(f"CSV exported: {csv_path}")

    if not args.skip_xlsx:
        xlsx_path = Path(args.xlsx).resolve()
        if export_xlsx(records, xlsx_path):
            print(f"XLSX exported: {xlsx_path}")
        else:
            print("Skipped XLSX export because openpyxl is not installed.")

    print(f"External search provider: {provider}")
    print(f"Total students exported: {len(records)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
