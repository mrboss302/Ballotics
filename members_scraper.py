#!/usr/bin/env python3
"""Ballotics / The Record: verified, fail-closed current congressional roster.

Source of truth: official House/Senate rosters and leadership directories.
Congress.gov member detail is used *only* for additional historical leadership.
No AI is used. Requires requests; CONGRESS_API_KEY is optional.

The existing latest-members.json shape is preserved. Additions are additive:
  member.leadership.roles, member.leadership.status, metadata.version,
  metadata.leadership, separate validation/change-audit reports.
"""
from __future__ import annotations

import argparse
import hashlib
from html.parser import HTMLParser
import json
import logging
import math
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests

LOGGER = logging.getLogger("ballotics-members")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

OUTPUT_FILE = "latest-members.json"
HOUSE_MEMBERS_URL = "https://clerk.house.gov/xml/lists/MemberData.xml"
SENATE_CVC_URL = "https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml"
SENATE_CFM_URL = "https://www.senate.gov/general/contact_information/senators_cfm.xml"
HOUSE_LEADERSHIP_URL = "https://clerk.house.gov/Members/ViewLeadership"
SENATE_LEADERSHIP_URL = "https://www.senate.gov/senators/leadership.htm"
CONGRESS_MEMBER_API = "https://api.congress.gov/v3/member/"
HEADERS = {"User-Agent": "BalloticsTheRecord/2.0 (public congressional data research; contact: app-maintainer)"}
TIMEOUT = 25
RETRIES = 3
MIN_CHAMBER_SIZE = {"House": 400, "Senate": 90}
MAX_CHAMBER_SIZE = {"House": 450, "Senate": 105}
BIOGUIDE_PATTERN = re.compile(r"^[A-Z]\d{6}$")
HOUSE_PROFILE_PATTERN = re.compile(r"/members/([A-Za-z]\d{6})(?:/)?$", re.I)
# Senate.gov leadership cards identify senators through portrait URLs such as
# /bioguide/photo/G/G000386.jpg; retain compatibility with older /bio/ links.
SENATE_BIO_PATTERN = re.compile(
    r"/(?:(?:search/)?bio|bioguide/photo/[A-Za-z])/([A-Za-z]\d{6})(?:\.(?:jpe?g|png))?/?$", re.I
)

HOUSE_TITLES = (
    "Speaker of the House", "Majority Leader", "Majority Whip",
    "Republican Conference Chair", "Republican Policy Committee Chair",
    "Minority Leader", "Minority Whip", "Democratic Caucus Chair",
    "Democratic Caucus Vice Chair",
)
SENATE_TITLES = (
    "President Pro Tempore", "Senate Majority Leader", "Majority Whip",
    "Republican Conference Chair", "Republican Policy Committee Chair",
    "Vice Chair of the Republican Conference", "Republican Senatorial Committee Chair",
    "Democratic Leader Chair of the Conference", "Democratic Whip",
    "Chair of Steering & Policy Committee", "Chair of Strategic Communications Committee",
    "Vice Chair of the Conference", "Chair of Outreach", "Democratic Conference Secretary",
    "Vice Chair of Outreach", "Deputy Democratic Conference Secretary",
)
# Titles can legitimately be held by more than one senator.
MULTIPLE_SENATE_TITLES = {"Vice Chair of the Conference", "Deputy Democratic Conference Secretary"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def congress_number(now: Optional[datetime] = None) -> int:
    year = (now or datetime.now(timezone.utc)).year
    return 119 + (year - 2025) // 2


def safe_text(node: Optional[ET.Element]) -> str:
    return (node.text or "").strip() if node is not None else ""


def safe_attr(node: Optional[ET.Element], key: str, default: str = "") -> str:
    return (node.get(key) or default) if node is not None else default


def normalize_xml(root: ET.Element) -> ET.Element:
    for el in root.iter():
        if isinstance(el.tag, str):
            el.tag = el.tag.split("}", 1)[-1].lower()
    return root


def normalize_party(value: str) -> Dict[str, str]:
    val = (value or "").strip()
    mapping = {
        "D": ("D", "Democratic"), "DEM": ("D", "Democratic"),
        "DEMOCRAT": ("D", "Democratic"), "DEMOCRATIC": ("D", "Democratic"),
        "R": ("R", "Republican"), "REP": ("R", "Republican"),
        "REPUBLICAN": ("R", "Republican"),
        "I": ("I", "Independent"), "IND": ("I", "Independent"),
        "INDEPENDENT": ("I", "Independent"),
        "ID": ("ID", "Independent Democrat"), "INDEPENDENT DEMOCRAT": ("ID", "Independent Democrat"),
        "L": ("L", "Libertarian"), "LIBERTARIAN": ("L", "Libertarian"),
    }
    code, label = mapping.get(val.upper(), ("", val))
    return {"code": code, "name": label}


def normalize_house_district(raw: str) -> str:
    text = (raw or "").strip()
    if text.upper() in {"AL", "AT LARGE", "AT-LARGE", "AT LARGE DISTRICT"}:
        return "At-Large"
    # House XML can use 1st, 2nd, 3rd, 12th, etc.
    match = re.fullmatch(r"(\d+)(?:st|nd|rd|th)?", text, re.I)
    if match:
        n = int(match.group(1))
        return "At-Large" if n == 0 else f"{n:02d}"
    return text


def is_vacant_house_seat(info: ET.Element) -> bool:
    for tag in ("firstname", "lastname", "official-name", "courtesy"):
        if safe_text(info.find(tag)).lower() in {"vacant", "vacancy", "currently vacant"}:
            return True
    return False


def request_url(url: str, *, session: Optional[requests.Session] = None,
                params: Optional[Dict[str, str]] = None) -> requests.Response:
    """Fail with an exception, never fabricate an empty successful feed.

    Avoid logging URL query params because they may contain a Congress.gov API key.
    """
    http = session or requests
    for attempt in range(1, RETRIES + 1):
        try:
            response = http.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            if attempt == RETRIES:
                raise RuntimeError(f"Request failed for {urlparse(url).netloc}{urlparse(url).path}") from exc
            LOGGER.warning("Source request failed (%s/%s) for %s", attempt, RETRIES, urlparse(url).path)
            time.sleep(min(2 ** (attempt - 1), 8))
    raise AssertionError("unreachable")


def fetch_xml(url: str, label: str, *, session: Optional[requests.Session] = None) -> Tuple[Optional[ET.Element], Dict[str, Any]]:
    meta = {"name": label, "url": url, "retrieved_at": utc_now_iso(),
            "status": "failed", "error": ""}
    try:
        response = request_url(url, session=session)
        root = normalize_xml(ET.fromstring(response.content))
    except (RuntimeError, ET.ParseError, ValueError) as exc:
        meta["error"] = str(exc)
        LOGGER.error("Could not load %s: %s", label, exc)
        return None, meta
    meta["status"] = "success"
    return root, meta


def committee_name_lookup(root: ET.Element) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for committee in root.findall(".//committees/committee"):
        code = committee.get("comcode", "")
        if code:
            lookup[code] = safe_text(committee.find("committee-fullname"))
        for sub in committee.findall("subcommittee"):
            subcode = sub.get("subcomcode", "")
            if subcode:
                lookup[subcode] = safe_text(sub.find("subcommittee-fullname"))
    return lookup


def parse_house_committees(member: ET.Element, lookup: Dict[str, str]) -> List[Dict[str, Any]]:
    container = member.find("committee-assignments")
    if container is None:
        return []
    committees: Dict[str, Dict[str, Any]] = {}
    for el in container.findall("committee"):
        code = el.get("comcode", "").strip()
        if code:
            committees[code] = {
                "type": "committee", "code": code, "name": lookup.get(code, ""),
                "rank": el.get("rank", ""), "leadership": el.get("leadership", ""),
                "subcommittees": [],
            }
    for el in container.findall("subcommittee"):
        code = el.get("subcomcode", "").strip()
        if not code:
            continue
        sub = {"type": "subcommittee", "code": code, "name": lookup.get(code, ""),
               "rank": el.get("rank", ""), "leadership": el.get("leadership", "")}
        # Real codes include AG03 -> AG00 and HED03 -> HED00.
        parent_code = code[:-2] + "00" if len(code) >= 4 else ""
        if parent_code in committees:
            committees[parent_code]["subcommittees"].append(sub)
        else:
            # Preserve the assignment instead of misattributing it to a parent.
            committees[code] = {**sub, "subcommittees": []}
    return list(committees.values())


def parse_house_members(root: ET.Element) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
    lookup = committee_name_lookup(root)
    output: Dict[str, Dict[str, Any]] = {}
    skipped = {"vacancies": 0, "missing_bioguide": 0, "duplicate_bioguide": 0}
    for node in root.findall(".//member"):
        info = node.find("member-info")
        if info is None:
            continue
        if is_vacant_house_seat(info):
            skipped["vacancies"] += 1
            continue
        bioguide = safe_text(info.find("bioguideid"))
        if not BIOGUIDE_PATTERN.fullmatch(bioguide):
            skipped["missing_bioguide"] += 1
            continue
        if bioguide in output:
            skipped["duplicate_bioguide"] += 1
            continue
        first = safe_text(info.find("firstname"))
        last = safe_text(info.find("lastname"))
        official = safe_text(info.find("official-name")) or " ".join(filter(None, (first, last)))
        state_node = info.find("state")
        output[bioguide] = {
            "bioguide_id": bioguide, "chamber": "House",
            "name": {"first": first, "last": last, "official": official,
                     "courtesy_title": safe_text(info.find("courtesy"))},
            "party": normalize_party(safe_text(info.find("party"))),
            "state": safe_attr(state_node, "postal-code"),
            "district": normalize_house_district(safe_text(info.find("district"))),
            "leadership": {"position": "", "source": "", "last_reviewed": "", "roles": [], "status": "unverified"},
            "committees": parse_house_committees(node, lookup),
            "contact": {"status": "matched", "phone": safe_text(info.find("phone")),
                        "office_building": safe_text(info.find("office-building")),
                        "office_room": safe_text(info.find("office-room")),
                        "office_zip": safe_text(info.find("office-zip")),
                        "address": "", "website": "", "email": ""},
            "house_data": {"town_name": safe_text(info.find("town")) or safe_text(info.find("townname")),
                           "prior_congress": safe_text(info.find("prior-congress")),
                           "caucus": safe_text(info.find("caucus")),
                           "elected_date": safe_attr(info.find("elected-date"), "date"),
                           "sworn_date": safe_attr(info.find("sworn-date"), "date")},
            "source": {"primary": "house_members_clerk_xml", "contact": "house_members_clerk_xml",
                       "contact_status": "matched"},
        }
    return output, skipped


def parse_senate_contacts(root: Optional[ET.Element]) -> Dict[str, Dict[str, str]]:
    if root is None:
        return {}
    output: Dict[str, Dict[str, str]] = {}
    for person in root.findall(".//member"):
        bio = safe_text(person.find("bioguide_id")) or safe_text(person.find("bioguideid"))
        if BIOGUIDE_PATTERN.fullmatch(bio):
            output[bio] = {
                "senate_class": safe_text(person.find("class")),
                "address": safe_text(person.find("address")),
                "phone": safe_text(person.find("phone")),
                "email": safe_text(person.find("email")),
                "website": safe_text(person.find("website")),
            }
    return output


def parse_senate_committees(person: ET.Element) -> List[Dict[str, Any]]:
    container = person.find("committees")
    if container is None:
        return []
    return [{"type": "committee", "code": el.get("code", ""), "name": safe_text(el),
             "rank": el.get("rank", ""), "leadership": el.get("leadership", ""), "subcommittees": []}
            for el in container.findall("committee")]


def parse_senate_members(root: ET.Element, contacts: Dict[str, Dict[str, str]],
                         old_members: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
    output: Dict[str, Dict[str, Any]] = {}
    stats = {"missing_bioguide": 0, "duplicate_bioguide": 0, "missing_contacts": 0,
             "preserved_contacts": 0}
    for person in root.findall(".//senator") + root.findall(".//member"):
        bio = safe_text(person.find("bioguideid")) or safe_text(person.find("bioguide_id"))
        if not BIOGUIDE_PATTERN.fullmatch(bio):
            stats["missing_bioguide"] += 1
            continue
        if bio in output:
            stats["duplicate_bioguide"] += 1
            continue
        name = person.find("name")
        first = safe_text(name.find("first")) if name is not None else ""
        last = safe_text(name.find("last")) if name is not None else ""
        official = " ".join(filter(None, (first, last)))
        contact = contacts.get(bio)
        status = "matched" if contact else "missing_from_contact_source"
        if not contact:
            stats["missing_contacts"] += 1
            prior = old_members.get(bio, {})
            previous_contact = prior.get("contact", {})
            if prior.get("chamber") == "Senate" and any(previous_contact.get(x) for x in ("phone", "address", "website", "email")):
                contact = {k: previous_contact.get(k, "") for k in ("phone", "address", "website", "email")}
                contact["senate_class"] = prior.get("senate_data", {}).get("senate_class", "")
                status = "stale_preserved"
                stats["preserved_contacts"] += 1
        contact = contact or {}
        output[bio] = {
            "bioguide_id": bio, "chamber": "Senate",
            "name": {"first": first, "last": last, "official": official, "courtesy_title": ""},
            "party": normalize_party(safe_text(person.find("party"))),
            "state": safe_text(person.find("state")), "district": "",
            "leadership": {"position": "", "source": "", "last_reviewed": "", "roles": [], "status": "unverified"},
            "committees": parse_senate_committees(person),
            "contact": {"status": status, "phone": contact.get("phone", ""),
                        "office_building": "", "office_room": "", "office_zip": "",
                        "address": contact.get("address", ""),
                        "website": contact.get("website", ""), "email": contact.get("email", "")},
            "senate_data": {"lis_member_id": person.get("lis_member_id") or safe_text(person.find("lis_member_id")),
                            "hometown": safe_text(person.find("hometown")),
                            "office": safe_text(person.find("office")),
                            "senate_class": contact.get("senate_class", "")},
            "source": {"primary": "senate_cvc_member_xml",
                       "contact": "senate_cfm_contact_xml" if status == "matched" else
                                  ("previous_verified_snapshot" if status == "stale_preserved" else ""),
                       "contact_status": status},
        }
    return output, stats


class EvidenceHTML(HTMLParser):
    """Records visible text and anchors, without relying on fragile CSS classes."""
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.words: List[str] = []
        self.links: List[Dict[str, Any]] = []
        self._anchor: Optional[Dict[str, Any]] = None
        self._hidden = 0

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag in {"style", "script", "noscript"}:
            self._hidden += 1
        if tag == "a":
            href = dict(attrs).get("href") or ""
            self._anchor = {"href": href, "start": len(self.words)}

    def handle_data(self, data: str) -> None:
        if self._hidden:
            return
        value = " ".join(data.split())
        if value:
            self.words.append(value)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"style", "script", "noscript"} and self._hidden:
            self._hidden -= 1
        if tag == "a" and self._anchor is not None:
            anchor = self._anchor
            anchor["end"] = len(self.words)
            anchor["text"] = " ".join(self.words[anchor["start"]:anchor["end"]])
            self.links.append(anchor)
            self._anchor = None


def parse_official_leadership(html: str, chamber: str, members: Dict[str, Dict[str, Any]],
                              source_url: str, congress: int) -> Tuple[Dict[str, List[Dict[str, Any]]], List[str]]:
    parser = EvidenceHTML()
    parser.feed(html)
    texts = parser.words
    errors: List[str] = []
    joined = " ".join(texts[:150])
    listed_congresses = re.findall(r"\b(\d{2,3})(?:st|nd|rd|th) Congress\b", joined, flags=re.I)
    if listed_congresses and congress not in {int(x) for x in listed_congresses}:
        return {}, [f"{chamber} directory is for Congress {listed_congresses}, expected {congress}"]
    roles: Dict[str, List[Dict[str, Any]]] = {}
    checked_at = utc_now_iso()
    if chamber == "House":
        prev_end = 0
        for link in parser.links:
            link_url = urlparse(urljoin(source_url, link["href"]))
            if link_url.hostname != "clerk.house.gov":
                continue
            match = HOUSE_PROFILE_PATTERN.search(link_url.path)
            if not match:
                continue
            bio = match.group(1).upper()
            # In Clerk cards, member name and role precede the 'View Profile' link.
            start = max(prev_end, link["start"] - 24)
            indexed = [(idx, token) for idx, token in enumerate(texts[start:link["start"]], start)
                       if token in HOUSE_TITLES]
            prev_end = link["end"]
            if not indexed:
                continue
            if len({title for _, title in indexed}) > 1:
                errors.append(f"Ambiguous House leadership card for {bio}")
                continue
            role = indexed[-1][1]
            if bio not in members or members[bio]["chamber"] != "House":
                errors.append(f"House directory references unknown/non-House member {bio}")
                continue
            roles.setdefault(bio, []).append({"position": role, "congress": congress,
                                               "current": True, "source": "house_clerk_leadership",
                                               "source_url": source_url, "verified_at": checked_at})
    else:
        # Senate.gov contains committee chair biographies after its party leadership
        # section. Do not confuse committee chairs with chamber/party leaders.
        def section_index(label: str) -> int:
            return next((idx for idx, t in enumerate(texts) if t == label), -1)
        cutoff = section_index("Committee Chair")
        if cutoff == -1:
            return {}, ["Senate leadership section boundary not found"]
        start_party = section_index("Political Party Leaders")
        if start_party == -1:
            return {}, ["Senate party leadership section not found"]
        for idx, link in enumerate(parser.links):
            link_url = urlparse(urljoin(source_url, link["href"]))
            if link_url.hostname != "bioguide.congress.gov":
                continue
            match = SENATE_BIO_PATTERN.search(link_url.path)
            if not match or link["start"] >= cutoff:
                continue
            bio = match.group(1).upper()
            next_bio = next((next_link["start"] for next_link in parser.links[idx+1:]
                             if urlparse(urljoin(source_url, next_link["href"])).hostname == "bioguide.congress.gov"
                             and SENATE_BIO_PATTERN.search(urlparse(urljoin(source_url, next_link["href"])).path)), cutoff)
            end = min(next_bio, cutoff, link["end"] + 22)
            seen = [tok for tok in texts[link["end"]:end] if tok in SENATE_TITLES]
            if not seen:
                continue
            if len(set(seen)) > 1:
                errors.append(f"Ambiguous Senate leadership card for {bio}")
                continue
            role = seen[0]
            # Before party leadership, only president pro tempore is relevant.
            if link["start"] < start_party and role != "President Pro Tempore":
                continue
            if bio not in members or members[bio]["chamber"] != "Senate":
                errors.append(f"Senate directory references unknown/non-Senate member {bio}")
                continue
            roles.setdefault(bio, []).append({"position": role, "congress": congress,
                                               "current": True, "source": "senate_official_leadership",
                                               "source_url": source_url, "verified_at": checked_at})

    unique: Dict[Tuple[str, str], Dict[str, Any]] = {}
    title_owners: Dict[str, set] = {}
    for bio, assignments in roles.items():
        for role in assignments:
            unique[(bio, role["position"])] = role
            title_owners.setdefault(role["position"], set()).add(bio)
    for title, owners in title_owners.items():
        if len(owners) > 1 and not (chamber == "Senate" and title in MULTIPLE_SENATE_TITLES):
            errors.append(f"Conflicting {chamber} leadership holders for {title}: {sorted(owners)}")
    filtered: Dict[str, List[Dict[str, Any]]] = {}
    for (bio, _), item in unique.items():
        filtered.setdefault(bio, []).append(item)
    # For a full live roster, detecting only a handful of leaders signals a
    # partially changed website layout, not an election result.
    chamber_members = sum(m["chamber"] == chamber for m in members.values())
    min_expected = (7 if chamber == "House" else 12) if chamber_members >= MIN_CHAMBER_SIZE[chamber] else 5
    if len(unique) < min_expected:
        errors.append(f"{chamber} leadership page yielded only {len(unique)} roles (minimum {min_expected})")
    return filtered, errors


def fetch_official_leadership(chamber: str, members: Dict[str, Dict[str, Any]],
                              congress: int, *, session: Optional[requests.Session] = None
                              ) -> Tuple[Optional[Dict[str, List[Dict[str, Any]]]], Dict[str, Any]]:
    url = HOUSE_LEADERSHIP_URL if chamber == "House" else SENATE_LEADERSHIP_URL
    meta = {"name": f"{chamber.lower()}_leadership_directory", "url": url,
            "retrieved_at": utc_now_iso(), "status": "failed", "error": "", "roles": 0}
    try:
        response = request_url(url, session=session)
        roles, errors = parse_official_leadership(response.text, chamber, members, url, congress)
        if errors:
            raise ValueError("; ".join(errors))
        meta["roles"] = sum(map(len, roles.values()))
        meta["status"] = "success"
        return roles, meta
    except (RuntimeError, ValueError, UnicodeError) as exc:
        meta["error"] = str(exc)
        LOGGER.error("Could not verify %s leadership: %s", chamber, exc)
        return None, meta


def parse_congress_leadership(payload: Dict[str, Any], bio: str) -> List[Dict[str, Any]]:
    doc = payload.get("member") or {}
    if doc.get("bioguideId", bio) != bio:
        raise ValueError("Congress.gov returned a different Bioguide ID")
    entries = doc.get("leadership") or []
    if isinstance(entries, dict):
        entries = entries.get("item", [])
    if not isinstance(entries, list):
        raise ValueError("Congress.gov leadership format unexpected")
    result = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        title = item.get("type", "").strip()
        if not title:
            continue
        try:
            congress = int(item.get("congress"))
        except (TypeError, ValueError):
            continue
        val = item.get("current", False)
        current = val is True or str(val).lower() == "true"
        result.append({"position": title, "congress": congress, "current": current,
                       "source": "congress_gov_member_api",
                       "source_url": f"{CONGRESS_MEMBER_API}{bio}",
                       "verified_at": utc_now_iso()})
    return result


def normalized_role(title: str) -> str:
    """Minimal, defensible equivalences for roles named differently by sources."""
    title = re.sub(r"\s+", " ", (title or "").strip()).casefold()
    return {"speaker of the house": "speaker", "senate majority leader": "majority leader"}.get(title, title)


def merge_role_history(current: List[Dict[str, Any]], historical: List[Dict[str, Any]],
                       congress: int) -> List[Dict[str, Any]]:
    """Directory is the authority for current roles; API/old snapshots for history.

    Keep source verification timestamps stable when a role has not changed.
    The run-level verification timestamp is reported in the validation file.
    """
    old_by_key = {}
    for role in historical:
        if role.get("position") and str(role.get("congress", "")).isdigit():
            key = (normalized_role(role["position"]), int(role["congress"]))
            old_by_key[key] = dict(role)
    out = {key: {**record, "current": False} for key, record in old_by_key.items()}
    for role in current:
        key = (normalized_role(role["position"]), congress)
        item = dict(role)
        earlier = old_by_key.get(key)
        if earlier and earlier.get("current") and earlier.get("source") == item.get("source") \
                and earlier.get("source_url") == item.get("source_url"):
            item["verified_at"] = earlier.get("verified_at", item.get("verified_at"))
        out[key] = item
    return sorted(out.values(), key=lambda r: (not r.get("current", False), -r["congress"], r["position"]))


def update_leadership(members: Dict[str, Dict[str, Any]],
                      old_members: Dict[str, Dict[str, Any]],
                      house_roles: Dict[str, List[Dict[str, Any]]],
                      senate_roles: Dict[str, List[Dict[str, Any]]],
                      congress: int, api_key: str = "", *,
                      session: Optional[requests.Session] = None,
                      max_api_lookups: int = 40, scan_all: bool = False) -> Dict[str, Any]:
    roles_by_bio = {**house_roles, **senate_roles}
    api_errors: List[str] = []
    discrepancies: List[str] = []
    candidate_ids = sorted(
        set(roles_by_bio) |
        {b for b, mem in old_members.items() if b in members and
         (mem.get("leadership", {}).get("roles") or mem.get("leadership", {}).get("position"))}
    )
    if scan_all:
        # Deliberate full historical backfill, not the default daily workflow.
        candidate_ids += [bio for bio in sorted(members) if bio not in set(candidate_ids)]
    candidates = candidate_ids[:max_api_lookups] if api_key and max_api_lookups > 0 else []
    api_history: Dict[str, List[Dict[str, Any]]] = {}
    for bio in candidates:
        try:
            response = request_url(f"{CONGRESS_MEMBER_API}{bio}", session=session,
                                   params={"format": "json", "api_key": api_key})
            api_history[bio] = parse_congress_leadership(response.json(), bio)
        except (RuntimeError, ValueError, requests.RequestException) as exc:
            api_errors.append(f"Congress.gov leadership history unavailable for {bio}: {str(exc)[:130]}")
    for bio, member in members.items():
        live = roles_by_bio.get(bio, [])
        prior = old_members.get(bio, {}).get("leadership", {}) or {}
        old_roles = prior.get("roles", [])
        if not isinstance(old_roles, list):
            old_roles = []
        api_roles = api_history.get(bio, [])
        older = api_roles + old_roles
        # Legacy records had only an unverified title with no effective date.
        # Do not silently promote that string into invented historical evidence.
        member["leadership"]["roles"] = merge_role_history(live, older, congress)
        member["leadership"]["status"] = "verified"
        if live:
            preferred = sorted(live, key=lambda r: (role_rank(r["position"]), r["position"]))[0]
            preserved = next((r for r in member["leadership"]["roles"] if r.get("current")
                              and normalized_role(r["position"]) == normalized_role(preferred["position"])),
                             preferred)
            member["leadership"].update({"position": preferred["position"],
                                          "source": preferred["source"],
                                          "last_reviewed": preserved["verified_at"][:10]})
        # Only check the same title and Congress. Different office coverage is
        # not itself a contradiction because the directories expose more roles.
        for role in api_roles:
            if int(role["congress"]) != congress:
                continue
            match = any(normalized_role(r["position"]) == normalized_role(role["position"]) for r in live)
            if match and not role["current"]:
                discrepancies.append(f"{bio}: directory lists {role['position']} as current, Congress.gov currently marks it false")
                member["leadership"]["status"] = "review_required"
            elif role["current"] and not match and normalized_role(role["position"]) in {
                    normalized_role(t) for t in (*HOUSE_TITLES, *SENATE_TITLES)}:
                discrepancies.append(f"{bio}: Congress.gov lists {role['position']} as current, absent from its official directory")
                member["leadership"]["status"] = "review_required"
        if member["leadership"]["status"] == "review_required":
            # Legacy consumers read leadership.position unconditionally. Clear it
            # rather than silently presenting an unresolved cross-source conflict.
            member["leadership"]["position"] = ""
            member["leadership"]["source"] = ""
    return {"source": "official_chamber_directories",
            "checked_at": utc_now_iso(),
            "congress_gov_api_used": bool(api_key),
            "api_requests": len(candidates), "api_errors": api_errors,
            "discrepancies": discrepancies,
            "house_current_roles": sum(map(len, house_roles.values())),
            "senate_current_roles": sum(map(len, senate_roles.values()))}


def role_rank(title: str) -> int:
    value = title.lower()
    if "speaker of the house" in value or "president pro tempore" in value:
        return 0
    if "leader" in value:
        return 1
    if "whip" in value:
        return 2
    return 3


def validate_members(members: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    warnings: List[Dict[str, str]] = []
    errors: List[Dict[str, str]] = []
    for bio, member in members.items():
        for field in ("bioguide_id", "chamber", "name", "party", "state"):
            if not member.get(field):
                errors.append({"bioguide_id": bio, "field": field, "message": "Required value missing"})
        if not BIOGUIDE_PATTERN.fullmatch(bio) or member.get("bioguide_id") != bio:
            errors.append({"bioguide_id": bio, "field": "bioguide_id", "message": "Invalid/mismatched Bioguide ID"})
        name = member.get("name") or {}
        if not (name.get("official") or (name.get("first") and name.get("last"))):
            errors.append({"bioguide_id": bio, "field": "name", "message": "No usable member name"})
        if not (member.get("party") or {}).get("name"):
            errors.append({"bioguide_id": bio, "field": "party", "message": "No party name"})
        elif not (member.get("party") or {}).get("code"):
            warnings.append({"bioguide_id": bio, "field": "party.code", "message": "Unrecognized party classification"})
        if member["chamber"] == "Senate" and member["contact"]["status"] != "matched":
            warnings.append({"bioguide_id": bio, "field": "contact.status",
                             "message": f"Senate contact not verified this run: {member['contact']['status']}"})
        for committee in member.get("committees", []):
            if not committee.get("name"):
                warnings.append({"bioguide_id": bio, "field": "committees", "message": f"Missing committee name: {committee.get('code')}"})
    return {"validated_at": utc_now_iso(), "total_members_checked": len(members),
            "warning_count": len(warnings), "error_count": len(errors),
            "warnings": warnings, "errors": errors}


def read_existing_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"metadata": {}, "members": {}}
    with path.open("r", encoding="utf-8") as fp:
        result = json.load(fp)
    if not isinstance(result, dict) or not isinstance(result.get("members"), dict):
        raise ValueError(f"Existing database invalid, refusing to overwrite: {path}")
    return result


def atomic_write(path: Path, value: Dict[str, Any]) -> None:
    """Use same-directory temp and atomic replace; clean temps on failure."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=f".{path.name}.", suffix=".tmp", delete=False) as fp:
            tmp_path = Path(fp.name)
            json.dump(value, fp, indent=2, ensure_ascii=False)
            fp.write("\n")
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_path, path)
        tmp_path = None
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)


def preserve_previous(path: Path) -> Optional[Path]:
    if not path.exists():
        return None
    backup = path.with_name(f"{path.stem}.previous{path.suffix}")
    # Create the backup from intact old generation before replacing original.
    staging = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".backup-", delete=False) as fp:
            staging = Path(fp.name)
            with path.open("rb") as src:
                shutil.copyfileobj(src, fp)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(staging, backup)
        staging = None
    finally:
        if staging is not None:
            staging.unlink(missing_ok=True)
    return backup


def append_audit(path: Path, report: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(report, ensure_ascii=False, sort_keys=True) + "\n")
        fp.flush()
        os.fsync(fp.fileno())


def roster_safety_errors(members: Dict[str, Dict[str, Any]],
                         old_members: Dict[str, Dict[str, Any]], *,
                         min_sizes: Optional[Dict[str, int]] = None,
                         max_removals: Optional[Dict[str, int]] = None,
                         approve_large_changes: bool = False) -> List[str]:
    minimum = min_sizes or MIN_CHAMBER_SIZE
    removal_caps = max_removals or {"House": 15, "Senate": 5}
    failures: List[str] = []
    for chamber in ("House", "Senate"):
        new_ids = {bio for bio, m in members.items() if m["chamber"] == chamber}
        old_ids = {bio for bio, m in old_members.items() if m.get("chamber") == chamber}
        if not (minimum[chamber] <= len(new_ids) <= MAX_CHAMBER_SIZE[chamber]):
            failures.append(f"{chamber} population implausible: {len(new_ids)}")
        removed = old_ids - new_ids
        added = new_ids - old_ids
        if not approve_large_changes and len(old_ids) >= minimum[chamber] and len(removed) > removal_caps[chamber]:
            failures.append(f"{chamber} unexpected removals: {len(removed)} ({sorted(removed)[:12]})")
        if not approve_large_changes and len(old_ids) >= minimum[chamber] and len(added) > removal_caps[chamber]:
            failures.append(f"{chamber} unexpected additions: {len(added)} ({sorted(added)[:12]})")
    return failures


def database_diff(previous: Dict[str, Dict[str, Any]], current: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    older, newer = set(previous), set(current)
    changed: Dict[str, List[str]] = {}
    for bio in older & newer:
        if previous[bio] != current[bio]:
            fields = [key for key in set(previous[bio]) | set(current[bio])
                      if previous[bio].get(key) != current[bio].get(key)]
            changed[bio] = sorted(fields)
    return {"added": sorted(newer - older), "removed": sorted(older - newer),
            "changed": changed, "added_count": len(newer - older),
            "removed_count": len(older - newer), "changed_count": len(changed)}


def build_member_database(filename: str = OUTPUT_FILE, *, api_key: Optional[str] = None,
                          max_api_lookups: int = 40, scan_all_history: bool = False,
                          session: Optional[requests.Session] = None,
                          min_sizes: Optional[Dict[str, int]] = None,
                          approve_large_changes: bool = False) -> bool:
    """Only publish complete, verified generations. False means no publication.

    All network calls and XML/HTML parsing happen before output replacement.
    Official leadership directory errors block publication, not just warning.
    """
    dest = Path(filename)
    validation_path = dest.with_name(f"{dest.stem}.validation.json")
    audit_path = dest.with_name(f"{dest.stem}.changes.jsonl")
    try:
        old = read_existing_json(dest)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        LOGGER.error("Refusing to overwrite unreadable database: %s", exc)
        return False
    old_members = old.get("members", {})
    house_xml, house_meta = fetch_xml(HOUSE_MEMBERS_URL, "house_members_clerk_xml", session=session)
    senate_xml, senate_meta = fetch_xml(SENATE_CVC_URL, "senate_cvc_member_xml", session=session)
    contact_xml, contact_meta = fetch_xml(SENATE_CFM_URL, "senate_cfm_contact_xml", session=session)
    sources = [house_meta, senate_meta, contact_meta]
    failures: List[str] = []
    if house_xml is None:
        failures.append("House primary roster unavailable")
    if senate_xml is None:
        failures.append("Senate primary roster unavailable")
    if failures:
        atomic_write(validation_path, {"checked_at": utc_now_iso(), "published": False,
                                       "errors": failures, "sources": sources})
        LOGGER.error("Publication blocked: %s", "; ".join(failures))
        return False
    assert house_xml is not None and senate_xml is not None
    house, hstats = parse_house_members(house_xml)
    senate, sstats = parse_senate_members(senate_xml, parse_senate_contacts(contact_xml), old_members)
    sources[0]["parsed_members"] = len(house)
    sources[0].update(hstats)
    sources[1]["parsed_members"] = len(senate)
    sources[1].update(sstats)
    members = {**house, **senate}
    failures.extend(roster_safety_errors(members, old_members, min_sizes=min_sizes,
                                        approve_large_changes=approve_large_changes))
    # An unexpectedly empty Senate contact response with HTTP 200 is just as
    # dangerous as a failed fetch, so preserve old values and report the gap.
    if contact_xml is None and any(m["contact"]["status"] != "stale_preserved" for m in senate.values()):
        failures.append("Senate contacts unavailable and no complete previous snapshot to preserve")
    expected_contacts = max(3, int(len(senate) * 0.90))
    if contact_xml is not None and len(parse_senate_contacts(contact_xml)) < expected_contacts:
        failures.append(f"Senate contact feed returned fewer than {expected_contacts} valid entries")
    if hstats["duplicate_bioguide"] or sstats["duplicate_bioguide"]:
        failures.append("Primary roster contains duplicate Bioguide IDs")
    if hstats["missing_bioguide"] > 2 or sstats["missing_bioguide"]:
        failures.append("Primary roster has unexpected missing Bioguide IDs")
    if len(house) + len(senate) != len(members):
        failures.append("Duplicate Bioguide IDs across chambers")
    validation = validate_members(members)
    if validation["error_count"]:
        failures.append(f"{validation['error_count']} critical member schema errors")
    # Skip leadership parsing if the roster itself is clearly invalid.
    leadership_meta = {}
    if not failures:
        congress = congress_number()
        house_roles, house_lead = fetch_official_leadership("House", members, congress, session=session)
        senate_roles, senate_lead = fetch_official_leadership("Senate", members, congress, session=session)
        sources.extend([house_lead, senate_lead])
        if house_roles is None or senate_roles is None:
            failures.append("Official leadership directories incomplete or unverified")
        else:
            # Reject large unexpected drops instead of clearing still-valid leaders.
            for chamber, incoming in (("House", house_roles), ("Senate", senate_roles)):
                prev_count = sum(1 for m in old_members.values() if m.get("chamber") == chamber
                                 and m.get("leadership", {}).get("position"))
                fresh_count = sum(len(v) for v in incoming.values())
                if prev_count >= 5 and fresh_count < max(5, math.ceil(prev_count * 0.80)):
                    failures.append(f"{chamber} leadership dropped unexpectedly: {prev_count} to {fresh_count}")
            if not failures:
                leadership_meta = update_leadership(
                    members, old_members, house_roles, senate_roles, congress,
                    api_key=api_key if api_key is not None else os.getenv("CONGRESS_API_KEY", ""),
                    session=session, max_api_lookups=max_api_lookups, scan_all=scan_all_history,
                )
                validation = validate_members(members)
                for note in leadership_meta.get("api_errors", []) + leadership_meta.get("discrepancies", []):
                    validation["warnings"].append({"bioguide_id": "", "field": "leadership", "message": note})
                validation["warning_count"] = len(validation["warnings"])
    result = {**validation, "checked_at": utc_now_iso(), "published": False,
              "blocking_errors": failures, "sources": sources, "leadership": leadership_meta}
    if failures:
        atomic_write(validation_path, result)
        LOGGER.error("Publication blocked: %s", "; ".join(failures))
        return False

    changes = database_diff(old_members, members)
    metadata = {
        "version": 3,
        "last_updated": utc_now_iso(),
        "large_roster_change_explicitly_approved": approve_large_changes,
        "total_members": len(members), "house_count": len(house), "senate_count": len(senate),
        "source_count": len(sources), "sources": sources,
        "leadership": leadership_meta,
        "validation": {"warning_count": validation["warning_count"],
                       "error_count": 0, "validation_file": str(validation_path.name)},
        "manual_overrides": {"house_leadership": {"enabled": False, "last_reviewed": "", "member_count": 0}},
    }
    result["published"] = True
    if old_members == members:
        # Preserve the generation timestamp on a true no-op.
        atomic_write(validation_path, {**result, "unchanged": True})
        LOGGER.info("No member changes; current generation preserved.")
        return True

    candidate = {"metadata": metadata, "members": members}
    serialized = (json.dumps(candidate, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    digest = hashlib.sha256(serialized).hexdigest()
    previous_digest = hashlib.sha256(dest.read_bytes()).hexdigest() if dest.exists() else ""
    audit = {"timestamp": utc_now_iso(), "status": "committed", "congress": congress_number(),
             "large_roster_change_explicitly_approved": approve_large_changes,
             "previous_sha256": previous_digest, "new_sha256": digest, "change_summary": changes}
    try:
        backup = preserve_previous(dest)
        atomic_write(dest, candidate)
    except OSError as exc:
        LOGGER.error("Could not atomically publish member database: %s", exc)
        atomic_write(validation_path, {**result, "published": False,
                                       "blocking_errors": [f"Publication I/O failure: {exc}"]})
        return False
    try:
        append_audit(audit_path, audit)
    except OSError as exc:
        LOGGER.error("Database saved but audit append failed, repair required: %s", exc)
        result.setdefault("warnings", []).append({"field": "audit", "message": str(exc)})
    atomic_write(validation_path, {**result, "unchanged": False, "changes": changes})
    LOGGER.info("Published %d records; backup=%s; changes=%d", len(members), backup, changes["changed_count"])
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Build verified Ballotics/The Record congressional member JSON")
    parser.add_argument("--output", default=OUTPUT_FILE, help="Output JSON path")
    parser.add_argument("--api-max-lookups", type=int, default=40,
                        help="Maximum optional Congress.gov leadership history lookups per run (default 40)")
    parser.add_argument("--scan-all-history", action="store_true",
                        help="Discover historical leadership for every current member, subject to API lookup budget")
    parser.add_argument("--approve-large-changes", action="store_true",
                        help="Explicitly approve unusual membership turnover after independent review")
    args = parser.parse_args()
    return 0 if build_member_database(args.output, max_api_lookups=max(0, args.api_max_lookups),
                                       scan_all_history=args.scan_all_history,
                                       approve_large_changes=args.approve_large_changes) else 1


if __name__ == "__main__":
    raise SystemExit(main())
