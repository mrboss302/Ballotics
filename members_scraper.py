import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests


# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("ballotics-members")


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

HEADERS = {
    "User-Agent": "Ballotics/The Record civic data importer; contact: app-maintainer"
}

OUTPUT_FILE = "latest-members.json"
VALIDATION_FILE = "latest-members.validation.json"

HOUSE_MEMBERS_URL = "https://clerk.house.gov/xml/lists/MemberData.xml"
SENATE_CVC_URL = "https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml"
SENATE_CFM_URL = "https://www.senate.gov/general/contact_information/senators_cfm.xml"

REQUEST_TIMEOUT = 20


# ------------------------------------------------------------
# Manually curated leadership data
# ------------------------------------------------------------
# Keep this separate from scraped data.
# This should be reviewed when congressional leadership changes.
# For production, consider moving this to leadership-overrides.json.

MANUAL_LEADERSHIP_LAST_REVIEWED = "2026-04-24"

HOUSE_LEADERSHIP = {
    "J000299": "Speaker of the House",   # Mike Johnson
    "S001176": "Majority Leader",        # Steve Scalise
    "E000294": "Majority Whip",          # Tom Emmer
    "J000294": "Minority Leader",        # Hakeem Jeffries
    "C001101": "Minority Whip"           # Katherine Clark
}


# ------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def safe_text(node: Optional[ET.Element]) -> str:
    return node.text.strip() if node is not None and node.text else ""


def safe_attr(node: Optional[ET.Element], attr: str, default: str = "") -> str:
    if node is None:
        return default
    return node.get(attr, default) or default


def strip_namespaces(root: ET.Element) -> ET.Element:
    """
    Removes XML namespaces and lowercases tags so government XML files
    are easier to query consistently.
    """
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]
        elem.tag = elem.tag.lower()
    return root


def normalize_party(raw_party: str) -> Dict[str, str]:
    """
    Returns both a stable party code and a display name.
    This keeps UI filters clean even when source formatting varies.
    """
    value = raw_party.strip()

    mapping = {
        "D": ("D", "Democratic"),
        "DEM": ("D", "Democratic"),
        "DEMOCRAT": ("D", "Democratic"),
        "DEMOCRATIC": ("D", "Democratic"),

        "R": ("R", "Republican"),
        "REP": ("R", "Republican"),
        "REPUBLICAN": ("R", "Republican"),

        "I": ("I", "Independent"),
        "IND": ("I", "Independent"),
        "INDEPENDENT": ("I", "Independent")
    }

    code, name = mapping.get(value.upper(), ("", value))
    return {
        "code": code,
        "name": name
    }


def normalize_house_district(raw_district: str) -> str:
    """
    Keeps at-large districts readable while normalizing numeric districts.
    """
    district = raw_district.strip()

    if not district:
        return ""

    if district.upper() in {"AL", "AT LARGE", "AT-LARGE"}:
        return "At-Large"

    if district.isdigit():
        return district.zfill(2)

    return district


def is_vacant_house_seat(info: ET.Element) -> bool:
    """
    Detects House vacancy placeholder records so they do not enter
    the member database as malformed representatives.
    """
    name_fields = [
        safe_text(info.find("firstname")),
        safe_text(info.find("lastname")),
        safe_text(info.find("official-name")),
        safe_text(info.find("courtesy"))
    ]

    combined_name = " ".join(name_fields).strip().lower()

    vacancy_terms = {
        "vacant",
        "vacancy",
        "currently vacant"
    }

    return any(term in combined_name for term in vacancy_terms)


def fetch_xml(url: str, source_name: str) -> Tuple[Optional[ET.Element], Dict[str, Any]]:
    """
    Fetches and parses an XML document.
    Returns the parsed root plus source metadata.
    Gracefully logs failures instead of crashing the whole importer.
    """
    retrieved_at = utc_now_iso()

    source_metadata = {
        "name": source_name,
        "url": url,
        "retrieved_at": retrieved_at,
        "status": "not_fetched",
        "http_status": None,
        "error": ""
    }

    try:
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        source_metadata["http_status"] = response.status_code
        response.raise_for_status()

        root = ET.fromstring(response.content)
        root = strip_namespaces(root)

        source_metadata["status"] = "success"
        return root, source_metadata

    except requests.RequestException as error:
        source_metadata["status"] = "failed"
        source_metadata["error"] = str(error)
        logger.error(f"Failed to fetch {source_name}: {error}")
        return None, source_metadata

    except ET.ParseError as error:
        source_metadata["status"] = "failed"
        source_metadata["error"] = f"XML parse error: {error}"
        logger.error(f"Failed to parse XML for {source_name}: {error}")
        return None, source_metadata


def read_existing_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {
            "metadata": {},
            "members": {}
        }

    try:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)

    except Exception as error:
        logger.warning(f"Could not read existing {path}: {error}")
        return {
            "metadata": {},
            "members": {}
        }


def write_json(path: str, data: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)


# ------------------------------------------------------------
# Validation
# ------------------------------------------------------------

REQUIRED_MEMBER_FIELDS = [
    "bioguide_id",
    "chamber",
    "name",
    "party",
    "state"
]


def validate_members(members: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Lightweight validation report.
    Not fatal by default because official data can be temporarily incomplete.
    Vacancy placeholders should be skipped before they reach this point.
    """
    warnings: List[Dict[str, str]] = []

    for bioguide_id, member in members.items():
        for field in REQUIRED_MEMBER_FIELDS:
            if field not in member or member[field] in ("", None, {}, []):
                warnings.append({
                    "bioguide_id": bioguide_id,
                    "field": field,
                    "message": f"Missing or empty required field: {field}"
                })

        name = member.get("name", {})
        if not name.get("first") and not name.get("last") and not name.get("official"):
            warnings.append({
                "bioguide_id": bioguide_id,
                "field": "name",
                "message": "Member has no usable name fields."
            })

        party = member.get("party", {})
        if not party.get("code") and not party.get("name"):
            warnings.append({
                "bioguide_id": bioguide_id,
                "field": "party",
                "message": "Member has no usable party data."
            })

        state = member.get("state", "")
        if not state:
            warnings.append({
                "bioguide_id": bioguide_id,
                "field": "state",
                "message": "Member has no state."
            })

        contact = member.get("contact", {})
        contact_status = contact.get("status", "")

        if member.get("chamber") == "Senate":
            if contact_status == "missing_from_contact_source":
                warnings.append({
                    "bioguide_id": bioguide_id,
                    "field": "contact",
                    "message": "Senator exists in CVC roster but is missing from Senate CFM contact source."
                })
            elif contact_status not in {"matched", "missing_from_contact_source"}:
                warnings.append({
                    "bioguide_id": bioguide_id,
                    "field": "contact.status",
                    "message": f"Unexpected Senate contact status: {contact_status}"
                })

    report = {
        "validated_at": utc_now_iso(),
        "total_members_checked": len(members),
        "warning_count": len(warnings),
        "warnings": warnings
    }

    return report


# ------------------------------------------------------------
# House importer
# ------------------------------------------------------------

def build_house_committee_lookup(root: ET.Element) -> Dict[str, str]:
    """
    Builds a lookup for House committee and subcommittee names.
    """
    committee_lookup: Dict[str, str] = {}

    for committee in root.findall(".//committees/committee"):
        comcode = committee.get("comcode", "")
        committee_name = safe_text(committee.find("committee-fullname"))

        if comcode and committee_name:
            committee_lookup[comcode] = committee_name

        for subcommittee in committee.findall("subcommittee"):
            subcomcode = subcommittee.get("subcomcode", "")
            subcommittee_name = safe_text(subcommittee.find("subcommittee-fullname"))

            if subcomcode and subcommittee_name:
                committee_lookup[subcomcode] = subcommittee_name

    return committee_lookup


def parse_house_committees(
    member: ET.Element,
    committee_lookup: Dict[str, str]
) -> List[Dict[str, Any]]:
    """
    Extracts and nests House committee/subcommittee assignments.
    """
    committees_by_code: Dict[str, Dict[str, Any]] = {}

    committee_assignments = member.find("committee-assignments")
    if committee_assignments is None:
        return []

    # First pass: main committees
    for committee in committee_assignments.findall("committee"):
        comcode = committee.get("comcode", "")

        committee_data: Dict[str, Any] = {
            "type": "committee",
            "code": comcode,
            "name": committee_lookup.get(comcode, ""),
            "rank": committee.get("rank", ""),
            "leadership": committee.get("leadership", ""),
            "subcommittees": []
        }

        committees_by_code[comcode] = committee_data

    # Second pass: subcommittees
    for subcommittee in committee_assignments.findall("subcommittee"):
        subcomcode = subcommittee.get("subcomcode", "")

        # In House data, parent committee codes generally follow this pattern.
        parent_comcode = subcomcode[:3] + "00" if len(subcomcode) >= 3 else ""

        subcommittee_data = {
            "type": "subcommittee",
            "code": subcomcode,
            "name": committee_lookup.get(subcomcode, ""),
            "rank": subcommittee.get("rank", ""),
            "leadership": subcommittee.get("leadership", "")
        }

        if parent_comcode in committees_by_code:
            committees_by_code[parent_comcode]["subcommittees"].append(subcommittee_data)
        else:
            # Fallback: preserve the assignment even if parent inference fails.
            committees_by_code[subcomcode] = {
                **subcommittee_data,
                "subcommittees": []
            }

    return list(committees_by_code.values())


def fetch_house_members() -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    logger.info("Fetching House members...")

    root, source_metadata = fetch_xml(HOUSE_MEMBERS_URL, "house_members_clerk_xml")
    if root is None:
        return {}, source_metadata

    committee_lookup = build_house_committee_lookup(root)
    members: Dict[str, Dict[str, Any]] = {}

    skipped_vacancies = 0
    skipped_missing_bioguide = 0

    for member in root.findall(".//member"):
        info = member.find("member-info")
        if info is None:
            continue

        if is_vacant_house_seat(info):
            skipped_vacancies += 1
            logger.info("Skipping vacant House seat placeholder.")
            continue

        bioguide_id = safe_text(info.find("bioguideid"))
        if not bioguide_id:
            skipped_missing_bioguide += 1
            logger.info("Skipping House member record with missing bioguide_id.")
            continue

        state_node = info.find("state")
        state_postal = safe_attr(state_node, "postal-code")

        elected_node = info.find("elected-date")
        sworn_node = info.find("sworn-date")

        first_name = safe_text(info.find("firstname"))
        last_name = safe_text(info.find("lastname"))
        official_name = safe_text(info.find("official-name"))
        raw_party = safe_text(info.find("party"))

        leadership_position = HOUSE_LEADERSHIP.get(bioguide_id, "")

        members[bioguide_id] = {
            "bioguide_id": bioguide_id,
            "chamber": "House",

            "name": {
                "first": first_name,
                "last": last_name,
                "official": official_name,
                "courtesy_title": safe_text(info.find("courtesy"))
            },

            "party": normalize_party(raw_party),
            "state": state_postal,

            "district": normalize_house_district(safe_text(info.find("district"))),

            "leadership": {
                "position": leadership_position,
                "source": "manual" if leadership_position else "",
                "last_reviewed": MANUAL_LEADERSHIP_LAST_REVIEWED if leadership_position else ""
            },

            "committees": parse_house_committees(member, committee_lookup),

            "contact": {
                "status": "matched",
                "phone": safe_text(info.find("phone")),
                "office_building": safe_text(info.find("office-building")),
                "office_room": safe_text(info.find("office-room")),
                "office_zip": safe_text(info.find("office-zip")),
                "address": "",
                "website": "",
                "email": ""
            },

            "house_data": {
                "town_name": safe_text(info.find("townname")),
                "prior_congress": safe_text(info.find("prior-congress")),
                "caucus": safe_text(info.find("caucus")),
                "elected_date": safe_attr(elected_node, "date"),
                "sworn_date": safe_attr(sworn_node, "date")
            },

            "source": {
                "primary": "house_members_clerk_xml",
                "contact": "house_members_clerk_xml",
                "contact_status": "matched"
            }
        }

    source_metadata["parsed_members"] = len(members)
    source_metadata["skipped_vacancies"] = skipped_vacancies
    source_metadata["skipped_missing_bioguide"] = skipped_missing_bioguide

    logger.info(
        f"Parsed {len(members)} House members. "
        f"Skipped {skipped_vacancies} vacancy placeholder(s) and "
        f"{skipped_missing_bioguide} record(s) with missing bioguide_id."
    )

    return members, source_metadata


# ------------------------------------------------------------
# Senate importer
# ------------------------------------------------------------

def build_senate_contact_lookup(root_cfm: Optional[ET.Element]) -> Dict[str, Dict[str, str]]:
    """
    Parses Senate contact data by bioguide_id.
    Returns empty lookup if contact XML is unavailable.
    """
    lookup: Dict[str, Dict[str, str]] = {}

    if root_cfm is None:
        return lookup

    for member in root_cfm.findall(".//member"):
        bioguide_id = safe_text(member.find("bioguide_id"))

        if not bioguide_id:
            continue

        lookup[bioguide_id] = {
            "senate_class": safe_text(member.find("class")),
            "address": safe_text(member.find("address")),
            "phone": safe_text(member.find("phone")),
            "email": safe_text(member.find("email")),
            "website": safe_text(member.find("website"))
        }

    return lookup


def parse_senate_name(person: ET.Element) -> Dict[str, str]:
    name_node = person.find("name")

    first_name = safe_text(name_node.find("first")) if name_node is not None else ""
    last_name = safe_text(name_node.find("last")) if name_node is not None else ""

    official_name = " ".join(part for part in [first_name, last_name] if part)

    return {
        "first": first_name,
        "last": last_name,
        "official": official_name,
        "courtesy_title": ""
    }


def parse_senate_committees(person: ET.Element) -> List[Dict[str, Any]]:
    """
    Senate CVC data is usually flatter than House committee data.
    We still return the same broad schema for UI consistency.
    """
    committees: List[Dict[str, Any]] = []

    committees_node = person.find("committees")
    if committees_node is None:
        return committees

    for committee in committees_node.findall("committee"):
        committees.append({
            "type": "committee",
            "code": committee.get("code", ""),
            "name": safe_text(committee),
            "rank": committee.get("rank", ""),
            "leadership": committee.get("leadership", ""),
            "subcommittees": []
        })

    return committees


def fetch_senate_members() -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    logger.info("Fetching Senate members...")

    root_cvc, cvc_source_metadata = fetch_xml(SENATE_CVC_URL, "senate_cvc_member_xml")
    root_cfm, cfm_source_metadata = fetch_xml(SENATE_CFM_URL, "senate_cfm_contact_xml")

    source_metadata = [cvc_source_metadata, cfm_source_metadata]

    if root_cvc is None:
        logger.error("Senate CVC member data unavailable. Cannot build Senate roster.")
        return {}, source_metadata

    contact_lookup = build_senate_contact_lookup(root_cfm)
    members: Dict[str, Dict[str, Any]] = {}

    skipped_missing_bioguide = 0
    missing_contact_matches = 0
    matched_contact_records = 0

    senate_people = root_cvc.findall(".//senator") + root_cvc.findall(".//member")

    for person in senate_people:
        lis_member_id = person.get("lis_member_id") or safe_text(person.find("lis_member_id"))

        bioguide_node = person.find("bioguideid")
        if bioguide_node is None:
            bioguide_node = person.find("bioguide_id")

        bioguide_id = safe_text(bioguide_node)
        if not bioguide_id:
            skipped_missing_bioguide += 1
            logger.info("Skipping Senate member record with missing bioguide_id.")
            continue

        raw_party = safe_text(person.find("party"))

        extra_contact = contact_lookup.get(bioguide_id)
        contact_status = "matched" if extra_contact else "missing_from_contact_source"

        if extra_contact:
            matched_contact_records += 1
        else:
            missing_contact_matches += 1

        extra_contact = extra_contact or {}

        leadership_position = safe_text(person.find("leadership_position"))

        members[bioguide_id] = {
            "bioguide_id": bioguide_id,
            "chamber": "Senate",

            "name": parse_senate_name(person),

            "party": normalize_party(raw_party),
            "state": safe_text(person.find("state")),

            # Keep district empty for schema consistency.
            "district": "",

            "leadership": {
                "position": leadership_position,
                "source": "senate_cvc_member_xml" if leadership_position else "",
                "last_reviewed": ""
            },

            "committees": parse_senate_committees(person),

            "contact": {
                "status": contact_status,
                "phone": extra_contact.get("phone", ""),
                "office_building": "",
                "office_room": "",
                "office_zip": "",
                "address": extra_contact.get("address", ""),
                "website": extra_contact.get("website", ""),
                "email": extra_contact.get("email", "")
            },

            "senate_data": {
                "lis_member_id": lis_member_id,
                "hometown": safe_text(person.find("hometown")),
                "office": safe_text(person.find("office")),
                "senate_class": extra_contact.get("senate_class", "")
            },

            "source": {
                "primary": "senate_cvc_member_xml",
                "contact": "senate_cfm_contact_xml" if contact_status == "matched" else "",
                "contact_status": contact_status
            }
        }

    cvc_source_metadata["parsed_members"] = len(members)
    cvc_source_metadata["skipped_missing_bioguide"] = skipped_missing_bioguide
    cvc_source_metadata["missing_contact_matches"] = missing_contact_matches
    cvc_source_metadata["matched_contact_records"] = matched_contact_records

    cfm_source_metadata["parsed_contact_records"] = len(contact_lookup)
    cfm_source_metadata["matched_contact_records"] = matched_contact_records
    cfm_source_metadata["unmatched_roster_members"] = missing_contact_matches

    logger.info(
        f"Parsed {len(members)} Senate members. "
        f"Matched {matched_contact_records} contact record(s). "
        f"Missing contact match for {missing_contact_matches} senator(s). "
        f"Skipped {skipped_missing_bioguide} record(s) with missing bioguide_id."
    )

    return members, source_metadata


# ------------------------------------------------------------
# Database builder
# ------------------------------------------------------------

def build_member_database() -> None:
    house_members, house_source = fetch_house_members()
    senate_members, senate_sources = fetch_senate_members()

    all_members = {
        **house_members,
        **senate_members
    }

    if not all_members:
        logger.error("No members were fetched. Refusing to write empty member database.")
        return

    existing_data = read_existing_json(OUTPUT_FILE)
    old_members = existing_data.get("members", {})

    validation_report = validate_members(all_members)

    if validation_report["warning_count"] > 0:
        logger.warning(
            f"Validation completed with {validation_report['warning_count']} warning(s). "
            f"See {VALIDATION_FILE}."
        )
    else:
        logger.info("Validation completed with no warnings.")

    write_json(VALIDATION_FILE, validation_report)

    if all_members == old_members:
        logger.info("No roster changes detected. Skipping update to preserve timestamp.")
        return

    logger.info("Roster changes detected. Updating JSON...")

    sources: List[Dict[str, Any]] = [house_source]
    sources.extend(senate_sources)

    new_data = {
        "metadata": {
            "last_updated": utc_now_iso(),
            "total_members": len(all_members),
            "house_count": len(house_members),
            "senate_count": len(senate_members),
            "source_count": len(sources),
            "sources": sources,
            "manual_overrides": {
                "house_leadership": {
                    "enabled": True,
                    "last_reviewed": MANUAL_LEADERSHIP_LAST_REVIEWED,
                    "member_count": len(HOUSE_LEADERSHIP)
                }
            },
            "validation": {
                "warning_count": validation_report["warning_count"],
                "validation_file": VALIDATION_FILE
            }
        },
        "members": all_members
    }

    write_json(OUTPUT_FILE, new_data)

    logger.info(f"Successfully saved {len(all_members)} members to {OUTPUT_FILE}.")


if __name__ == "__main__":
    build_member_database()
