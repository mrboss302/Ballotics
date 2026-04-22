import json
import logging
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from openai import OpenAI

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

CONGRESS_API_KEY = os.environ.get("CONGRESS_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

DEFAULT_OUTPUT_FILE = "latest-votes.json"
DEFAULT_TIMEOUT = 20
DEFAULT_RETRIES = 3
HOUSE_PAGE_SIZE = 250

BASE_YEAR = 2025
BASE_CONGRESS = 119

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("ballotics-votes")

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def get_current_congress_and_session() -> Tuple[int, int]:
    year = datetime.now(timezone.utc).year
    congress = BASE_CONGRESS + (year - BASE_YEAR) // 2
    session = 1 if year % 2 == 1 else 2
    return congress, session

def safe_text(node: Optional[ET.Element], default: str = "") -> str:
    if node is None or node.text is None:
        return default
    return node.text.strip()

def safe_int(node: Optional[ET.Element], default: int = 0) -> int:
    try:
        return int(safe_text(node, str(default)))
    except (TypeError, ValueError):
        return default

def request_with_retries(url: str, *, headers: Optional[Dict[str, str]] = None, timeout: int = DEFAULT_TIMEOUT, retries: int = DEFAULT_RETRIES) -> Optional[requests.Response]:
    merged_headers = {**HEADERS, **(headers or {})}
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=merged_headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            logger.warning("Request failed (%s/%s): %s | %s", attempt, retries, url, exc)
            if attempt < retries:
                time.sleep(2 ** (attempt - 1))
            else:
                logger.error("Giving up on URL: %s", url)
    return None

def load_database(filename: str) -> Dict[str, Any]:
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"metadata": {"generated_at": None, "congress": None, "session": None, "version": 2}, "votes": [], "errors": []}

def save_database(filename: str, db: Dict[str, Any]) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)

def append_error(db: Dict[str, Any], source: str, item_id: str, message: str) -> None:
    db.setdefault("errors", []).append({"timestamp": utc_now_iso(), "source": source, "item_id": item_id, "message": message})

def vote_sort_key(vote: Dict[str, Any]) -> Tuple[str, int]:
    return (vote.get("date") or "", int(vote.get("roll_call_number", 0)))

def find_existing_vote_ids(db: Dict[str, Any]) -> set:
    return {v["id"] for v in db.get("votes", []) if "id" in v}

def get_highest_roll_call_number(db: Dict[str, Any], chamber: str, congress: int, session: int) -> int:
    numbers = [int(v.get("roll_call_number", 0)) for v in db.get("votes", []) if v.get("chamber") == chamber and v.get("congress") == congress and v.get("session") == session]
    return max(numbers, default=0)

def build_congress_gov_url(congress: int, bill_type: str, bill_number: str) -> Optional[str]:
    """Generates a stable deep link to Congress.gov for the bill text."""
    if not bill_type or not bill_number:
        return None
        
    bt = bill_type.lower().replace(".", "").strip()
    type_map = {
        "hr": "house-bill", "s": "senate-bill",
        "hjres": "house-joint-resolution", "sjres": "senate-joint-resolution",
        "hconres": "house-concurrent-resolution", "sconres": "senate-concurrent-resolution",
        "hres": "house-resolution", "sres": "senate-resolution"
    }
    
    mapped_type = type_map.get(bt)
    if mapped_type:
        return f"https://www.congress.gov/bill/{congress}th-congress/{mapped_type}/{bill_number}/text"
    return None

# -----------------------------------------------------------------------------
# AI Summary
# -----------------------------------------------------------------------------

def get_ai_summary(vote_description: str, bill_info: Optional[Dict[str, str]] = None) -> str:
    if not openai_client: return "Summary pending."

    context = f"Vote Description: {vote_description}\n"
    if bill_info:
        context += f"Associated Legislation: {bill_info.get('type', '')} {bill_info.get('number', '')}".strip()

    system_prompt = (
        "You are an expert non-partisan legislative analyst. Your goal is to explain what a bill does and who it affects.\n"
        "Rules:\n1. If the text describes a procedural rule, focus on the bill or policy at issue.\n"
        "2. Translate legislative jargon into plain English.\n3. Use active voice.\n4. Be concise, max 3 sentences.\n"
        "5. If the text is purely procedural and reveals no substantive policy, say: 'Technical procedural vote.'\n"
        "6. Use only the provided text. Do not rely on outside knowledge."
    )

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": context}],
            temperature=0.1
        )
        content = response.choices[0].message.content
        return content.strip() if content else "Summary pending."
    except Exception as exc:
        logger.warning("AI summary generation failed: %s", exc)
        return "Summary pending."

# -----------------------------------------------------------------------------
# Senate ID Mapping
# -----------------------------------------------------------------------------

def build_senate_id_map() -> Dict[str, str]:
    logger.info("Building Senate ID translation map...")
    url = "https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml"
    response = request_with_retries(url)
    if not response: return {}

    id_map: Dict[str, str] = {}
    try:
        root = ET.fromstring(response.content)
        for elem in root.iter():
            if '}' in elem.tag: elem.tag = elem.tag.split('}', 1)[1]
            elem.tag = elem.tag.lower()
            
        for person in root.findall(".//senator") + root.findall(".//member"):
            lis = person.get("lis_member_id")
            if not lis: lis = safe_text(person.find("lis_member_id"))
                
            bio_node = person.find("bioguideid")
            if bio_node is None: bio_node = person.find("bioguide_id")
            bio = safe_text(bio_node)
            
            if lis and bio: id_map[lis.strip()] = bio.strip()
    except ET.ParseError as exc:
        logger.error("Failed parsing Senate ID map XML: %s", exc)
    return id_map

# -----------------------------------------------------------------------------
# Vote Record Normalization
# -----------------------------------------------------------------------------

def make_vote_record(*, chamber: str, congress: int, session: int, roll_call_number: int, date: str, question: str, 
                     result: str, vote_description: str, bill_type: str, bill_number: str, source_url: Optional[str], 
                     member_votes: Dict[str, str], totals: Dict[str, int], extra_details: Dict[str, str], ai_summary: Optional[str] = None) -> Dict[str, Any]:
    
    congress_gov_url = build_congress_gov_url(congress, bill_type, bill_number)
    
    return {
        "id": f"{congress}-{session}-{chamber[0]}-{roll_call_number}",
        "chamber": chamber,
        "congress": congress,
        "session": session,
        "roll_call_number": roll_call_number,
        "date": date,
        "question": question,
        "result": result,
        "vote_description": vote_description,
        "majority_requirement": extra_details.get("majority_requirement", ""),
        "vote_type": extra_details.get("vote_type", ""),
        "bill": {
            "congress": congress,
            "type": bill_type,
            "number": bill_number,
            "congress_gov_url": congress_gov_url,
            "ai_summary": ai_summary or "Summary pending."
        },
        "amendment": {
            "number": extra_details.get("amendment_number", ""),
            "purpose": extra_details.get("amendment_purpose", "")
        },
        "tie_breaker": {
            "by_whom": extra_details.get("tie_breaker_by_whom", ""),
            "vote": extra_details.get("tie_breaker_vote", "")
        },
        "totals": {
            "yeas": totals.get("yeas", 0), "nays": totals.get("nays", 0),
            "present": totals.get("present", 0), "not_voting": totals.get("not_voting", 0)
        },
        "member_votes": member_votes,
        "source_url": source_url
    }

# -----------------------------------------------------------------------------
# House Sync
# -----------------------------------------------------------------------------

def fetch_house_vote_index(congress: int, session: int) -> List[Dict[str, Any]]:
    if not CONGRESS_API_KEY: return []
    all_votes, offset = [], 0
    while True:
        url = f"https://api.congress.gov/v3/house-vote/{congress}/{session}?format=json&limit={HOUSE_PAGE_SIZE}&offset={offset}&api_key={CONGRESS_API_KEY}"
        response = request_with_retries(url)
        if not response: break
        try: data = response.json()
        except ValueError: break
        
        page_votes = data.get("houseRollCallVotes", [])
        if not page_votes: break
        all_votes.extend(page_votes)
        
        if not data.get("pagination", {}).get("next"): break
        offset += HOUSE_PAGE_SIZE
    return all_votes

def parse_house_vote_detail(xml_url: str, db: Dict[str, Any], vote_id: str) -> Tuple[str, Dict[str, int], Dict[str, str], Dict[str, str]]:
    totals = {"yeas": 0, "nays": 0, "present": 0, "not_voting": 0}
    member_votes = {}
    extra_details = {}
    vote_description = ""

    response = request_with_retries(xml_url)
    if not response:
        append_error(db, "house_detail_fetch", vote_id, f"No response from {xml_url}")
        return vote_description, totals, member_votes, extra_details

    try:
        root = ET.fromstring(response.content)
        vote_description = safe_text(root.find(".//vote-desc"))
        totals["yeas"] = safe_int(root.find(".//yea-total"))
        totals["nays"] = safe_int(root.find(".//nay-total"))
        totals["present"] = safe_int(root.find(".//present-total"))
        totals["not_voting"] = safe_int(root.find(".//not-voting-total"))
        
        # New Extract: Majority Requirement and Vote Type
        extra_details["majority_requirement"] = safe_text(root.find(".//majority"))
        extra_details["vote_type"] = safe_text(root.find(".//vote-type"))

        for rv in root.findall(".//recorded-vote"):
            legislator = rv.find("legislator")
            bioguide = legislator.get("name-id", "").strip() if legislator is not None else ""
            if bioguide: member_votes[bioguide] = safe_text(rv.find("vote"))
    except ET.ParseError as exc:
        append_error(db, "house_detail_parse", vote_id, str(exc))

    return vote_description, totals, member_votes, extra_details

def process_house_votes(db: Dict[str, Any], congress: int, session: int, *, enrich_ai: bool = False) -> int:
    existing_ids = find_existing_vote_ids(db)
    highest_roll = get_highest_roll_call_number(db, "House", congress, session)
    new_count = 0

    for item in fetch_house_vote_index(congress, session):
        try: roll = int(item.get("rollCallNumber", 0))
        except (TypeError, ValueError): continue

        vote_id = f"{congress}-{session}-H-{roll}"
        if vote_id in existing_ids or roll <= highest_roll: continue

        xml_url = item.get("sourceDataURL")
        vote_description, totals, member_votes, extra_details = "", {"yeas": 0, "nays": 0, "present": 0, "not_voting": 0}, {}, {}

        if xml_url:
            vote_description, totals, member_votes, extra_details = parse_house_vote_detail(xml_url, db, vote_id)

        bill_type = (item.get("legislationType") or "").strip()
        bill_number = str(item.get("legislationNumber") or "").strip()
        summary = get_ai_summary(vote_description or item.get("question", ""), {"type": bill_type, "number": bill_number}) if enrich_ai else None

        record = make_vote_record(
            chamber="House", congress=congress, session=session, roll_call_number=roll,
            date=item.get("startDate") or "", question=item.get("question") or "", result=item.get("result") or "",
            vote_description=vote_description, bill_type=bill_type, bill_number=bill_number,
            source_url=xml_url, member_votes=member_votes, totals=totals, extra_details=extra_details, ai_summary=summary
        )
        db["votes"].append(record)
        new_count += 1
    return new_count

# -----------------------------------------------------------------------------
# Senate Sync
# -----------------------------------------------------------------------------

def fetch_senate_vote_index(congress: int, session: int) -> List[ET.Element]:
    url = f"https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_{congress}_{session}.xml"
    response = request_with_retries(url)
    if not response: return []
    try: return ET.fromstring(response.content).findall(".//vote")
    except ET.ParseError: return []

def parse_senate_vote_detail(detail_url: str, id_map: Dict[str, str], db: Dict[str, Any], vote_id: str) -> Tuple[Dict[str, int], Dict[str, str], Dict[str, str]]:
    totals = {"yeas": 0, "nays": 0, "present": 0, "not_voting": 0}
    member_votes = {}
    extra_details = {}

    response = request_with_retries(detail_url)
    if not response:
        append_error(db, "senate_detail_fetch", vote_id, f"No response from {detail_url}")
        return totals, member_votes, extra_details

    try:
        root = ET.fromstring(response.content)
        count = root.find(".//count")
        if count is not None:
            totals["yeas"] = safe_int(count.find("yeas"))
            totals["nays"] = safe_int(count.find("nays"))
            totals["present"] = safe_int(count.find("present"))
            totals["not_voting"] = safe_int(count.find("absent"))
            
        # New Extract: Deep metadata mapping the old Core Data schema
        extra_details["majority_requirement"] = safe_text(root.find(".//majority_requirement"))
        extra_details["document_type"] = safe_text(root.find(".//document_type"))
        extra_details["document_number"] = safe_text(root.find(".//document_number"))
        extra_details["amendment_number"] = safe_text(root.find(".//amendment_number"))
        extra_details["amendment_purpose"] = safe_text(root.find(".//amendment_purpose"))
        extra_details["tie_breaker_by_whom"] = safe_text(root.find(".//tie_breaker_by_whom"))
        extra_details["tie_breaker_vote"] = safe_text(root.find(".//tie_breaker_vote"))

        for member in root.findall(".//member"):
            lis_id = safe_text(member.find("lis_member_id"))
            bioguide = id_map.get(lis_id) or lis_id
            if bioguide: member_votes[bioguide] = safe_text(member.find("vote_cast"))
    except ET.ParseError as exc:
        append_error(db, "senate_detail_parse", vote_id, str(exc))

    return totals, member_votes, extra_details

def process_senate_votes(db: Dict[str, Any], congress: int, session: int, id_map: Dict[str, str], *, enrich_ai: bool = False) -> int:
    existing_ids = find_existing_vote_ids(db)
    highest_roll = get_highest_roll_call_number(db, "Senate", congress, session)
    new_count = 0

    for vote in fetch_senate_vote_index(congress, session):
        vote_number_text = safe_text(vote.find("vote_number"))
        try: roll = int(vote_number_text)
        except ValueError: continue

        vote_id = f"{congress}-{session}-S-{roll}"
        if vote_id in existing_ids or roll <= highest_roll: continue

        detail_url = f"https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{vote_number_text.zfill(5)}.xml"
        totals, member_votes, extra_details = parse_senate_vote_detail(detail_url, id_map, db, vote_id)

        # Dynamic Bill Mapping
        bill_type = extra_details.get("document_type", "Senate Vote")
        bill_number = extra_details.get("document_number", vote_number_text)

        title = safe_text(vote.find("title"))
        question = safe_text(vote.find("question"))
        summary = get_ai_summary(title or question, {"type": bill_type, "number": bill_number}) if enrich_ai else None

        record = make_vote_record(
            chamber="Senate", congress=congress, session=session, roll_call_number=roll,
            date=safe_text(vote.find("vote_date")), question=question, result=safe_text(vote.find("result")),
            vote_description=title, bill_type=bill_type, bill_number=bill_number,
            source_url=detail_url, member_votes=member_votes, totals=totals, extra_details=extra_details, ai_summary=summary
        )
        db["votes"].append(record)
        new_count += 1
    return new_count

# -----------------------------------------------------------------------------
# AI Enrichment Pass
# -----------------------------------------------------------------------------

def enrich_missing_summaries(db: Dict[str, Any], max_items: Optional[int] = None) -> int:
    updated = 0
    for vote in db.get("votes", []):
        current_summary = vote.get("bill", {}).get("ai_summary", "").strip()
        if current_summary and current_summary != "Summary pending." and not current_summary.startswith("Summary pending:"): continue

        bill = vote.get("bill", {})
        vote["bill"]["ai_summary"] = get_ai_summary(vote.get("vote_description") or vote.get("question") or "", {"type": bill.get("type", ""), "number": bill.get("number", "")})
        updated += 1
        if max_items is not None and updated >= max_items: break
    return updated

# -----------------------------------------------------------------------------
# Main Orchestration
# -----------------------------------------------------------------------------

def build_vote_database(*, congress: Optional[int] = None, session: Optional[int] = None, filename: str = DEFAULT_OUTPUT_FILE, enrich_ai_during_sync: bool = False, enrich_missing_after_sync: bool = True, enrich_limit: Optional[int] = None) -> None:
    if congress is None or session is None: congress, session = get_current_congress_and_session()
    logger.info("Starting sync for Congress %s Session %s", congress, session)

    db = load_database(filename)
    db["metadata"].update({"congress": congress, "session": session})

    new_house = process_house_votes(db, congress, session, enrich_ai=enrich_ai_during_sync)
    new_senate = process_senate_votes(db, congress, session, build_senate_id_map(), enrich_ai=enrich_ai_during_sync)

    if enrich_missing_after_sync:
        enriched = enrich_missing_summaries(db, max_items=enrich_limit)
        logger.info("Enriched %s vote summaries.", enriched)

    db["votes"].sort(key=vote_sort_key, reverse=True)
    db["metadata"]["generated_at"] = utc_now_iso()
    save_database(filename, db)

if __name__ == "__main__":
    build_vote_database(enrich_ai_during_sync=False, enrich_missing_after_sync=True, enrich_limit=25)
