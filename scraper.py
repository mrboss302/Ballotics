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

# Anchor for Congress number calculation.
# Update BASE_YEAR and BASE_CONGRESS when a new Congress begins in January
# of the next odd-numbered year (e.g. 120th Congress begins January 2027).
BASE_YEAR = 2025
BASE_CONGRESS = 119

# Standard User-Agent to bypass basic government firewalls
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("ballotics-votes")

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def get_current_congress_and_session() -> Tuple[int, int]:
    """
    Calculates the current Congress and session number from a known anchor point.

    Congress numbers flip in January of odd-numbered years, not at the calendar
    year boundary, so we anchor on a verified (year, congress) pair and derive
    forward from there. Update BASE_YEAR / BASE_CONGRESS at the top of the file
    when a new Congress is seated.

    Session 1 runs in odd-numbered years, session 2 in even-numbered years.
    """
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


def request_with_retries(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
) -> Optional[requests.Response]:
    """
    GET request with exponential backoff retry. Always sends the default
    HEADERS merged with any caller-supplied overrides.

    Note: The unused `expect_json` parameter has been removed. Callers are
    responsible for parsing the response in whichever format they need.
    """
    merged_headers = {**HEADERS, **(headers or {})}

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=merged_headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            logger.warning(
                "Request failed (%s/%s): %s | %s",
                attempt, retries, url, exc
            )
            if attempt < retries:
                time.sleep(2 ** (attempt - 1))
            else:
                logger.error("Giving up on URL: %s", url)
    return None


def load_database(filename: str) -> Dict[str, Any]:
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)

    return {
        "metadata": {
            "generated_at": None,
            "congress": None,
            "session": None,
            "version": 2
        },
        "votes": [],
        "errors": []
    }


def save_database(filename: str, db: Dict[str, Any]) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)


def append_error(db: Dict[str, Any], source: str, item_id: str, message: str) -> None:
    db.setdefault("errors", []).append({
        "timestamp": utc_now_iso(),
        "source": source,
        "item_id": item_id,
        "message": message
    })


def vote_sort_key(vote: Dict[str, Any]) -> Tuple[str, int]:
    return (vote.get("date") or "", int(vote.get("roll_call_number", 0)))


def find_existing_vote_ids(db: Dict[str, Any]) -> set:
    return {v["id"] for v in db.get("votes", []) if "id" in v}


def get_highest_roll_call_number(
    db: Dict[str, Any],
    chamber: str,
    congress: int,
    session: int
) -> int:
    numbers = [
        int(v.get("roll_call_number", 0))
        for v in db.get("votes", [])
        if v.get("chamber") == chamber
        and v.get("congress") == congress
        and v.get("session") == session
    ]
    return max(numbers, default=0)

# -----------------------------------------------------------------------------
# AI Summary
# -----------------------------------------------------------------------------

def get_ai_summary(vote_description: str, bill_info: Optional[Dict[str, str]] = None) -> str:
    """
    Generates a concise, non-partisan summary from only the provided text.
    """
    if not openai_client:
        return "Summary pending."

    context = f"Vote Description: {vote_description}\n"
    if bill_info:
        context += (
            f"Associated Legislation: "
            f"{bill_info.get('type', '')} {bill_info.get('number', '')}".strip()
        )

    system_prompt = (
        "You are an expert non-partisan legislative analyst. "
        "Your goal is to explain what a bill does and who it affects.\n"
        "Rules:\n"
        "1. If the text describes a procedural rule, focus on the bill or policy at issue.\n"
        "2. Translate legislative jargon into plain English.\n"
        "3. Use active voice.\n"
        "4. Be concise, max 3 sentences.\n"
        "5. If the text is purely procedural and reveals no substantive policy, say: "
        "'Technical procedural vote.'\n"
        "6. Use only the provided text. Do not rely on outside knowledge."
    )

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": context}
            ],
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
    """
    Maps Senate LIS member IDs to Bioguide IDs.

    This is intentionally called once at the orchestration level and passed
    into process_senate_votes() to avoid redundant network fetches if this
    function is ever called in a multi-session loop.
    """
    logger.info("Building Senate ID translation map...")
    url = "https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml"
    response = request_with_retries(url)

    if not response:
        logger.error("Could not fetch Senate ID map.")
        return {}

    id_map: Dict[str, str] = {}

    try:
        root = ET.fromstring(response.content)
        for member in root.findall(".//member"):
            lis = member.find("lis_member_id")
            bio = member.find("bioguideId") or member.find("bioguide_id")
            lis_text = safe_text(lis)
            bio_text = safe_text(bio)
            if lis_text and bio_text:
                id_map[lis_text] = bio_text
    except ET.ParseError as exc:
        logger.error("Failed parsing Senate ID map XML: %s", exc)

    logger.info("Loaded %s Senate LIS -> Bioguide mappings.", len(id_map))
    return id_map

# -----------------------------------------------------------------------------
# Vote Record Normalization
# -----------------------------------------------------------------------------

def make_vote_record(
    *,
    chamber: str,
    congress: int,
    session: int,
    roll_call_number: int,
    date: str,
    question: str,
    result: str,
    vote_description: str,
    bill_type: str,
    bill_number: str,
    source_url: Optional[str],
    member_votes: Dict[str, str],
    totals: Dict[str, int],
    ai_summary: Optional[str] = None
) -> Dict[str, Any]:
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
        "bill": {
            "type": bill_type,
            "number": bill_number,
            "ai_summary": ai_summary or "Summary pending."
        },
        "totals": {
            "yeas": totals.get("yeas", 0),
            "nays": totals.get("nays", 0),
            "present": totals.get("present", 0),
            "not_voting": totals.get("not_voting", 0)
        },
        "member_votes": member_votes,
        "source_url": source_url
    }

# -----------------------------------------------------------------------------
# House Sync
# -----------------------------------------------------------------------------

def fetch_house_vote_index(congress: int, session: int) -> List[Dict[str, Any]]:
    """
    Paginates through the Congress API House vote index.
    """
    if not CONGRESS_API_KEY:
        logger.error("Missing CONGRESS_API_KEY.")
        return []

    all_votes: List[Dict[str, Any]] = []
    offset = 0

    while True:
        url = (
            f"https://api.congress.gov/v3/house-vote/{congress}/{session}"
            f"?format=json&limit={HOUSE_PAGE_SIZE}&offset={offset}&api_key={CONGRESS_API_KEY}"
        )
        response = request_with_retries(url)
        if not response:
            break

        try:
            data = response.json()
        except ValueError as exc:
            logger.error("Invalid JSON from House vote index: %s", exc)
            break

        page_votes = data.get("houseRollCallVotes", [])
        if not page_votes:
            break

        all_votes.extend(page_votes)

        pagination = data.get("pagination", {})
        next_url = pagination.get("next")
        if not next_url:
            break

        offset += HOUSE_PAGE_SIZE

    logger.info("Fetched %s House vote index records.", len(all_votes))
    return all_votes


def parse_house_vote_detail(
    xml_url: str,
    db: Dict[str, Any],
    vote_id: str
) -> Tuple[str, Dict[str, int], Dict[str, str]]:
    """
    Fetches and parses a House vote detail XML file.

    Returns: (vote_description, totals, member_votes)

    Parse failures are recorded in db["errors"] so the calling sync loop
    has an audit trail of what was dropped rather than silently skipping.
    """
    totals = {"yeas": 0, "nays": 0, "present": 0, "not_voting": 0}
    member_votes: Dict[str, str] = {}
    vote_description = ""

    response = request_with_retries(xml_url)
    if not response:
        append_error(db, "house_detail_fetch", vote_id, f"No response from {xml_url}")
        return vote_description, totals, member_votes

    try:
        root = ET.fromstring(response.content)
        vote_description = safe_text(root.find(".//vote-desc"))
        totals["yeas"] = safe_int(root.find(".//yea-total"))
        totals["nays"] = safe_int(root.find(".//nay-total"))
        totals["present"] = safe_int(root.find(".//present-total"))
        totals["not_voting"] = safe_int(root.find(".//not-voting-total"))

        for rv in root.findall(".//recorded-vote"):
            legislator = rv.find("legislator")
            vote_node = rv.find("vote")
            bioguide = legislator.get("name-id", "").strip() if legislator is not None else ""
            cast = safe_text(vote_node)
            if bioguide:
                member_votes[bioguide] = cast
    except ET.ParseError as exc:
        logger.warning("Failed parsing House detail XML %s: %s", xml_url, exc)
        append_error(db, "house_detail_parse", vote_id, str(exc))

    return vote_description, totals, member_votes


def process_house_votes(
    db: Dict[str, Any],
    congress: int,
    session: int,
    *,
    enrich_ai: bool = False
) -> int:
    logger.info("Syncing House votes for Congress %s Session %s", congress, session)

    existing_ids = find_existing_vote_ids(db)
    highest_roll = get_highest_roll_call_number(db, "House", congress, session)
    new_count = 0

    for item in fetch_house_vote_index(congress, session):
        try:
            roll = int(item.get("rollCallNumber", 0))
        except (TypeError, ValueError):
            logger.warning("Skipping House vote with invalid rollCallNumber: %s", item)
            continue

        vote_id = f"{congress}-{session}-H-{roll}"
        if vote_id in existing_ids or roll <= highest_roll:
            continue

        logger.info("Processing House roll %s", roll)

        xml_url = item.get("sourceDataURL")
        vote_description = ""
        totals = {"yeas": 0, "nays": 0, "present": 0, "not_voting": 0}
        member_votes: Dict[str, str] = {}

        if xml_url:
            vote_description, totals, member_votes = parse_house_vote_detail(
                xml_url, db, vote_id
            )
        else:
            append_error(db, "house_missing_xml", vote_id, "No sourceDataURL in index record")

        bill_type = (item.get("legislationType") or "").strip()
        bill_number = str(item.get("legislationNumber") or "").strip()

        summary = None
        if enrich_ai:
            summary = get_ai_summary(
                vote_description or item.get("question", ""),
                {"type": bill_type, "number": bill_number}
            )

        record = make_vote_record(
            chamber="House",
            congress=congress,
            session=session,
            roll_call_number=roll,
            date=item.get("startDate") or "",
            question=item.get("question") or "",
            result=item.get("result") or "",
            vote_description=vote_description,
            bill_type=bill_type,
            bill_number=bill_number,
            source_url=xml_url,
            member_votes=member_votes,
            totals=totals,
            ai_summary=summary
        )

        db["votes"].append(record)
        new_count += 1

    logger.info("Added %s new House votes.", new_count)
    return new_count

# -----------------------------------------------------------------------------
# Senate Sync
# -----------------------------------------------------------------------------

def fetch_senate_vote_index(congress: int, session: int) -> List[ET.Element]:
    url = f"https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_{congress}_{session}.xml"
    response = request_with_retries(url)
    if not response:
        return []

    try:
        root = ET.fromstring(response.content)
        votes = root.findall(".//vote")
        logger.info("Fetched %s Senate vote index records.", len(votes))
        return votes
    except ET.ParseError as exc:
        logger.error("Failed parsing Senate vote index XML: %s", exc)
        return []


def parse_senate_vote_detail(
    detail_url: str,
    id_map: Dict[str, str],
    db: Dict[str, Any],
    vote_id: str
) -> Tuple[Dict[str, int], Dict[str, str]]:
    """
    Fetches and parses a Senate vote detail XML file.

    Parse failures are recorded in db["errors"] for audit.
    """
    totals = {"yeas": 0, "nays": 0, "present": 0, "not_voting": 0}
    member_votes: Dict[str, str] = {}

    response = request_with_retries(detail_url)
    if not response:
        append_error(db, "senate_detail_fetch", vote_id, f"No response from {detail_url}")
        return totals, member_votes

    try:
        root = ET.fromstring(response.content)

        count = root.find(".//count")
        if count is not None:
            totals["yeas"] = safe_int(count.find("yeas"))
            totals["nays"] = safe_int(count.find("nays"))
            totals["present"] = safe_int(count.find("present"))
            totals["not_voting"] = safe_int(count.find("absent"))

        for member in root.findall(".//member"):
            lis_id = safe_text(member.find("lis_member_id"))
            vote_cast = safe_text(member.find("vote_cast"))
            
            # The Fix: Fallback to the lis_id if the Bioguide map is empty or fails
            bioguide = id_map.get(lis_id) or lis_id
            
            if bioguide:
                member_votes[bioguide] = vote_cast
    except ET.ParseError as exc:
        logger.warning("Failed parsing Senate detail XML %s: %s", detail_url, exc)
        append_error(db, "senate_detail_parse", vote_id, str(exc))

    return totals, member_votes


def process_senate_votes(
    db: Dict[str, Any],
    congress: int,
    session: int,
    id_map: Dict[str, str],
    *,
    enrich_ai: bool = False
) -> int:
    """
    Syncs Senate votes for a given Congress and session.

    The Senate LIS -> Bioguide id_map is accepted as a parameter rather than
    built internally. This avoids a redundant network fetch when syncing
    multiple sessions in one run.
    """
    logger.info("Syncing Senate votes for Congress %s Session %s", congress, session)

    existing_ids = find_existing_vote_ids(db)
    highest_roll = get_highest_roll_call_number(db, "Senate", congress, session)
    new_count = 0

    for vote in fetch_senate_vote_index(congress, session):
        vote_number_text = safe_text(vote.find("vote_number"))
        try:
            roll = int(vote_number_text)
        except ValueError:
            logger.warning("Skipping Senate vote with invalid vote_number: %s", vote_number_text)
            continue

        vote_id = f"{congress}-{session}-S-{roll}"
        if vote_id in existing_ids or roll <= highest_roll:
            continue

        logger.info("Processing Senate roll %s", roll)

        padded_number = vote_number_text.zfill(5)
        detail_url = (
            f"https://www.senate.gov/legislative/LIS/roll_call_votes/"
            f"vote{congress}{session}/vote_{congress}_{session}_{padded_number}.xml"
        )

        totals, member_votes = parse_senate_vote_detail(detail_url, id_map, db, vote_id)

        question = safe_text(vote.find("question"))
        title = safe_text(vote.find("title"))
        result = safe_text(vote.find("result"))
        vote_date = safe_text(vote.find("vote_date"))

        summary = None
        if enrich_ai:
            summary = get_ai_summary(
                title or question,
                {"type": "Senate Vote", "number": vote_number_text}
            )

        record = make_vote_record(
            chamber="Senate",
            congress=congress,
            session=session,
            roll_call_number=roll,
            date=vote_date,
            question=question,
            result=result,
            vote_description=title,
            bill_type="Senate Vote",
            bill_number=vote_number_text,
            source_url=detail_url,
            member_votes=member_votes,
            totals=totals,
            ai_summary=summary
        )

        db["votes"].append(record)
        new_count += 1

    logger.info("Added %s new Senate votes.", new_count)
    return new_count

# -----------------------------------------------------------------------------
# AI Enrichment Pass
# -----------------------------------------------------------------------------

def enrich_missing_summaries(db: Dict[str, Any], max_items: Optional[int] = None) -> int:
    """
    Fills in missing or placeholder AI summaries without re-fetching vote data.
    """
    updated = 0

    for vote in db.get("votes", []):
        current_summary = vote.get("bill", {}).get("ai_summary", "").strip()
        needs_summary = (
            not current_summary
            or current_summary == "Summary pending."
            or current_summary.startswith("Summary pending:")
        )

        if not needs_summary:
            continue

        bill = vote.get("bill", {})
        source_text = vote.get("vote_description") or vote.get("question") or ""
        summary = get_ai_summary(
            source_text,
            {"type": bill.get("type", ""), "number": bill.get("number", "")}
        )
        vote["bill"]["ai_summary"] = summary
        updated += 1

        logger.info("Generated AI summary for %s", vote.get("id"))

        if max_items is not None and updated >= max_items:
            break

    return updated

# -----------------------------------------------------------------------------
# Main Orchestration
# -----------------------------------------------------------------------------

def build_vote_database(
    *,
    congress: Optional[int] = None,
    session: Optional[int] = None,
    filename: str = DEFAULT_OUTPUT_FILE,
    enrich_ai_during_sync: bool = False,
    enrich_missing_after_sync: bool = True,
    enrich_limit: Optional[int] = None
) -> None:
    """
    Main entry point. Syncs House and Senate votes for the given Congress/session,
    then optionally enriches any votes still missing AI summaries.

    The Senate ID map is built once here and passed into process_senate_votes()
    to avoid redundant network fetches if this function is later extended to
    sync multiple sessions in a single run.
    """
    if congress is None or session is None:
        congress, session = get_current_congress_and_session()

    logger.info("Starting sync for Congress %s Session %s", congress, session)

    db = load_database(filename)
    db["metadata"]["congress"] = congress
    db["metadata"]["session"] = session

    new_house = process_house_votes(
        db,
        congress,
        session,
        enrich_ai=enrich_ai_during_sync
    )

    # Build the Senate ID map once here rather than inside process_senate_votes
    # so it is only fetched once regardless of how many sessions are processed.
    senate_id_map = build_senate_id_map()

    new_senate = process_senate_votes(
        db,
        congress,
        session,
        senate_id_map,
        enrich_ai=enrich_ai_during_sync
    )

    if enrich_missing_after_sync:
        enriched = enrich_missing_summaries(db, max_items=enrich_limit)
        logger.info("Enriched %s vote summaries.", enriched)

    db["votes"].sort(key=vote_sort_key, reverse=True)
    db["metadata"]["generated_at"] = utc_now_iso()

    save_database(filename, db)

    logger.info(
        "Done. Added %s House votes and %s Senate votes. Total votes: %s",
        new_house, new_senate, len(db.get("votes", []))
    )


if __name__ == "__main__":
    build_vote_database(
        enrich_ai_during_sync=False,
        enrich_missing_after_sync=True,
        enrich_limit=25
    )
