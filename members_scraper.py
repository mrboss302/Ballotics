import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("ballotics-members")

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
OUTPUT_FILE = "latest-members.json"

def safe_text(node) -> str:
    return node.text.strip() if node is not None and node.text else ""

def strip_namespaces(root):
    """Removes annoying hidden namespaces from government XMLs."""
    for elem in root.iter():
        if '}' in elem.tag:
            elem.tag = elem.tag.split('}', 1)[1]
        elem.tag = elem.tag.lower()
    return root

def fetch_house_members() -> dict:
    logger.info("Fetching House Members...")
    url = "https://clerk.house.gov/xml/lists/MemberData.xml"
    response = requests.get(url, headers=HEADERS, timeout=15)
    if response.status_code != 200:
        logger.error("Failed to fetch House XML")
        return {}

    root = strip_namespaces(ET.fromstring(response.content))
    members = {}

    for member in root.findall(".//member"):
        info = member.find("member-info")
        if info is None: continue

        bioguide_node = info.find("bioguideid")
        bioguide = safe_text(bioguide_node)
        if not bioguide: continue

        state_node = info.find("state")
        state_postal = safe_text(state_node.find("postal-code")) if state_node is not None else ""

        members[bioguide] = {
            "bioguide_id": bioguide,
            "chamber": "House",
            "first_name": safe_text(info.find("firstname")),
            "last_name": safe_text(info.find("lastname")),
            "party": safe_text(info.find("party")),
            "state": state_postal,
            "house_data": {
                "district": safe_text(info.find("district")),
                "town_name": safe_text(info.find("townname")),
                "phone": safe_text(info.find("phone"))
            }
        }
    return members

def fetch_senate_members() -> dict:
    logger.info("Fetching Senate Members...")
    url = "https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml"
    response = requests.get(url, headers=HEADERS, timeout=15)
    if response.status_code != 200:
        logger.error("Failed to fetch Senate XML")
        return {}

    root = strip_namespaces(ET.fromstring(response.content))
    members = {}

    for person in root.findall(".//senator") + root.findall(".//member"):
        lis_id = person.get("lis_member_id") or safe_text(person.find("lis_member_id"))
        
        bio_node = person.find("bioguideid")
        if bio_node is None:
            bio_node = person.find("bioguide_id")
        bioguide = safe_text(bio_node)

        if not bioguide: continue
        
        name_node = person.find("name")
        first_name = safe_text(name_node.find("first")) if name_node is not None else ""
        last_name = safe_text(name_node.find("last")) if name_node is not None else ""

        members[bioguide] = {
            "bioguide_id": bioguide,
            "chamber": "Senate",
            "first_name": first_name,
            "last_name": last_name,
            "party": safe_text(person.find("party")),
            "state": safe_text(person.find("state")),
            "senate_data": {
                "lis_member_id": lis_id,
                "hometown": safe_text(person.find("hometown")),
                "office": safe_text(person.find("office")),
                "leadership_position": safe_text(person.find("leadership_position"))
            }
        }
    return members

def build_member_database():
    house_members = fetch_house_members()
    senate_members = fetch_senate_members()

    # Merge dictionaries
    all_members = {**house_members, **senate_members}

    # Load existing file to check for differences
    existing_data = {"metadata": {}, "members": {}}
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r") as f:
                existing_data = json.load(f)
        except Exception:
            pass

    # THE DELTA CHECK: Only update the timestamp if the roster actually changed
    old_members = existing_data.get("members", {})
    
    if all_members == old_members:
        logger.info("No roster changes detected. Skipping update to preserve timestamp.")
        return
        
    logger.info("Roster changes detected! Updating JSON...")
    
    new_data = {
        "metadata": {
            "last_updated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "total_members": len(all_members),
            "house_count": len(house_members),
            "senate_count": len(senate_members)
        },
        "members": all_members
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(new_data, f, indent=2, ensure_ascii=False)
        
    logger.info(f"Successfully saved {len(all_members)} members to {OUTPUT_FILE}.")

if __name__ == "__main__":
    build_member_database()
