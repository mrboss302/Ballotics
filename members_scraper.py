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

# Update these based on the current Congress session
HOUSE_LEADERSHIP = {
    "J000299": "Speaker of the House",      # Mike Johnson
    "S001176": "Majority Leader",           # Steve Scalise
    "E000294": "Majority Whip",             # Tom Emmer
    "J000294": "Minority Leader",           # Hakeem Jeffries
    "C001101": "Minority Whip"              # Katherine Clark
}

def safe_text(node) -> str:
    return node.text.strip() if node is not None and node.text else ""

def strip_namespaces(root):
    """Removes annoying hidden namespaces from government XMLs and lowercases tags."""
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
        state_postal = state_node.get("postal-code", "") if state_node is not None else ""

        # Dynamically extract all committee and subcommittee data
        committees = []
        committee_assignments = info.find("committee-assignments")
        if committee_assignments is not None:
            for c in committee_assignments.findall("committee"):
                com_data = {"type": "committee", "comcode": c.get("comcode", ""), "rank": c.get("rank", "")}
                if c.get("leadership"):
                    com_data["leadership"] = c.get("leadership")
                
                com_name = safe_text(c)
                if com_name:
                    com_data["name"] = com_name
                committees.append(com_data)
                
            for s in committee_assignments.findall("subcommittee"):
                sub_data = {"type": "subcommittee", "subcomcode": s.get("subcomcode", ""), "rank": s.get("rank", "")}
                if s.get("leadership"):
                    sub_data["leadership"] = s.get("leadership")
                
                sub_name = safe_text(s)
                if sub_name:
                    sub_data["name"] = sub_name
                committees.append(sub_data)

        # Extract dates safely
        elected_node = info.find("elected-date")
        sworn_node = info.find("sworn-date")

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
                "phone": safe_text(info.find("phone")),
                "office_building": safe_text(info.find("office-building")),
                "office_room": safe_text(info.find("office-room")),
                "office_zip": safe_text(info.find("office-zip")),
                "committees": committees,
                "leadership_position": HOUSE_LEADERSHIP.get(bioguide, ""), 
                "prior_congress": safe_text(info.find("prior-congress")),
                "caucus": safe_text(info.find("caucus")),
                "courtesy_title": safe_text(info.find("courtesy")),
                "official_name": safe_text(info.find("official-name")),
                "elected_date": elected_node.get("date", "") if elected_node is not None else "",
                "sworn_date": sworn_node.get("date", "") if sworn_node is not None else ""
            }
        }
    return members

def fetch_senate_members() -> dict:
    logger.info("Fetching Senate Members (CVC Data)...")
    url_cvc = "https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml"
    response_cvc = requests.get(url_cvc, headers=HEADERS, timeout=15)
    
    logger.info("Fetching Senate Members (CFM Contact Data)...")
    url_cfm = "https://www.senate.gov/general/contact_information/senators_cfm.xml"
    response_cfm = requests.get(url_cfm, headers=HEADERS, timeout=15)

    if response_cvc.status_code != 200 or response_cfm.status_code != 200:
        logger.error("Failed to fetch one or both Senate XMLs")
        return {}

    root_cvc = strip_namespaces(ET.fromstring(response_cvc.content))
    root_cfm = strip_namespaces(ET.fromstring(response_cfm.content))
    
    # Pre-parse CFM data into a dictionary by bioguide_id for easy lookup
    cfm_lookup = {}
    for member in root_cfm.findall(".//member"):
        bio_node = member.find("bioguide_id")
        bioguide = safe_text(bio_node)
        if not bioguide: continue
        
        cfm_lookup[bioguide] = {
            "senate_class": safe_text(member.find("class")),
            "address": safe_text(member.find("address")),
            "phone": safe_text(member.find("phone")),
            "email": safe_text(member.find("email")),
            "website": safe_text(member.find("website"))
        }

    members = {}

    for person in root_cvc.findall(".//senator") + root_cvc.findall(".//member"):
        lis_id = person.get("lis_member_id") or safe_text(person.find("lis_member_id"))
        
        bio_node = person.find("bioguideid")
        if bio_node is None:
            bio_node = person.find("bioguide_id")
        bioguide = safe_text(bio_node)

        if not bioguide: continue
        
        name_node = person.find("name")
        first_name = safe_text(name_node.find("first")) if name_node is not None else ""
        last_name = safe_text(name_node.find("last")) if name_node is not None else ""

        # Extract committees from CVC
        committees = []
        coms_node = person.find("committees")
        if coms_node is not None:
            for c in coms_node.findall("committee"):
                committees.append({
                    "type": "committee",
                    "code": c.get("code", ""),
                    "name": safe_text(c)
                })

        # Match extended data using bioguide_id
        extra_data = cfm_lookup.get(bioguide, {})

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
                "leadership_position": safe_text(person.find("leadership_position")),
                "committees": committees,
                "senate_class": extra_data.get("senate_class", ""),
                "address": extra_data.get("address", ""),
                "phone": extra_data.get("phone", ""), # Prefers CFM phone as it is frequently updated
                "email": extra_data.get("email", ""),
                "website": extra_data.get("website", "")
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
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
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
