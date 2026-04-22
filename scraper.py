import os
import json
import requests
import xml.etree.ElementTree as ET
from openai import OpenAI
from datetime import datetime

# Load the secret API keys from GitHub Actions environment
congress_key = os.environ.get("CONGRESS_API_KEY")
openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

# Standard User-Agent to bypass basic government firewalls
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

def get_current_congress_and_session():
    """Dynamically calculates the current Congress and Session based on today's year."""
    now = datetime.utcnow()
    congress = (now.year - 1789) // 2 + 1
    session = 1 if now.year % 2 != 0 else 2
    return congress, session

def build_senate_id_map():
    """Fetches the Senate roster and maps lis_member_id -> bioguide_id"""
    print("Building Senate LIS to Bioguide map...")
    url = "https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml"
    
    response = requests.get(url, headers=HEADERS)
    id_map = {}
    
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        for member in root.findall('.//member'):
            lis_id = member.find('lis_member_id')
            bioguide = member.find('bioguideId') # Capital 'I' in the Senate XML
            
            if lis_id is not None and bioguide is not None:
                id_map[lis_id.text] = bioguide.text
    else:
        print("Warning: Failed to fetch Senate ID map.")
        
    return id_map

def get_ai_summary(bill_text):
    """Sends bill text to OpenAI with strict anti-hallucination guardrails."""
    system_prompt = (
        "You are an objective legal analyst. Your task is to summarize the provided congressional bill or vote description in 300 words or less. "
        "You must adhere strictly to the following rules:\n"
        "1. Base your summary EXACTLY and ONLY on the text provided. Do not invent, hallucinate, or infer details.\n"
        "2. Do not use external knowledge or attempt to explain outside context.\n"
        "3. Maintain absolute political neutrality and impartiality.\n"
        "4. If the provided text is too brief to summarize (e.g., just a short title or a technical amendment like 'Strike paragraph 3'), "
        "do not guess. Reply exactly with: 'No substantive text available to summarize.'"
    )

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Summarize this text: {bill_text}"}
            ],
            temperature=0.0 # Forces deterministic, factual output
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"AI Error: {e}")
        return "Summary unavailable."

def process_house_votes(existing_data, highest_vote_num, congress, session):
    """Fetches and processes new House votes using the JSON API and Clerk XMLs."""
    print(f"Fetching House index for {congress} Session {session}...")
    url = f"https://api.congress.gov/v3/house-vote/{congress}/{session}?format=json&limit=250&api_key={congress_key}"
    
    response = requests.get(url)
    if response.status_code != 200:
        print("Failed to fetch House API.")
        return 0

    data = response.json()
    new_votes_processed = 0
    
    for vote in data.get("houseRollCallVotes", []):
        roll_num = int(vote.get("rollCallNumber", 0))
        
        # DELTA CHECK
        if roll_num <= highest_vote_num:
            continue
            
        print(f"Processing NEW House Roll Call {roll_num}...")
        
        clerk_xml_url = vote.get("sourceDataURL")
        member_votes_dict = {}
        totals_dict = {"yeas": 0, "nays": 0, "present": 0, "not_voting": 0}
        vote_description = ""
        
        # Parse the detailed Clerk XML
        if clerk_xml_url:
            try:
                xml_response = requests.get(clerk_xml_url, headers=HEADERS)
                xml_root = ET.fromstring(xml_response.content)
                
                # Extract descriptive text for the AI
                desc_element = xml_root.find('.//vote-desc')
                if desc_element is not None and desc_element.text:
                    vote_description = desc_element.text
                
                # Extract totals
                totals_dict["yeas"] = int(xml_root.find('.//yea-total').text or 0)
                totals_dict["nays"] = int(xml_root.find('.//nay-total').text or 0)
                totals_dict["present"] = int(xml_root.find('.//present-total').text or 0)
                totals_dict["not_voting"] = int(xml_root.find('.//not-voting-total').text or 0)
                
                # Extract individual votes (name-id is the Bioguide ID)
                for recorded_vote in xml_root.findall('.//recorded-vote'):
                    legislator = recorded_vote.find('legislator')
                    vote_cast = recorded_vote.find('vote')
                    
                    if legislator is not None and vote_cast is not None:
                        bioguide_id = legislator.get('name-id')
                        if bioguide_id:
                            member_votes_dict[bioguide_id] = vote_cast.text
            except Exception as e:
                print(f"Error parsing House XML for roll {roll_num}: {e}")

        # Fallback text if XML doesn't have a description
        if not vote_description:
            vote_description = f"House Vote regarding {vote.get('legislationType', '')} {vote.get('legislationNumber', '')}"
            
        summary = get_ai_summary(vote_description)
        
        vote_record = {
            "id": f"{congress}-{session}-H-{roll_num}",
            "chamber": "House",
            "congress": congress,
            "session": session,
            "roll_call_number": roll_num,
            "date": vote.get("startDate"),
            "question": vote.get("question", "On Passage"), 
            "result": vote.get("result"),
            "bill": {
                "legislation_type": vote.get("legislationType"),
                "legislation_number": vote.get("legislationNumber"),
                "ai_summary": summary
            },
            "totals": totals_dict,
            "member_votes": member_votes_dict
        }
        
        existing_data["votes"].append(vote_record)
        new_votes_processed += 1
        
    return new_votes_processed

def process_senate_votes(existing_data, highest_vote_num, congress, session):
    """Fetches and processes new Senate votes using the XML index and translating IDs."""
    senate_id_map = build_senate_id_map()
    
    print(f"Fetching Senate XML index for {congress} Session {session}...")
    index_url = f"https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_{congress}_{session}.xml"
    
    response = requests.get(index_url, headers=HEADERS)
    if response.status_code != 200:
        print("Failed to fetch Senate XML index.")
        return 0
        
    root = ET.fromstring(response.content)
    new_votes_processed = 0
    
    for vote in root.findall('.//vote'):
        vote_num_str = vote.find('vote_number').text
        roll_num = int(vote_num_str)
        
        # DELTA CHECK
        if roll_num <= highest_vote_num:
            continue
            
        print(f"Processing NEW Senate Roll Call {roll_num}...")
        question = vote.find('question').text if vote.find('question') is not None else ""
        result = vote.find('result').text if vote.find('result') is not None else ""
        title = vote.find('title').text if vote.find('title') is not None else ""
        
        member_votes_dict = {}
        totals_dict = {"yeas": 0, "nays": 0, "present": 0, "not_voting": 0}
        vote_description = title
        
        # Parse the detailed Senate XML
        detail_url = f"https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{vote_num_str.zfill(5)}.xml"
        try:
            detail_response = requests.get(detail_url, headers=HEADERS)
            if detail_response.status_code == 200:
                detail_root = ET.fromstring(detail_response.content)
                
                # Extract extended text for AI
                doc_text = detail_root.find('.//vote_document_text')
                if doc_text is not None and doc_text.text:
                    vote_description += " " + doc_text.text
                
                # Extract totals
                count_element = detail_root.find('.//count')
                if count_element is not None:
                    totals_dict["yeas"] = int(count_element.find('yeas').text or 0)
                    totals_dict["nays"] = int(count_element.find('nays').text or 0)
                    totals_dict["present"] = int(count_element.find('present').text or 0)
                    totals_dict["not_voting"] = int(count_element.find('absent').text or 0)
                
                # Extract individual votes and translate LIS -> Bioguide
                for member in detail_root.findall('.//member'):
                    lis_id = member.find('lis_member_id').text
                    vote_cast = member.find('vote_cast').text
                    
                    if lis_id and vote_cast:
                        # Translate ID, fallback to LIS if not found
                        bioguide_id = senate_id_map.get(lis_id, lis_id)
                        member_votes_dict[bioguide_id] = vote_cast
        except Exception as e:
            print(f"Error parsing Senate detail XML for roll {roll_num}: {e}")

        summary = get_ai_summary(vote_description)
        
        vote_record = {
            "id": f"{congress}-{session}-S-{roll_num}",
            "chamber": "Senate",
            "congress": congress,
            "session": session,
            "roll_call_number": roll_num,
            "date": vote.find('vote_date').text if vote.find('vote_date') is not None else "",
            "question": question,
            "result": result,
            "bill": {
                "legislation_type": "Senate Issue",
                "legislation_number": vote.find('issue').text if vote.find('issue') is not None else "",
                "ai_summary": summary
            },
            "totals": totals_dict,
            "member_votes": member_votes_dict
        }
        
        existing_data["votes"].append(vote_record)
        new_votes_processed += 1
        
    return new_votes_processed

def build_vote_database():
    """Main orchestration function to build and update the database."""
    congress, session = get_current_congress_and_session()
    filename = "latest-votes.json"
    
    # Default structure
    existing_data = {
        "metadata": {"congress": congress, "session": session}, 
        "votes": []
    }
    
    # Read existing database
    if os.path.exists(filename):
        try:
            with open(filename, "r") as infile:
                existing_data = json.load(infile)
                print(f"Loaded existing database with {len(existing_data.get('votes', []))} total votes.")
        except Exception as e:
            print(f"Error reading local JSON, starting fresh: {e}")
            
    # Find high-water marks
    highest_house = 0
    highest_senate = 0
    
    for vote in existing_data.get("votes", []):
        roll_num = vote.get("roll_call_number", 0)
        if vote.get("chamber") == "House" and roll_num > highest_house:
            highest_house = roll_num
        elif vote.get("chamber") == "Senate" and roll_num > highest_senate:
            highest_senate = roll_num

    print(f"High-water marks -> House: {highest_house}, Senate: {highest_senate}")

    # Process new data
    new_house = process_house_votes(existing_data, highest_house, congress, session)
    new_senate = process_senate_votes(existing_data, highest_senate, congress, session)
    
    total_new = new_house + new_senate

    # Save to disk
    if total_new > 0:
        existing_data["metadata"]["generated_at"] = datetime.utcnow().isoformat() + "Z"
        with open(filename, "w") as outfile:
            json.dump(existing_data, outfile, indent=2)
        print(f"Successfully added {total_new} new votes ({new_house} House, {new_senate} Senate) to {filename}.")
    else:
        print("No new votes found. File is already up to date.")

if __name__ == "__main__":
    build_vote_database()
