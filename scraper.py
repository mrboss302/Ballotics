import os
import json
import requests
from openai import OpenAI
from datetime import datetime

# Load the secret API keys
congress_key = os.environ.get("CONGRESS_API_KEY")
openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def get_ai_summary(bill_text):
    """Sends bill text to OpenAI and returns a plain-English summary."""
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an objective legal analyst. Summarize this bill in 300 words or less. Be impartial."},
                {"role": "user", "content": f"Summarize this bill: {bill_text}"}
            ],
            temperature=0.2
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"AI Error: {e}")
        return "Summary unavailable."

def build_vote_database():
    filename = "latest-votes.json"
    
    # Default structure if the file doesn't exist yet
    existing_data = {
        "metadata": {"congress": 119, "session": 2}, 
        "votes": []
    }
    
    # 1. READ EXISTING DATA (Swift equivalent: FileManager + JSONDecoder)
    if os.path.exists(filename):
        with open(filename, "r") as infile:
            existing_data = json.load(infile)
            print("Loaded existing database from disk.")
            
    # 2. FIND HIGH-WATER MARK 
    highest_roll_call = 0
    for vote in existing_data.get("votes", []):
        roll_num = vote.get("roll_call_number", 0)
        if roll_num > highest_roll_call:
            highest_roll_call = roll_num
            
    print(f"Highest local roll call: {highest_roll_call}")

    # 3. FETCH API INDEX
    url = f"https://api.congress.gov/v3/house-vote/119/2?format=json&limit=250&api_key={congress_key}"
    print("Fetching index from Congress.gov...")
    response = requests.get(url)
    data = response.json()
    
    new_votes_processed = 0
    
    # 4. FILTER AND PROCESS ONLY NEW VOTES
    for vote in data.get("houseRollCallVotes", []):
        roll_num = vote.get("rollCallNumber")
        
        # THE DELTA CHECK: Skip if we already have it in the JSON file
        if roll_num <= highest_roll_call:
            continue
            
        print(f"Processing NEW Roll Call {roll_num}...")
        bill_num = vote.get("legislationNumber", "Unknown")
        
        # Placeholder for where the XML parser will go
        placeholder_bill_text = f"This is the official text for bill {bill_num}."
        
        # Ask AI for the summary (This ONLY costs money for brand new votes)
        summary = get_ai_summary(placeholder_bill_text)
        
        # Map the data to our Unified Schema
        vote_record = {
            "id": f"119-2-H-{roll_num}",
            "chamber": "House",
            "congress": 119,
            "session": 2,
            "roll_call_number": roll_num,
            "date": vote.get("startDate"),
            "question": "On Passage",
            "result": vote.get("result"),
            "bill": {
                "legislation_type": vote.get("legislationType"),
                "legislation_number": bill_num,
                "ai_summary": summary
            },
            "totals": { "yeas": 218, "nays": 210, "present": 1, "not_voting": 6 },
            "member_votes": { "M001157": "Yea" }
        }
        
        # Append the new dictionary to our main list
        existing_data["votes"].append(vote_record)
        new_votes_processed += 1

    # 5. SAVE DATA (Swift equivalent: JSONEncoder)
    if new_votes_processed > 0:
        # Update the generation timestamp
        existing_data["metadata"]["generated_at"] = datetime.utcnow().isoformat() + "Z"
        
        # Write the whole structure back to the file
        with open(filename, "w") as outfile:
            json.dump(existing_data, outfile, indent=2)
        print(f"Successfully added {new_votes_processed} new votes to {filename}.")
    else:
        print("No new votes found. File is already up to date.")

if __name__ == "__main__":
    build_vote_database()
