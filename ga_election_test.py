import requests
import json

# Standard headers to act like a normal web browser
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

def test_clarity_scraper():
    print("--- TESTING GEORGIA 2024 ELECTION SCRAPER (HALL COUNTY) ---")
    
    # 1. The Clarity Election ID for Hall County's Nov 2024 General Election is 122850
    base_url = "https://results.enr.clarityelections.com/GA/Hall/122850"
    
    # 2. First, we have to ask the server what the latest "version" of the data is
    print("\n1. Fetching current data version...")
    ver_response = requests.get(f"{base_url}/current_ver.txt", headers=HEADERS)
    
    if ver_response.status_code != 200:
        print(f"Failed to fetch version: {ver_response.status_code}")
        return
        
    version = ver_response.text.strip()
    print(f"   -> Latest Version: {version}")
    
    # 3. Now we use that version to download the hidden summary.json file
    summary_url = f"{base_url}/{version}/json/en/summary.json"
    print(f"\n2. Fetching summary JSON from: {summary_url}")
    
    sum_response = requests.get(summary_url, headers=HEADERS)
    if sum_response.status_code != 200:
        print(f"Failed to fetch summary: {sum_response.status_code}")
        return
        
    data = sum_response.json()
    
    print("\n3. Parsing Presidential Race Results...")
    # Clarity JSON is heavily minified to save bandwidth. 
    # 'Contests' is the list of races. 
    contests = data.get('Contests', [])
    
    for contest in contests:
        race_name = contest.get('C', '') # 'C' = Contest Name
        
        # Look specifically for the Presidential race
        if "President of the US" in race_name:
            print(f"\nRace Found: {race_name}")
            
            candidates = contest.get('CH', []) # 'CH' = Choices (Candidates)
            votes = contest.get('V', [])       # 'V' = Votes arrays
            
            # The Votes array is deeply nested. Usually votes[0][0] is the total votes for candidate 0.
            # Let's zip them together and print the results
            for idx, candidate in enumerate(candidates):
                try:
                    # Depending on the state's exact setup, total votes are usually the first element of the vote array
                    total_votes = votes[0][idx] 
                    print(f"  - {candidate}: {total_votes:,} votes")
                except IndexError:
                    print(f"  - {candidate}: Data structure mismatch")
            break

if __name__ == "__main__":
    test_clarity_scraper()
