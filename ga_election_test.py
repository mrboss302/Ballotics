import requests

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

def test_clarity_scraper():
    print("--- TESTING GEORGIA 2024 ELECTION SCRAPER (HALL COUNTY) ---")
    
    base_url = "https://results.enr.clarityelections.com/GA/Hall/122850"
    
    print("\n1. Fetching current data version...")
    ver_response = requests.get(f"{base_url}/current_ver.txt", headers=HEADERS)
    
    if ver_response.status_code != 200:
        print(f"Failed to fetch version: {ver_response.status_code}")
        return
        
    version = ver_response.text.strip()
    print(f"   -> Latest Version: {version}")
    
    summary_url = f"{base_url}/{version}/json/en/summary.json"
    print(f"\n2. Fetching summary JSON from: {summary_url}")
    
    sum_response = requests.get(summary_url, headers=HEADERS)
    if sum_response.status_code != 200:
        print(f"Failed to fetch summary: {sum_response.status_code}")
        return
        
    data = sum_response.json()
    
    print("\n3. Parsing Presidential Race Results...")
    
    # DEFENSIVE FIX: If the root is already a list, use it. Otherwise, look for 'Contests'.
    contests = data if isinstance(data, list) else data.get('Contests', [])
    
    for contest in contests:
        race_name = contest.get('C', '')
        
        if "President of the US" in race_name:
            print(f"\nRace Found: {race_name}")
            
            candidates = contest.get('CH', []) 
            votes = contest.get('V', [])       
            
            for idx, candidate in enumerate(candidates):
                try:
                    # DEFENSIVE FIX: Check if votes are a 2D array (split by Early/Absentee/Day-of) 
                    # or just a flat 1D array of totals.
                    if len(votes) > 0 and isinstance(votes[0], list):
                        total_votes = int(votes[0][idx])
                    else:
                        total_votes = int(votes[idx])
                        
                    print(f"  - {candidate}: {total_votes:,} votes")
                except (IndexError, TypeError, ValueError):
                    print(f"  - {candidate}: Data structure mismatch")
            break

if __name__ == "__main__":
    test_clarity_scraper()
