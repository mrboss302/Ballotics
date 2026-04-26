import requests
import json

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

def test_sos_api_scraper():
    print("--- TESTING GEORGIA SOS MODERN API ---")
    
    url = "https://results.sos.ga.gov/results/public/api/elections/Georgia/40726SpecialElection/data"
    
    print(f"\n1. Fetching data from: {url}")
    response = requests.get(url, headers=HEADERS)
    
    if response.status_code != 200:
        print(f"Failed to fetch data: {response.status_code}")
        return
        
    data = response.json()
    
    print("\n2. Root Keys and their Data Types:")
    for key, value in data.items():
        print(f"  - {key}: {type(value).__name__} (Length: {len(value)})")
    
    print("\n3. Peeking at 'ballotItems' (The Races & Candidates)...")
    if 'ballotItems' in data:
        # Print the first 1000 characters to see the schema
        print(json.dumps(data['ballotItems'], indent=2)[:1000])
        
    print("\n4. Peeking at 'ballotItemWithBreakdown' (The Votes!)...")
    if 'ballotItemWithBreakdown' in data:
        # Get the first key from the dictionary to peek at its contents
        first_key = list(data['ballotItemWithBreakdown'].keys())[0]
        sample = data['ballotItemWithBreakdown'][first_key]
        print(f"\nSample for ID {first_key}:")
        print(json.dumps(sample, indent=2)[:1000])

if __name__ == "__main__":
    test_sos_api_scraper()
