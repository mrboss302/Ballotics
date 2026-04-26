import requests
import json

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

def test_sos_api_scraper():
    print("--- TESTING GEORGIA SOS MODERN API ---")
    
    # The golden URL you found!
    url = "https://results.sos.ga.gov/results/public/api/elections/Georgia/40726SpecialElection/data"
    
    print(f"\n1. Fetching data from: {url}")
    response = requests.get(url, headers=HEADERS)
    
    if response.status_code != 200:
        print(f"Failed to fetch data: {response.status_code}")
        return
        
    data = response.json()
    
    print("\n2. Root JSON Keys:")
    print(list(data.keys()))
    
    print("\n3. Peeking at the data structure...")
    
    # Usually modern APIs have a 'contests', 'races', or 'results' key
    # Let's try to find the list of races
    for key in ['contests', 'races', 'results', 'election']:
        if key in data:
            items = data[key]
            print(f"\nFound '{key}' array with {len(items)} items.")
            
            if len(items) > 0:
                print("\n--- First Item Sample ---")
                print(json.dumps(items[0], indent=2)[:1000]) # Print first 1000 chars of the first item
            return
            
    # If we didn't find those common keys, just print a chunk of the raw JSON so we can eyeball it
    print("\nCould not find a standard 'contests' array. Here is a raw sample:")
    print(json.dumps(data, indent=2)[:1000])

if __name__ == "__main__":
    test_sos_api_scraper()
