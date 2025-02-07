import requests
from pymongo import MongoClient
import json
from datetime import datetime
import time
import re

# MongoDB setup
client = MongoClient("mongodb://localhost:27017/")
db = client["scraping_repository"]  # Single database for all states

# Function to clean and convert numerical values
def clean_number(value):
    try:
        if value is None:
            return None
        cleaned_value = re.sub(r'[^\d.-]', '', value).strip().replace(',', '')
        return float(cleaned_value) if cleaned_value else None
    except ValueError:
        print(f"Error converting value: {value}")
        return None

# Function to fetch dynamic demand data for all states
def fetch_dynamic_demand_data():
    url = "https://meritindia.in/StateWiseDetails/BindCurrentStateStatus"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }

    # Fetch all states
    states_url = "https://meritindia.in/StateWiseDetails/BindStateListToRedirect"
    states_response = requests.post(states_url, headers=headers)

    if states_response.status_code == 200:
        try:
            states_data = states_response.json()
            states = {state["StateName"].replace(" ", "-").lower(): state["StateCode"] for state in states_data}
            print(f"Fetched states: {states}")
        except Exception as e:
            print(f"Error processing states data: {e}")
            return
    else:
        print(f"Failed to fetch states data. Status Code: {states_response.status_code}, Response: {states_response.text}")
        return

    # Iterate over all states and fetch demand data
    for state_name, state_code in states.items():
        payload = {"StateCode": state_code}
        response = requests.post(url, data=json.dumps(payload), headers=headers)

        if response.status_code == 200:
            try:
                data = response.json()
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Collection for the specific state
                state_collection = db[state_name]

                for item in data:
                    demand = item.get("Demand")
                    own_generation = item.get("ISGS")
                    import_value = item.get("ImportData")

                    # Skip if all values are missing
                    if demand is None and own_generation is None and import_value is None:
                        print(f"No data available for {state_name}. Skipping this state.")
                        continue

                    demand_data = {
                        "timestamp": timestamp,
                        "demand_met": clean_number(demand),
                        "own_generation": clean_number(own_generation),
                        "import": clean_number(import_value)
                    }

                    # Check the most recent document in the collection
                    last_entry = state_collection.find_one(sort=[("_id", -1)])

                    # Insert only if data has changed
                    if not last_entry or (
                        demand_data["demand_met"] != last_entry.get("demand_met") or
                        demand_data["own_generation"] != last_entry.get("own_generation") or
                        demand_data["import"] != last_entry.get("import")
                    ):
                        state_collection.insert_one(demand_data)
                        print(f"Dynamic demand data for {state_name} stored successfully: {demand_data}")
                    else:
                        print(f"No changes in data for {state_name}. Skipping insertion.")

            except Exception as e:
                print(f"Error processing dynamic demand data for {state_name}: {e}")
        else:
            print(f"Failed to fetch demand data for {state_name}. Status Code: {response.status_code}, Response: {response.text}")

if __name__ == "__main__":
    print("Starting dynamic demand data fetching every 1 minute...")
    try:
        while True:
            fetch_dynamic_demand_data()
            time.sleep(60)  # Fetch data every 1 minute
    except KeyboardInterrupt:
        print("Stopping dynamic demand data fetching...")