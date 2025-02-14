import requests
import time
import json
import mysql.connector
import re
from datetime import datetime

# 🔹 Replace with your ScraperAPI key
SCRAPER_API_KEY = "e7e88af9057b9cf9d36378ed364728a8"

# 🔹 MySQL Database Connection
def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Admin@123",
        database="scraping_repository"
    )

# 🔹 Fetch data via ScraperAPI
def fetch_with_scraperapi(url, payload=None):
    scraper_url = f"http://api.scraperapi.com/?api_key={SCRAPER_API_KEY}&url={url}"
    headers = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}

    response = requests.post(scraper_url, headers=headers, json=payload, timeout=30)
    return response

# 🔹 Retry mechanism with exponential backoff
def fetch_with_retry(url, payload=None, retries=5):
    for attempt in range(retries):
        try:
            response = fetch_with_scraperapi(url, payload)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            wait_time = 2 ** attempt
            print(f"Request failed: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    
    log_error("Global", f"Failed to fetch {url} after {retries} retries.")
    return None

# 🔹 Fetch state list
def fetch_states():
    url = "https://meritindia.in/StateWiseDetails/BindStateListToRedirect"
    response = fetch_with_retry(url)

    if response:
        try:
            states_data = response.json()
            return {state["StateName"]: state["StateCode"] for state in states_data}
        except json.JSONDecodeError as e:
            log_error("Global", f"Error parsing states JSON: {e}")
            return {}
    return {}

# 🔹 Fetch dynamic demand data for all states
def fetch_dynamic_demand_data():
    url = "https://meritindia.in/StateWiseDetails/BindCurrentStateStatus"

    states = fetch_states()
    if not states:
        print("No states fetched. Skipping data retrieval.")
        return

    for state_name, state_code in states.items():
        payload = {"StateCode": state_code}
        response = fetch_with_retry(url, payload)

        if response:
            try:
                data = response.json()
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                for item in data:
                    demand = item.get("Demand")
                    own_generation = item.get("ISGS")
                    import_value = item.get("ImportData")

                    if demand is None and own_generation is None and import_value is None:
                        log_error(state_name, "No data available for this state.")
                        continue

                    demand_data = {
                        "state_name": state_name,
                        "timestamp": timestamp,
                        "demand_met": clean_number(demand),
                        "own_generation": clean_number(own_generation),
                        "import_value": clean_number(import_value)
                    }
                    insert_demand_data(demand_data)

            except json.JSONDecodeError as e:
                log_error(state_name, f"Error parsing demand JSON: {e}")
        else:
            log_error(state_name, f"Failed to fetch demand data after retries.")

# 🔹 Function to insert demand data into MySQL
def insert_demand_data(demand_data):
    db = connect_db()
    cursor = db.cursor()

    cursor.execute("""
        SELECT demand_met, own_generation, import_value FROM dynamic_demand
        WHERE state_name = %s ORDER BY timestamp DESC LIMIT 1
    """, (demand_data["state_name"],))
    
    last_entry = cursor.fetchone()

    if not last_entry or (
        demand_data["demand_met"] != last_entry[0] or
        demand_data["own_generation"] != last_entry[1] or
        demand_data["import_value"] != last_entry[2]
    ):
        cursor.execute("""
            INSERT INTO dynamic_demand (state_name, timestamp, demand_met, own_generation, import_value)
            VALUES (%s, %s, %s, %s, %s)
        """, (demand_data["state_name"], demand_data["timestamp"], demand_data["demand_met"],
              demand_data["own_generation"], demand_data["import_value"]))
        db.commit()
        print(f"Stored data for {demand_data['state_name']}: {demand_data}")

    cursor.close()
    db.close()

# 🔹 Function to log errors into MySQL
def log_error(state_name, error_message):
    db = connect_db()
    cursor = db.cursor()
    
    cursor.execute("""
        INSERT INTO error_logs (state_name, error_message)
        VALUES (%s, %s)
    """, (state_name, error_message))
    
    db.commit()
    cursor.close()
    db.close()
    
    print(f"Logged error for {state_name}: {error_message}")

# 🔹 Function to clean and convert numerical values
def clean_number(value):
    try:
        if value is None:
            return None
        cleaned_value = re.sub(r'[^\d.-]', '', value).strip().replace(',', '')
        return float(cleaned_value) if cleaned_value else None
    except ValueError:
        return None

# 🔹 Main loop for continuous data collection
if __name__ == "__main__":
    print("Starting dynamic demand data fetching every 1 minute...")
    try:
        while True:
            fetch_dynamic_demand_data()
            time.sleep(60)
    except KeyboardInterrupt:
        print("Stopping dynamic demand data fetching...")