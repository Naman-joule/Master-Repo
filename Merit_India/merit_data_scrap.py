import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import re
from datetime import datetime, timedelta
import time
import json

# Function to clean and convert numerical values
def clean_number(value):
    try:
        cleaned_value = re.sub(r'[^\d.-]', '', value).strip()
        return float(cleaned_value) if cleaned_value else 0.0
    except ValueError:
        print(f"Error converting value: '{value}'")
        return 0.0

# Function to extract the date of the data from the page
def extract_date(soup):
    try:
        date_element = soup.find("input", id="DateChangeStateWise")
        if date_element:
            date_text = date_element.get("value", "").strip()
            extracted_date = datetime.strptime(date_text, "%d %b %Y").strftime("%Y-%m-%d")
            print(f"Extracted Date: {extracted_date}")
            return extracted_date
        else:
            print("Date element not found. Using current date.")
            return datetime.now().strftime("%Y-%m-%d")
    except Exception as e:
        print(f"Error extracting date: {e}")
        return datetime.now().strftime("%Y-%m-%d")

# Function to fetch all state names and codes dynamically
def fetch_states():
    url = "https://meritindia.in/StateWiseDetails/BindStateListToRedirect"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }
    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        try:
            data = response.json()
            states = {state["StateName"].replace(" ", "-").lower(): state["StateCode"] for state in data}
            print(f"Fetched states: {states}")
            return states
        except Exception as e:
            print(f"Error processing states data: {e}")
            return {}
    else:
        print(f"Failed to fetch states data. Status Code: {response.status_code}, Response: {response.text}")
        return {}

# Function to dynamically choose the state and set up the MongoDB database
def setup_database_and_state(state_name, state_code):
    try:
        db_name = f"energy_data_{state_name.replace(' ', '_').lower()}"
        print(f"Setting up database for state: {state_name} ({state_code})")
        client = MongoClient("mongodb://localhost:27017/")
        db = client[db_name]
        return db
    except Exception as e:
        print(f"Error setting up database for {state_name}: {e}")
        return None

# Scraping portfolio data
def scrape_portfolio_data(soup, date, portfolio_collection):
    if portfolio_collection.find_one({"date": date}):
        print(f"Portfolio data for {date} already exists. Skipping.")
        return None

    portfolio_data = {"date": date}
    try:
        state_gen = soup.find("div", class_="state_gen_data")
        if state_gen:
            values = state_gen.find_all("div", class_="portfolio_sub_value")
            portfolio_data["state_generation_energy"] = clean_number(values[0].text) if len(values) > 0 else 0
            portfolio_data["state_generation_avg_MW"] = clean_number(values[1].text) if len(values) > 1 else 0
            portfolio_data["state_generation_MP"] = clean_number(values[2].text) if len(values) > 2 else 0
            portfolio_data["state_generation_avg_MP"] = clean_number(values[3].text) if len(values) > 3 else 0

        central_gen = soup.find("div", class_="central_gen_data")
        if central_gen:
            values = central_gen.find_all("div", class_="portfolio_sub_value")
            portfolio_data["central_isgs_energy"] = clean_number(values[0].text) if len(values) > 0 else 0
            portfolio_data["central_isgs_avg_MW"] = clean_number(values[1].text) if len(values) > 1 else 0
            portfolio_data["central_isgs_MP"] = clean_number(values[2].text) if len(values) > 2 else 0
            portfolio_data["central_isgs_avg_MP"] = clean_number(values[3].text) if len(values) > 3 else 0

        portfolio_collection.insert_one(portfolio_data)
        print(f"Portfolio data stored for {date}.")
        return portfolio_data

    except Exception as e:
        print(f"Error scraping portfolio data: {e}")
        return None

# Scraping station data
def scrape_station_data(soup, date, station_collection):
    if station_collection.find_one({"date": date}):
        print(f"Station data for {date} already exists. Skipping.")
        return []

    stations = []
    try:
        table = soup.find("table", class_="footable")
        rows = table.find("tbody").find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            station = {
                "date": date,
                "station": cells[1].text.strip(),
                "plant_capacity_MW": clean_number(cells[2].text),
                "capacity_allocated_to_state": clean_number(cells[3].text),
                "station_type": cells[4].text.strip(),
                "ownership": cells[5].text.strip(),
                "variable_cost": clean_number(cells[6].text),
                "fixed_cost": clean_number(cells[7].text),
                "total_cost": clean_number(cells[8].text),
                "declared_capability": clean_number(cells[9].text),
                "schedule": clean_number(cells[10].text)
            }
            stations.append(station)

        station_collection.insert_many(stations)
        print(f"Station data stored for {date}.")
        return stations

    except Exception as e:
        print(f"Error scraping station data: {e}")
        return []

# Scraping purchase data
def scrape_purchase_data(soup, date, purchase_data_collection):
    if purchase_data_collection.find_one({"date": date}):
        print(f"Purchase data for {date} already exists. Skipping.")
        return []

    purchases = []
    try:
        table = soup.find("table", id="CompletedRequest_table")
        rows = table.find("tbody").find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            purchase = {
                "date": date,
                "description": cells[1].text.strip(),
                "total_energy_purchased_during_day": clean_number(cells[2].text),
                "max_procurement_cost": clean_number(cells[3].text),
                "min_procurement_cost": clean_number(cells[4].text),
                "avg_procurement_cost": clean_number(cells[5].text),
                "power_purchased_max_rate_during_day": clean_number(cells[6].text)
            }
            purchases.append(purchase)

        purchase_data_collection.insert_many(purchases)
        print(f"Purchase data stored for {date}.")
        return purchases

    except Exception as e:
        print(f"Error scraping purchase data: {e}")
        return []

# Scraping and storing data for a specific state and date
def scrape_and_store_for_state_and_date(state_name, state_code, date):
    url = f"https://meritindia.in/state-data/{state_name.replace(' ', '-').lower()}"
    session = requests.Session()
    initial_response = session.get(url)
    soup = BeautifulSoup(initial_response.content, "html.parser")

    # Set up the MongoDB database
    db = setup_database_and_state(state_name, state_code)
    if db is None:
        print(f"Skipping data scraping for {state_name} due to database setup failure.")
        return

    portfolio_collection = db["portfolio"]
    station_collection = db["stations"]
    purchase_data_collection = db["purchase_data"]

    csrf_token = soup.find("input", {"name": "__RequestVerificationToken"})["value"]

    payload = {
        "StateCode": state_code,
        "StateName": state_name,
        "SelectedDateValue": date,
        "__RequestVerificationToken": csrf_token
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": url,
        "User-Agent": "Mozilla/5.0"
    }
    response = session.post(url, data=payload, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        page_date = extract_date(soup)

        scrape_portfolio_data(soup, page_date, portfolio_collection)
        scrape_station_data(soup, page_date, station_collection)
        scrape_purchase_data(soup, page_date, purchase_data_collection)

        print(f"Data scraped and stored for {state_name} on {page_date}.")
    else:
        print(f"Failed to fetch data for {state_name} on {date}. Status Code: {response.status_code}.")

if __name__ == "__main__":
    # Fetch all state names and codes dynamically
    states = fetch_states()

    start_date = datetime.strptime("2024-12-28", "%Y-%m-%d")
    end_date = datetime.now()

    for state_name, state_code in states.items():
        print(f"Scraping data for state: {state_name}")
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%d %b %Y")
            scrape_and_store_for_state_and_date(state_name, state_code, date_str)
            current_date += timedelta(days=1)