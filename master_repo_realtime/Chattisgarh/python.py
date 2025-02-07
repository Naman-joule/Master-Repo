import threading
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
import time
import urllib3

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def scrape_generation_data(table):
    try:
        generation_data = {}
        now = datetime.now()
        generation_data["Time"] = now.strftime("%H:%M")
        generation_data["Date"] = now.strftime("%Y-%m-%d")

        rows = table.find_all('tr')
        current_section = None
        for row in rows:
            cols = row.find_all('td')
            if len(cols) == 2:
                key = cols[0].text.strip().replace("#", "_unit_").replace(" ", "_").replace("__", "_").upper()
                value = float(cols[1].text.strip())
                if key == "TOTAL":
                    key = f"{current_section}_TOTAL" if current_section else "UNKNOWN_TOTAL"
                elif key == "TOTAL_OF_CSPGCL":
                    key = "CSPGCL_TOTAL"
                elif key == "TOTAL_OF_CSPGCL_&_IPP/CPP":
                    key = "CSPGCL_&_IPP/CPP_TOTAL"
                generation_data[key] = value
            elif len(cols) == 1 and "colspan" in cols[0].attrs:
                current_section = cols[0].text.strip().replace(" ", "_").upper()
        return generation_data
    except Exception as e:
        print(f"An error occurred while scraping generation data: {e}")
        return {}

def scrape_cg_system_summary(table):
    try:
        system_summary = {}
        now = datetime.now()
        system_summary["Time"] = now.strftime("%H:%M")
        system_summary["Date"] = now.strftime("%Y-%m-%d")

        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) == 2:
                key = cols[0].text.strip().replace(" ", "_").upper()
                value = float(cols[1].text.strip())
                system_summary[key] = value
        return system_summary
    except Exception as e:
        print(f"An error occurred while scraping system summary: {e}")
        return {}

def has_data_changed(new_data, latest_data):
    ignore_keys = {"inserted_at", "updated_at"}
    new_data_copy = {k: v for k, v in new_data.items() if k not in ignore_keys}
    latest_data_copy = {k: v for k, v in latest_data.items() if k not in ignore_keys}
    return new_data_copy != latest_data_copy

def save_to_mongodb(data, db_name, collection_name):
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client[db_name]
        collection = db[collection_name]

        latest_data = collection.find_one(sort=[("_id", -1)])
        now = datetime.now()
        if not latest_data or has_data_changed(data, latest_data):
            data["inserted_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
            data["updated_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
            collection.insert_one(data)
            print(f"New data inserted into the {collection_name} collection.")
        else:
            collection.update_one(
                {"_id": latest_data["_id"]},
                {"$set": {"updated_at": now.strftime("%Y-%m-%d %H:%M:%S")}}
            )
            print(f"No changes detected in the {collection_name} collection. Only updated the 'updated_at' field.")
    except Exception as e:
        print(f"An error occurred while saving to MongoDB: {e}")

def scrape_generation_every_2_minutes(url, db_name):
    while True:
        try:
            response = requests.get(url, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            generation_table = soup.find('table', class_='table table-bordered mytable')
            if generation_table:
                generation_data = scrape_generation_data(generation_table)
                save_to_mongodb(generation_data, db_name, "generation_data_chhattisgarh")
        except Exception as e:
            print(f"An error occurred during generation data scraping: {e}")
        time.sleep(120)  # Wait for 2 minutes

def scrape_summary_every_30_seconds(url, db_name):
    while True:
        try:
            response = requests.get(url, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            system_summary_table = soup.find('div', class_='updatea').find('table', class_='table')
            if system_summary_table:
                system_summary_data = scrape_cg_system_summary(system_summary_table)
                save_to_mongodb(system_summary_data, db_name, "cg_system_summary")
        except Exception as e:
            print(f"An error occurred during system summary scraping: {e}")
        time.sleep(30)  # Wait for 30 seconds

if __name__ == "__main__":
    url = "https://sldccg.com/gen.php"
    db_name = "scraping_repository"

    # Run scrapers in parallel threads
    threading.Thread(target=scrape_generation_every_2_minutes, args=(url, db_name)).start()
    threading.Thread(target=scrape_summary_every_30_seconds, args=(url, db_name)).start()