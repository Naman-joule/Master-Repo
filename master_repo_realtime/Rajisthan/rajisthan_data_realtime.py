import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
from datetime import datetime

def scrape_and_store():
    # MongoDB setup
    client = MongoClient("mongodb://localhost:27017/")
    db = client["scraping_repository"]
    collection = db["realtime_overview"]

    # Configure WebDriver
    options = Options()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        # Open the URL
        url = "https://sldc.rajasthan.gov.in/rrvpnl/rajasthan-overview"
        driver.get(url)

        # Wait for the table to load (waiting for tbody with rows)
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#overview_data tbody tr")))

        # Locate the table
        table = driver.find_element(By.ID, "overview_data")
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")

        # Extract data from the table
        record = {
            "inserted_at": datetime.now().strftime("%Y-%m-%d %H:%M")
        }

        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) == 3:  # Ensure the row has the correct number of columns
                scada_name = cols[0].text.strip()
                value = float(cols[1].text.strip())
                datetime_str = cols[2].text.strip()

                # Parse datetime into separate Date and Time fields
                dt_object = datetime.strptime(datetime_str, "%d %b %Y %H:%M:%S")
                record["Date"] = dt_object.strftime("%Y-%m-%d")
                record["Time"] = dt_object.strftime("%H:%M")
                record[scada_name] = value

        # Check if the new record is different from the last inserted record
        last_record = collection.find_one(sort=[("_id", -1)])
        if last_record:
            # Remove MongoDB-specific fields for comparison
            last_record.pop("_id", None)
            last_record.pop("inserted_at", None)

        if last_record != record:
            collection.insert_one(record)
            print("Data inserted successfully into MongoDB")
        else:
            print("No changes detected. Data not inserted.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the WebDriver and MongoDB connection
        driver.quit()
        client.close()

# Run the function every 1 minute
if __name__ == "__main__":
    while True:
        scrape_and_store()
        time.sleep(60)