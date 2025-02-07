import os
import time
import logging
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB Connection Setup
client = MongoClient("mongodb://localhost:27017/")  # Change if necessary
db = client["hperc_db"]
collection = db["cases_by_year"]

# Folder setup for storing PDFs
BASE_DIR = "Renewable_Energy_Certificate_Registry/HPERC/Cases"
os.makedirs(BASE_DIR, exist_ok=True)

# URL to scrape
base_url = "https://hperc.org/?page_id=253"

def setup_driver():
    """Set up Selenium WebDriver."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=options)
    return driver

def get_years(driver):
    """Fetch all available years from the dropdown filter using Selenium."""
    driver.get(base_url)
    time.sleep(2)  # Allow page to load
    
    try:
        select_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "txtKeyword"))
        )
        select = Select(select_element)
        years = [option.get_attribute("value") for option in select.options]
        logging.info(f"Years fetched: {years}")
        return years
    except Exception as e:
        logging.error(f"Error fetching years: {e}")
        return []

def download_pdf(pdf_url, folder):
    filename = pdf_url.split("/")[-1]
    file_path = os.path.join(folder, filename)
    
    try:
        response = requests.get(pdf_url, stream=True)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            logging.info(f"Downloaded: {filename}")
            return filename, file_path
        else:
            logging.error(f"Failed to download {filename}")
    except requests.RequestException as e:
        logging.error(f"Error downloading {filename}: {e}")
    
    return None, None

def extract_data(driver, year):
    """Extract case data for a given year using Selenium."""
    logging.info(f"Scraping data for year: {year}")
    driver.get(base_url)
    time.sleep(2)  # Wait for the page to load
    
    try:
        select_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "txtKeyword"))
        )
        select = Select(select_element)
        select.select_by_value(year)
        time.sleep(2)  # Wait for the page to reload with filtered results
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        table = soup.find("table", {"border": "1px"})
        
        if not table:
            logging.warning(f"No table found for year {year}. Logging response:")
            logging.warning(driver.page_source[:1000])  # Log first 1000 chars for debugging
            return []
        
        tbody = table.find("tbody")
        if not tbody:
            logging.warning(f"No tbody found for year {year}. Logging response:")
            logging.warning(driver.page_source[:1000])  # Log first 1000 chars for debugging
            return []
        
        data_list = []
        for row in tbody.find_all("tr"):
            columns = row.find_all("td")
            if len(columns) < 4:
                continue
            
            case_no = columns[1].text.strip()
            description_tag = columns[2].find("a")
            date_of_order = columns[3].text.strip().replace(".", "-")
            
            if description_tag:
                description = description_tag.text.strip()
                pdf_url = urljoin(base_url, description_tag["href"])  # Absolute URL for the PDF
                
                file_name, file_path = download_pdf(pdf_url, BASE_DIR)
                
                if file_name:
                    record = {
                        "year": year.split("-")[0],
                        "case_no": case_no,
                        "description": description,
                        "date_of_order": date_of_order,
                        "file_name": file_name,
                        "file_path": file_path,
                    }
                    data_list.append(record)
                    collection.insert_one(record)
                    logging.info(f"Inserted into MongoDB: {record}")
        
        return data_list
    except Exception as e:
        logging.error(f"Error processing year {year}: {e}")
        return []

def main():
    driver = setup_driver()
    all_data = []
    years = get_years(driver)
    
    for year in years:
        try:
            all_data.extend(extract_data(driver, year))
        except Exception as e:
            logging.error(f"Error processing year {year}: {e}")
    
    driver.quit()
    logging.info("Scraping complete.")

if __name__ == "__main__":
    main()