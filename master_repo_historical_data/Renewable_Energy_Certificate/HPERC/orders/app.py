import os
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from urllib.parse import urljoin

# MongoDB Connection Setup
client = MongoClient("mongodb://localhost:27017/")  # Change if necessary
db = client["hperc_db"]
collection = db["orders"]

# Folder setup for storing PDFs
BASE_DIR = "Renewable_Energy_Certificate_Registry/HPERC/Orders"
os.makedirs(BASE_DIR, exist_ok=True)

# URLs to scrape
urls = [
    "https://hperc.org/?page_id=143",
    "https://hperc.org/?page_id=147",
    "https://hperc.org/?page_id=145",
    "https://hperc.org/?page_id=141",
    "https://hperc.org/?page_id=160",
    "https://hperc.org/?page_id=164"
]

def download_pdf(pdf_url, folder):
    filename = pdf_url.split("/")[-1]
    file_path = os.path.join(folder, filename)
    
    response = requests.get(pdf_url, stream=True)
    if response.status_code == 200:
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        print(f"Downloaded: {filename}")
        return filename, file_path
    else:
        print(f"Failed to download {filename}")
        return None, None

def extract_data(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    
    table = soup.find("table", {"border": "1px"})
    if not table:
        print(f"No table found on {url}")
        return []
    
    data_list = []
    for row in table.find_all("tr")[1:]:  # Skipping the header row
        columns = row.find_all("td")
        if len(columns) < 2:
            continue
        
        description_tag = columns[1].find("a")
        date_of_order = columns[1].find("td", style="border-left: solid grey 1px;")
        
        if description_tag and date_of_order:
            description = description_tag.text.strip()
            pdf_url = urljoin(url, description_tag["href"])  # Absolute URL for the PDF
            date_of_order = date_of_order.text.strip()
            
            year = date_of_order.split("-")[-1]  # Extracting year from date
            file_name, file_path = download_pdf(pdf_url, BASE_DIR)
            
            if file_name:
                record = {
                    "year": year,
                    "description": description,
                    "date_of_order": "-".join(date_of_order.split("-")),
                    "file_name": file_name,
                    "file_path": file_path,
                }
                data_list.append(record)
                collection.insert_one(record)
                print(f"Inserted into MongoDB: {record}")
    
    return data_list

def main():
    all_data = []
    for url in urls:
        print(f"Scraping: {url}")
        all_data.extend(extract_data(url))
    print("Scraping complete.")

if __name__ == "__main__":
    main()
