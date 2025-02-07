import os
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from urllib.parse import urljoin
import time

# MongoDB Connection Setup
client = MongoClient("mongodb://localhost:27017/")  # Change if necessary
db = client["hperc_db"]
collection = db["cases"]

# Folder setup for storing PDFs
BASE_DIR = "Renewable_Energy_Certificate_Registry/HPERC/Cases"
os.makedirs(BASE_DIR, exist_ok=True)

# URLs to scrape
urls = [
    "https://hperc.org/?page_id=278",
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

def get_all_pages(base_url):
    pages = [base_url]
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, "html.parser")
    
    while True:
        next_page = soup.find("a", class_="paginate_button next")
        if not next_page or "disabled" in next_page.get("class", []):
            break
        next_page_url = urljoin(base_url, next_page["href"])
        pages.append(next_page_url)
        response = requests.get(next_page_url)
        soup = BeautifulSoup(response.text, "html.parser")
        time.sleep(1)
    
    return pages

def extract_data(url):
    data_list = []
    pages = get_all_pages(url)
    
    for page_url in pages:
        print(f"Scraping: {page_url}")
        response = requests.get(page_url)
        soup = BeautifulSoup(response.text, "html.parser")
        
        table = soup.find("table", {"id": "tablepress-4"})
        if not table:
            print(f"No table found on {page_url}")
            continue
        
        for row in table.find("tbody").find_all("tr"):
            columns = row.find_all("td")
            if len(columns) < 4:
                continue
            
            case_no = columns[1].text.strip()
            description_tag = columns[2].find("a")
            date_of_order = columns[3].text.strip().replace(".", "-")
            
            if description_tag:
                description = description_tag.text.strip()
                pdf_url = urljoin(page_url, description_tag["href"])  # Absolute URL for the PDF
                
                year = date_of_order.split("-")[-1]  # Extracting year from date
                file_name, file_path = download_pdf(pdf_url, BASE_DIR)
                
                if file_name:
                    record = {
                        "year": year,
                        "case_no": case_no,
                        "description": description,
                        "date_of_order": date_of_order,
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
        all_data.extend(extract_data(url))
    print("Scraping complete.")

if __name__ == "__main__":
    main()