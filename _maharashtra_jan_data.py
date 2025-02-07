import os
import requests
from datetime import datetime, timedelta

# Base URL and file prefix
base_url = "https://mahasldc.in/wp-content/reports/"
file_prefix = "dr0_"

# Directory to save downloaded reports
save_path = r"D:\maharastra_jan_2025"
os.makedirs(save_path, exist_ok=True)

# Date range: 2025-01-01 to today
start_date = datetime(2025, 1, 1)
end_date = datetime.now()

def download_reports(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        # Format the date components
        day = current_date.strftime("%d")  # Day as two digits
        month = current_date.strftime("%m")  # Month as two digits
        year = current_date.strftime("%Y")  # Year as four digits

        # Construct the file URL
        file_name = f"{file_prefix}{day}{month}{year}.pdf"
        file_url = f"{base_url}{file_name}"
        file_path = os.path.join(save_path, file_name)

        try:
            # Attempt to download the file
            print(f"Downloading: {file_url}")
            response = requests.get(file_url)
            if response.status_code == 200:
                # Save the file if available
                with open(file_path, "wb") as file:
                    file.write(response.content)
                print(f"Saved: {file_path}")
            else:
                print(f"Report not available for {current_date.strftime('%Y-%m-%d')}: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {file_url}: {e}")

        # Move to the next date
        current_date += timedelta(days=1)

# Run the download function
download_reports(start_date, end_date)
