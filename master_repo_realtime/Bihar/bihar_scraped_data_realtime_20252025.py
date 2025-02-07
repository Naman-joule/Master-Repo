import requests
import datetime
import time  # For adding delays
from pymongo import MongoClient

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "scraping repository"
COLLECTION_NAME = "yearly_data_2025"

# Initialize MongoDB client
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Function to fetch data from the API
def fetch_scada_data(date):
    url = f"https://sldc.bsptcl.co.in:8086/api/SCADA/Get/GetAllSCADADataDatewise?date={date}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()  # Assuming the response is JSON
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {date}: {e}")
        return None

# Function to format data to match the required schema
def format_data(data):
    formatted_data = []
    for record in data:
        try:
            # Correctly parse and reformat the Date and Time fields
            raw_date = record["Date"]
            day = raw_date[:2]  # Extract day (DD)
            month = raw_date[2:4]  # Extract month (MM)
            year = "20" + raw_date[4:6]  # Extract year (YY) and prefix with '20'
            date = f"{year}-{month}-{day}"  # Reformat to YYYY-MM-DD

            # Parse and format the Time field
            time = datetime.datetime.strptime(record["Time"], "%H%M%S").strftime("%H:%M")
            
            # Ensure RevNo is properly cast to an integer
            rev_no = record.get("RevNo", 0)
            if isinstance(rev_no, str) or isinstance(rev_no, float):
                rev_no = int(float(rev_no))  # Convert to float first, then to int
            
            # Format and typecast other fields
            formatted_data.append({
                "Date": date,
                "Time": time,
                "Freq": float(record.get("Freq", 0.0)),
                "RevNo": rev_no,
                "DemandMet": float(record.get("DemandMet", 0.0)),
                "DSMMet": float(record.get("DSMMet", 0.0)),
                "NB_NET_DWL": float(record.get("NB_NET_DWL", 0.0)),
                "SB_NET_DWL": float(record.get("SB_NET_DWL", 0.0)),
                "SB_NET_SCHD": float(record.get("SB_NET_SCHD", 0.0)),
                "NB_NET_SCHD": float(record.get("NB_NET_SCHD", 0.0)),
                "NB_DEMAND_MET": float(record.get("NB_DEMAND_MET", 0.0)),
                "SB_DEMAND_MET": float(record.get("SB_DEMAND_MET", 0.0)),
                "Scheduled": float(record.get("Scheduled", 0.0)),
                "Actual": float(record.get("Actual", 0.0)),
                "Deviation": float(record.get("Deviation", 0.0)),
                "UI": float(record.get("UI", 0.0)),
                "StateGeneration": float(record.get("StateGeneration", 0.0)),
                "StateRevNo": int(record.get("StateRevNo", 0)),
                "Remarks": int(record.get("Remarks", 0)),
                "ThermalGeneration": float(record.get("ThermalGeneration", 0.0)),
                "RAILWAY_DRAWL": float(record.get("RAILWAY_DRAWL", 0.0)),
            })
        except (ValueError, KeyError) as e:
            print(f"Error formatting record: {record}, error: {e}")
    return formatted_data

# Function to check if data has changed
def has_data_changed(new_data):
    # Fetch the latest record from MongoDB
    last_record = collection.find_one(sort=[("_id", -1)])  # Find the latest record
    if not last_record:
        return True  # If no record exists, always insert the new data

    # Compare the latest record with the new data
    for field in new_data:
        if field != "Time" and new_data[field] != last_record.get(field):
            return True  # Data has changed
    return False

# Function to save data to MongoDB if it doesn't already exist
def save_to_mongo(data):
    if data:
        for record in data:
            # Check if a record with the same Date and Time already exists
            existing_record = collection.find_one({"Date": record["Date"], "Time": record["Time"]})
            if not existing_record:
                try:
                    collection.insert_one(record)
                    print(f"Inserted record for Date: {record['Date']} and Time: {record['Time']} into MongoDB.")
                except Exception as e:
                    print(f"Error inserting data into MongoDB: {e}")
            else:
                print(f"Record already exists for Date: {record['Date']} and Time: {record['Time']}, skipping...")


# Function to fetch historical data
def fetch_historical_data(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%d-%B-%Y")  # Format: 01-January-2025
        print(f"Fetching historical data for {date_str}...")

        # Fetch data from the API
        data = fetch_scada_data(date_str)

        if data:
            formatted_data = format_data(data)
            save_to_mongo(formatted_data)
        else:
            print(f"No data available for {date_str}")

        # Move to the next day
        current_date += datetime.timedelta(days=1)

# Function to fetch real-time data every minute
def fetch_realtime_data():
    while True:
        today = datetime.date.today()
        date_str = today.strftime("%d-%B-%Y")  # Format: 01-January-2025
        print(f"Fetching real-time data for {date_str}...")

        # Fetch data from the API
        data = fetch_scada_data(date_str)

        # Format and save data if changes are detected
        if data:
            formatted_data = format_data(data)
            save_to_mongo(formatted_data)
        else:
            print(f"No data available for {date_str}")

        # Wait for 1 minute before fetching again
        time.sleep(60)

# Start the process
start_date = datetime.date(2025, 1, 1)
end_date = datetime.date.today()

# Fetch historical data first
fetch_historical_data(start_date, end_date)

# Start real-time data fetching
fetch_realtime_data()
