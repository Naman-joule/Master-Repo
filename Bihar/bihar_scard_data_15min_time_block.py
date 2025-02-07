import requests
import datetime
import time  # For adding delays
from pymongo import MongoClient

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "scraping repository"
COLLECTION_NAME = "yearly_data_15Min_Block_2025"

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

# Function to log if data is not present
def log_missing_data(date):
    print(f"Data is not present for the date: {date}")

# Function to format data to match the required schema and group by time blocks
def format_and_group_data(data):
    time_blocks = {}  # Dictionary to hold grouped records by time block
    for record in data:
        try:
            # Correctly parse and reformat the Date and Time fields
            raw_date = record["Date"]
            day = raw_date[:2]  # Extract day (DD)
            month = raw_date[2:4]  # Extract month (MM)
            year = "20" + raw_date[4:6]  # Extract year (YY) and prefix with '20'
            date = f"{year}-{month}-{day}"  # Reformat to YYYY-MM-DD

            # Parse and format the Time field to determine the time block
            time_str = datetime.datetime.strptime(record["Time"], "%H%M%S").strftime("%H:%M")
            hour, minute = map(int, time_str.split(":"))
            
            # Determine the time block (15-minute intervals starting from 00:01)
            block_offset = (minute - 1) // 15  # Adjust starting point to begin at 00:01
            if minute == 0:
                block_offset = -1  # Handle edge case for "00:00"

            # Calculate TimeBlock
            time_block_minute = ((block_offset + 1) * 15 + 1) % 60  # Wrap minutes after 60
            time_block_hour = (hour + ((block_offset + 1) * 15 + 1) // 60) % 24  # Wrap hours after 24
            time_block_start = f"{time_block_hour:02d}:{time_block_minute:02d}"

            # Calculate BlockNo
            block_no = (hour * 4) + block_offset + 2  # BlockNo starts from 2 due to 00:01 adjustment
            
            # Ensure RevNo is properly cast to an integer
            rev_no = record.get("RevNo", 0)
            if isinstance(rev_no, str) or isinstance(rev_no, float):
                rev_no = int(float(rev_no))  # Convert to float first, then to int

            # Calculate average for each field within each time block
            if time_block_start not in time_blocks:
                time_blocks[time_block_start] = {
                    "Date": date,
                    "TimeBlock": time_block_start,
                    "BlockNo": block_no,
                    "Freq": [],
                    "RevNo": [],
                    "DemandMet": [],
                    "DSMMet": [],
                    "NB_NET_DWL": [],
                    "SB_NET_DWL": [],
                    "SB_NET_SCHD": [],
                    "NB_NET_SCHD": [],
                    "NB_DEMAND_MET": [],
                    "SB_DEMAND_MET": [],
                    "Scheduled": [],
                    "Actual": [],
                    "Deviation": [],
                    "UI": [],
                    "StateGeneration": [],
                    "StateRevNo": [],
                    "Remarks": [],
                    "ThermalGeneration": [],
                    "RAILWAY_DRAWL": []
                }
            
            # Append the data for the time block
            time_blocks[time_block_start]["Freq"].append(float(record.get("Freq", 0.0)))
            time_blocks[time_block_start]["RevNo"].append(int(float(record.get("RevNo", 0.0))))  # Convert RevNo to int
            time_blocks[time_block_start]["DemandMet"].append(float(record.get("DemandMet", 0.0)))
            time_blocks[time_block_start]["DSMMet"].append(float(record.get("DSMMet", 0.0)))
            time_blocks[time_block_start]["NB_NET_DWL"].append(float(record.get("NB_NET_DWL", 0.0)))
            time_blocks[time_block_start]["SB_NET_DWL"].append(float(record.get("SB_NET_DWL", 0.0)))
            time_blocks[time_block_start]["SB_NET_SCHD"].append(float(record.get("SB_NET_SCHD", 0.0)))
            time_blocks[time_block_start]["NB_NET_SCHD"].append(float(record.get("NB_NET_SCHD", 0.0)))
            time_blocks[time_block_start]["NB_DEMAND_MET"].append(float(record.get("NB_DEMAND_MET", 0.0)))
            time_blocks[time_block_start]["SB_DEMAND_MET"].append(float(record.get("SB_DEMAND_MET", 0.0)))
            time_blocks[time_block_start]["Scheduled"].append(float(record.get("Scheduled", 0.0)))
            time_blocks[time_block_start]["Actual"].append(float(record.get("Actual", 0.0)))
            time_blocks[time_block_start]["Deviation"].append(float(record.get("Deviation", 0.0)))
            time_blocks[time_block_start]["UI"].append(float(record.get("UI", 0.0)))
            time_blocks[time_block_start]["StateGeneration"].append(float(record.get("StateGeneration", 0.0)))
            time_blocks[time_block_start]["StateRevNo"].append(int(record.get("StateRevNo", 0)))  # Convert to int
            time_blocks[time_block_start]["Remarks"].append(int(record.get("Remarks", 0)))  # Convert to int
            time_blocks[time_block_start]["ThermalGeneration"].append(float(record.get("ThermalGeneration", 0.0)))
            time_blocks[time_block_start]["RAILWAY_DRAWL"].append(float(record.get("RAILWAY_DRAWL", 0.0)))

        except (ValueError, KeyError) as e:
            print(f"Error formatting record: {record}, error: {e}")
    
    # Calculate average for each time block
    for block, values in time_blocks.items():
        for field, field_values in values.items():
            if isinstance(field_values, list) and field_values:  # Ensure it's a non-empty list
                values[field] = sum(field_values) / len(field_values)
            elif isinstance(field_values, list):  # Empty list
                values[field] = 0.0

    return list(time_blocks.values())



# Function to save data to MongoDB
def save_to_mongo(data):
    if data:
        try:
            for record in data:
                # Check if the record already exists based on Date and TimeBlock
                existing_record = collection.find_one({"Date": record["Date"], "TimeBlock": record["TimeBlock"]})
                if not existing_record:
                    # Insert the new record if it doesn't exist
                    collection.insert_one(record)
                    print(f"Inserted record for {record['Date']} - {record['TimeBlock']}")
                else:
                    print(f"Record already exists for {record['Date']} - {record['TimeBlock']}, skipping...")
        except Exception as e:
            print(f"Error inserting data into MongoDB: {e}")


# Function to fetch historical and real-time data
def fetch_historical_and_realtime_data(start_date):
    current_date = start_date
    while True:  # Infinite loop for continuous fetching
        today = datetime.date.today()
        date_str = current_date.strftime("%d-%B-%Y")  # Format: 01-January-2025
        print(f"Fetching data for {date_str}...")

        # Fetch data from the API
        data = fetch_scada_data(date_str)

        if data:
            formatted_data = format_and_group_data(data)
            save_to_mongo(formatted_data)
        else:
            log_missing_data(date_str)

        # Move to the next day if processing historical data
        if current_date < today:
            current_date += datetime.timedelta(days=1)
        else:
            # Wait 15 minutes for real-time updates
            print("Waiting for the next 15-minute cycle...")
            time.sleep(900)

# Start fetching data from 01-January-2025
start_date = datetime.date(2025, 1, 1)
fetch_historical_and_realtime_data(start_date)
