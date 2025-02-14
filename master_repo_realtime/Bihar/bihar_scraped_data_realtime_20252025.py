import requests
import datetime
import time
import mysql.connector
from mysql.connector import Error

# MySQL Configuration
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "Admin@123"
MYSQL_DB = "scraping_repository"
TABLE_NAME = "Bihar_yearly_data_2025"
ERROR_LOG_TABLE = "error_logs"

# Function to establish MySQL connection
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        return conn
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

# Function to create database and table dynamically
def setup_database():
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Date DATE,
            TimeBlock TIME,
            Freq FLOAT,
            RevNo INT,
            DemandMet FLOAT,
            DSMMet FLOAT,
            NB_NET_DWL FLOAT,
            SB_NET_DWL FLOAT,
            SB_NET_SCHD FLOAT,
            NB_NET_SCHD FLOAT,
            NB_DEMAND_MET FLOAT,
            SB_DEMAND_MET FLOAT,
            Scheduled FLOAT,
            Actual FLOAT,
            Deviation FLOAT,
            UI FLOAT,
            StateGeneration FLOAT,
            StateRevNo INT,
            Remarks TEXT,
            ThermalGeneration FLOAT,
            RAILWAY_DRAWL FLOAT,
            UNIQUE(Date, TimeBlock)
        )'''
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()

# Function to fetch data from API
def fetch_scada_data(date):
    url = f"https://sldc.bsptcl.co.in:8086/api/SCADA/Get/GetAllSCADADataDatewise?date={date}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {date}: {e}")
        return None

# Function to format and process data
def process_data(data, date):
    processed_data = []
    for record in data:
        try:
            raw_time = record.get("Time", "000000")  # Default to midnight if missing
            time_obj = datetime.datetime.strptime(raw_time, "%H%M%S").time()
            time_block = time_obj.strftime("%H:%M:%S")

            processed_data.append({
                "Date": date,
                "TimeBlock": time_block,
                "Freq": float(record.get("Freq", 0.0)),
                "RevNo": int(float(record.get("RevNo", 0.0))),
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
                "Remarks": record.get("Remarks", ""),
                "ThermalGeneration": float(record.get("ThermalGeneration", 0.0)),
                "RAILWAY_DRAWL": float(record.get("RAILWAY_DRAWL", 0.0))
            })
        except Exception as e:
            print(f"Error processing record: {record}, error: {e}")
    return processed_data

# Function to save data to MySQL
def save_to_mysql(data):
    if not data:
        return
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            insert_query = f'''
            INSERT INTO {TABLE_NAME} (
                Date, TimeBlock, Freq, RevNo, DemandMet, DSMMet,
                NB_NET_DWL, SB_NET_DWL, SB_NET_SCHD, NB_NET_SCHD, NB_DEMAND_MET,
                SB_DEMAND_MET, Scheduled, Actual, Deviation, UI, StateGeneration,
                StateRevNo, Remarks, ThermalGeneration, RAILWAY_DRAWL
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                Freq=VALUES(Freq), DemandMet=VALUES(DemandMet), Actual=VALUES(Actual)'''
            records = [(record["Date"], record["TimeBlock"], record["Freq"], record["RevNo"],
                        record["DemandMet"], record["DSMMet"], record["NB_NET_DWL"], record["SB_NET_DWL"],
                        record["SB_NET_SCHD"], record["NB_NET_SCHD"], record["NB_DEMAND_MET"], record["SB_DEMAND_MET"],
                        record["Scheduled"], record["Actual"], record["Deviation"], record["UI"],
                        record["StateGeneration"], record["StateRevNo"], record["Remarks"],
                        record["ThermalGeneration"], record["RAILWAY_DRAWL"]) for record in data]
            cursor.executemany(insert_query, records)
            conn.commit()
            cursor.close()
        except Error as e:
            print(f"Error inserting data into MySQL: {e}")
        finally:
            conn.close()

# Function to fetch and store historical data
def fetch_historical_data(start_date, end_date):
    setup_database()
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%d-%B-%Y")
        print(f"Fetching historical data for {date_str}...")
        data = fetch_scada_data(date_str)
        if data:
            processed_data = process_data(data, current_date)
            save_to_mysql(processed_data)
        current_date += datetime.timedelta(days=1)

fetch_historical_data(datetime.date(2025, 1, 1), datetime.date.today())