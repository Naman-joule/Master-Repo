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
TABLE_NAME = "Bihar_yearly_data_15Min_Block_2025"
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
            BlockNo INT,
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
            UNIQUE(Date, BlockNo)
        )'''
        cursor.execute(create_table_query)

        create_error_log_table = f'''
        CREATE TABLE IF NOT EXISTS {ERROR_LOG_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            error_message TEXT
        )'''
        cursor.execute(create_error_log_table)
        
        conn.commit()
        cursor.close()
        conn.close()

# Function to log errors
def log_error(error_message):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(f"INSERT INTO {ERROR_LOG_TABLE} (error_message) VALUES (%s)", (error_message,))
            conn.commit()
            cursor.close()
        except Error as e:
            print(f"Error logging to MySQL: {e}")
        finally:
            conn.close()

# Function to fetch data from API
def fetch_scada_data(date):
    url = f"https://sldc.bsptcl.co.in:8086/api/SCADA/Get/GetAllSCADADataDatewise?date={date}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        log_error(f"Error fetching data for {date}: {e}")
        return None

# Function to ensure all 96 time blocks are covered
def format_and_group_data(data, date):
    formatted_data = []
    
    for record in data:
        try:
            time_str = record["Time"]
            time_obj = datetime.datetime.strptime(time_str, "%H%M%S").time()
            block_no = (time_obj.hour * 4) + (time_obj.minute // 15) + 1
            
            formatted_data.append((
                date, time_obj.strftime("%H:%M:%S"), block_no,
                float(record.get("Freq", 0.0)),
                int(float(record.get("RevNo", 0.0))),
                float(record.get("DemandMet", 0.0)),
                float(record.get("DSMMet", 0.0)),
                float(record.get("NB_NET_DWL", 0.0)),
                float(record.get("SB_NET_DWL", 0.0)),
                float(record.get("SB_NET_SCHD", 0.0)),
                float(record.get("NB_NET_SCHD", 0.0)),
                float(record.get("NB_DEMAND_MET", 0.0)),
                float(record.get("SB_DEMAND_MET", 0.0)),
                float(record.get("Scheduled", 0.0)),
                float(record.get("Actual", 0.0)),
                float(record.get("Deviation", 0.0)),
                float(record.get("UI", 0.0)),
                float(record.get("StateGeneration", 0.0)),
                int(record.get("StateRevNo", 0)),
                record.get("Remarks", ""),
                float(record.get("ThermalGeneration", 0.0)),
                float(record.get("RAILWAY_DRAWL", 0.0))
            ))
        except Exception as e:
            log_error(f"Error processing record {record}: {e}")
    
    return formatted_data

# Function to save data to MySQL
def save_to_mysql(data):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            insert_query = f'''
            INSERT INTO {TABLE_NAME} (
                Date, TimeBlock, BlockNo, Freq, RevNo, DemandMet, DSMMet,
                NB_NET_DWL, SB_NET_DWL, SB_NET_SCHD, NB_NET_SCHD, NB_DEMAND_MET,
                SB_DEMAND_MET, Scheduled, Actual, Deviation, UI, StateGeneration,
                StateRevNo, Remarks, ThermalGeneration, RAILWAY_DRAWL
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE Freq=VALUES(Freq), DemandMet=VALUES(DemandMet), Actual=VALUES(Actual)'''
            cursor.executemany(insert_query, data)
            conn.commit()
            cursor.close()
        except Error as e:
            log_error(f"Error inserting data into MySQL: {e}")
        finally:
            conn.close()

# Fetch and store historical and real-time data
def fetch_historical_and_realtime_data():
    setup_database()
    start_date = datetime.date(2025, 1, 1)
    current_date = datetime.date.today()
    while start_date < current_date:
        date_str = start_date.strftime("%d-%B-%Y")
        print(f"Fetching data for {date_str}...")
        data = fetch_scada_data(date_str)
        if data:
            formatted_data = format_and_group_data(data, start_date)
            save_to_mysql(formatted_data)
        start_date += datetime.timedelta(days=1)
    while True:
        date_str = current_date.strftime("%d-%B-%Y")
        print(f"Fetching real-time data for {date_str}...")
        data = fetch_scada_data(date_str)
        if data:
            formatted_data = format_and_group_data(data, current_date)
            save_to_mysql(formatted_data)
        time.sleep(900)

fetch_historical_and_realtime_data()
