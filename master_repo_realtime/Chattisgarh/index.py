import threading
import requests
import mysql.connector
from bs4 import BeautifulSoup
from datetime import datetime
import time
import urllib3
import re
import os
import signal
import subprocess
import logging

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    filename="scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Graceful termination event for threads
terminate_event = threading.Event()

# Ensure script runs at a fixed interval using cron
def setup_cron_job():
    script_path = os.path.abspath(__file__)
    cron_job = f"*/2 * * * * /usr/bin/python3 {script_path} >> /tmp/scraper.log 2>&1"

    existing_cron = subprocess.getoutput("crontab -l")

    if cron_job not in existing_cron:
        new_cron = existing_cron + f"\n{cron_job}\n" if existing_cron else cron_job
        process = subprocess.Popen("crontab", stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.communicate(input=new_cron.encode())
        print("Cron job added successfully.")

# Kill the script after execution
def terminate_script():
    pid = os.getpid()
    print(f"Terminating script with PID: {pid}")
    os.kill(pid, signal.SIGTERM)

# MySQL Connection Setup
def get_db_connection():
    conn = mysql.connector.connect(
        host="localhost",
        user="jwergos_prakarsh",
        password="Kevlar@2001"
    )
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS jwergos_scraping_repository")
    cursor.close()
    conn.close()

    return mysql.connector.connect(
        host="localhost",
        user="jwergos_prakarsh",
        password="Kevlar@2001",
        database="jwergos_scraping_repository"
    )

def sanitize_column_name(name):
    name = re.sub(r"[^A-Za-z0-9_]+", "_", name).upper()  # Standardize column names
    
    # Ensure correct unit replacements without duplication
    if "KWB_" in name and "KWB_UNIT_" not in name:
        name = name.replace("KWB_", "KWB_UNIT_")
    if "DSPMTPS_" in name and "DSPMTPS_UNIT_" not in name:
        name = name.replace("DSPMTPS_", "DSPMTPS_UNIT_")
    if "BANGO_" in name and "BANGO_UNIT_" not in name and "BANGO_TOTAL" not in name:
        name = name.replace("BANGO_", "BANGO_UNIT_")
    if "MARWA_" in name and "MARWA_UNIT_" not in name and "MARWA_TPS_TOTAL" not in name:
        name = name.replace("MARWA_", "MARWA_UNIT_")
    
    # Prevent double replacement issues
    name = name.replace("MARWA_UNIT_TPS_TOTAL", "MARWA_TPS_TOTAL")
    
    # Ensure 'TOTAL' values are correctly renamed
    name = name.replace("BANGO_UNIT_HPS_TOTAL", "BANGO_TOTAL")  # Fix incorrect renaming
    name = name.replace("TOTAL_OF_CSPGCL_IPP_CPP", "CSPGCL_IPP_CPP_TOTAL")
    name = name.replace("TOTAL_OF_CSPGCL", "CSPGCL_TOTAL")
    
    return name.rstrip("_")  # Remove trailing underscores

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cg_system_summary (
            id INT AUTO_INCREMENT PRIMARY KEY,
            TIME VARCHAR(10),
            DATE DATE,
            NORMAL_RATE_OF_CHARGE_FOR_DEVIATION FLOAT,
            WR_FREQUENCY FLOAT,
            POWER_AVAILABILITY FLOAT,
            CG_DEMAND FLOAT,
            CG_SCHEDULE FLOAT,
            CG_DRAWL_FROM_CENTRAL_SECTOR FLOAT,
            CG_UNDER_DRAWL_OVER_DRAWL FLOAT,
            SOLAR_INJECTION_AT_132_LEVEL FLOAT,
            BIOMASS_INJECTION FLOAT,
            LOAD_SHEEDING_MW FLOAT,
            UNRESTRICTED_DEMAND FLOAT,
            inserted_at DATETIME,
            updated_at DATETIME
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS error_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            error_message TEXT,
            timestamp DATETIME
        )
    """)
    conn.commit()
    conn.close()


def scrape_cg_system_summary(table):
    try:
        system_summary = {}
        now = datetime.now()
        system_summary["TIME"] = now.strftime("%H:%M")
        system_summary["DATE"] = now.strftime("%Y-%m-%d")

        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) == 2:
                key = sanitize_column_name(cols[0].text.strip())
                value = float(cols[1].text.strip())
                system_summary[key] = value
        return system_summary
    except Exception as e:
        print(f"An error occurred while scraping system summary: {e}")
        return {}
    
def log_error(error_message):
    """Logs errors into the database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO error_log (error_message, timestamp)
            VALUES (%s, %s)
        """
        cursor.execute(query, (error_message, datetime.now()))
        conn.commit()
        conn.close()
    except Exception as db_error:
        print(f"Failed to log error: {db_error}")

def save_to_mysql(data, table_name):
    try:
        if not data:
            print(f"No data to insert into {table_name}")
            return
        
        conn = get_db_connection()
        cursor = conn.cursor()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        sanitized_data = {sanitize_column_name(k): v for k, v in data.items()}
        columns = ", ".join(sanitized_data.keys())
        values = tuple(sanitized_data.values())
        placeholders = ", ".join(["%s"] * len(sanitized_data))
        print(f"Inserting into {table_name}: {sanitized_data}")
        query = f"""
            INSERT INTO {table_name} ({columns}, inserted_at, updated_at)
            VALUES ({placeholders}, %s, %s)
        """
        cursor.execute(query, values + (now, now))
        conn.commit()
        conn.close()
        print("Data successfully inserted!")
    except Exception as e:
        print(f"An error occurred while saving to MySQL: {e}")


def scrape_summary_every_30_seconds(url):
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        system_summary_table = soup.find('div', class_='updatea').find('table', class_='table')
        if system_summary_table:
            system_summary_data = scrape_cg_system_summary(system_summary_table)
            save_to_mysql(system_summary_data, "cg_system_summary")
    except Exception as e:
        print(f"An error occurred during system summary scraping: {e}")

if __name__ == "__main__":
    url = "https://sldccg.com/gen.php"
    create_tables()

    summary_thread = threading.Thread(target=scrape_summary_every_30_seconds, args=(url,))
    
    summary_thread.start()

    try:
        summary_thread.join()
    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
        terminate_event.set()
