import time
import mysql.connector
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime

# 🔹 MySQL Database Connection Function
def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="jwergos_prakarsh",         # Replace with your MySQL username
        password="Kevlar@2001", # Replace with your MySQL password
    )

# 🔹 Create Database and Table Dynamically
def setup_database():
    db = connect_db()
    cursor = db.cursor()
    
    cursor.execute("CREATE DATABASE IF NOT EXISTS scraping_repository")
    cursor.execute("USE jwergos_scraping_repository")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rajisthan_realtime_overview (
            id INT AUTO_INCREMENT PRIMARY KEY,
            inserted_at DATETIME,
            Date DATE,
            Time TIME
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS error_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            error_message TEXT,
            logged_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    db.commit()
    cursor.close()
    db.close()

# 🔹 Function to Sanitize Column Names
def sanitize_column_name(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

# 🔹 Function to Log Errors in MySQL
def log_error(error_message):
    db = mysql.connector.connect(
        host="localhost",
        user="jwergos_prakarsh",
        password="Kevlar@2001",
        database="scraping_repository"
    )
    cursor = db.cursor()
    
    cursor.execute("""
        INSERT INTO error_logs (error_message) VALUES (%s)
    """, (error_message,))
    
    db.commit()
    cursor.close()
    db.close()
    print(f"❌ Logged error: {error_message}")

# 🔹 Scrape and Store Data in MySQL
def scrape_and_store():
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

        # Wait for the table to load
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#overview_data tbody tr")))

        # Locate the table
        table = driver.find_element(By.ID, "overview_data")
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")

        # Extract data
        inserted_at = datetime.now().strftime("%Y-%m-%d %H:%M")
        data = {
            "inserted_at": inserted_at,
            "Date": "",
            "Time": "",
        }

        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) == 3:  # Ensure correct number of columns
                scada_name = sanitize_column_name(cols[0].text.strip().upper().replace(" ", "_"))  # Sanitize column name
                value = float(cols[1].text.strip())
                datetime_str = cols[2].text.strip()

                # Parse datetime into separate Date and Time fields
                dt_object = datetime.strptime(datetime_str, "%d %b %Y %H:%M:%S")
                data["Date"] = dt_object.strftime("%Y-%m-%d")
                data["Time"] = dt_object.strftime("%H:%M")

                # Store value in the corresponding field
                data[scada_name] = value

        # Store Data in MySQL
        insert_data(data)

    except Exception as e:
        log_error(str(e))

    finally:
        driver.quit()

# 🔹 Insert Data into MySQL
def insert_data(data):
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Admin@123",
        database="scraping_repository"
    )
    cursor = db.cursor()
    
    # Ensure all columns exist in the table
    cursor.execute("SHOW COLUMNS FROM rajisthan_realtime_overview")
    existing_columns = [column[0] for column in cursor.fetchall()]
    
    for key in data.keys():
        sanitized_key = sanitize_column_name(key)
        if sanitized_key not in existing_columns:
            cursor.execute(f"ALTER TABLE rajisthan_realtime_overview ADD COLUMN {sanitized_key} FLOAT NULL")
    
    db.commit()
    
    try:
        columns = ", ".join([sanitize_column_name(col) for col in data.keys()])
        placeholders = ", ".join(["%s"] * len(data))
        sql_query = f"INSERT INTO rajisthan_realtime_overview ({columns}) VALUES ({placeholders})"
        sql_values = tuple(data.values())
        
        cursor.execute(sql_query, sql_values)
        db.commit()
        print("✅ Data inserted successfully into MySQL")
    
    except mysql.connector.Error as e:
        log_error(f"MySQL Error: {e}")
        print(f"❌ MySQL Error: {e}")
    
    finally:
        cursor.close()
        db.close()

# 🔹 Run the function every 1 minute
if __name__ == "__main__":
    setup_database()
    
    scrape_and_store()
        