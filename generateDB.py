import os
import re
import csv
import sqlite3

# === Config ===
CSV_DIR = os.path.join(os.path.dirname(__file__), "csv")
DB_FILE = os.path.join(os.path.dirname(__file__), "csv_data.db")

# === Connect to SQLite ===
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# === Drop all existing tables ===
cursor.execute("PRAGMA foreign_keys = OFF;")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
for (table,) in tables:
    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")

# === Process all CSV files ===
for file in os.listdir(CSV_DIR):
    if not file.endswith(".csv"):
        continue

    table_name = os.path.splitext(file)[0]
    path = os.path.join(CSV_DIR, file)

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)

        # Sanitize headers
        columns = [
            re.sub(r"[^a-zA-Z0-9_]", "_", h).lower()
            for h in headers
        ]

        # Create table
        col_defs = ", ".join([f"`{c}` TEXT" for c in columns])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table_name}` ({col_defs})")

        # Insert rows
        placeholders = ", ".join(["?"] * len(columns))
        insert_sql = f"INSERT INTO `{table_name}` ({', '.join('`'+c+'`' for c in columns)}) VALUES ({placeholders})"
        row_count = 0
        for row in reader:
            cursor.execute(insert_sql, row)
            row_count += 1

        conn.commit()
        print(f"Loaded: {table_name} ({len(columns)} columns, {row_count} rows)")

print("\nAll CSVs loaded into SQLite.")
conn.close()
