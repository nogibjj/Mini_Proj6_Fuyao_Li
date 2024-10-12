"""
Transforms and Loads data into the local SQLite3 database
"""
import sqlite3
import csv


# load the csv file and insert into a new sqlite3 database
def load(dataset="data/trump.csv"):
    """ "Transforms and Loads data into the local SQLite3 database"""
    with open(dataset, newline="", encoding="utf-8") as file:
        payload = csv.reader(file, delimiter=",")
        # skips the header of csv
        next(payload)
        conn = sqlite3.connect("CityDB.db")
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS CityDB")
        c.execute(
            """
            CREATE TABLE CityDB (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                location TEXT,
                city TEXT,
                state TEXT,
                lat FLOAT,
                lng FLOAT
            )
        """
        )
    
        # insert
        c.executemany(
            """
            INSERT INTO CityDB 
            (date, location, city, state, lat, lng) 
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        conn.commit()
        conn.close()
        
    return "CityDB.db"