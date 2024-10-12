"""Query the database"""

import sqlite3


# Define a file to show database operations
LOG_FILE = "db_log.md"


def add_operation(query):
    """adds to a query markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"```sql\n{query}\n```\n\n")


def read_data():
    """read data"""
    conn = sqlite3.connect("CityDB.db")
    c = conn.cursor()
    c.execute("SELECT * FROM CityDB")
    data = c.fetchall()
    # print(data)
    add_operation("SELECT * FROM CityDB")
    return data


def create_subject(query):
    """create example query"""
    date, location, city, state, lat, lng = query
    conn = sqlite3.connect("CityDB.db")
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO CityDB (date, location, city, state, lat, lng) 
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (date, location, city, state, lat, lng),
    )
    conn.commit()
    c.close()
    conn.close()

    # Log the query
    add_operation(
        f"""INSERT INTO ServeTimesDB VALUES 
        ({date}, {location}, {city}, {state}, {lat}, {lng});"""
    )
    return True


def update_subject(query):
    """update example query"""
    record_id, date, location, city, state, lat, lng = query
    conn = sqlite3.connect("CityDB.db")
    c = conn.cursor()
    c.execute(
        """
        UPDATE CityDB SET date=?, location=?, city=?, 
        state=?, lat=?, lng=? WHERE id=?
        """,
        (date, location, city, state, lat, lng, record_id),
    )
    conn.commit()
    c.close()
    conn.close()

    # Log the query
    add_operation(
        f"""UPDATE ServeTimesDB SET 
        date={date}, location={location},
        city={city}, state={state}, 
        lat={lat}, lng={lng}
        WHERE id={record_id};"""
    )
    return True


def delete_subject(record_id):
    """delete example query"""
    conn = sqlite3.connect("CityDB.db")
    c = conn.cursor()
    c.execute("DELETE FROM CityDB WHERE id=?", (record_id,))
    conn.commit()
    c.close()
    conn.close()

    # Log the query
    add_operation(f"DELETE FROM CityDB WHERE id={record_id};")
    return True
