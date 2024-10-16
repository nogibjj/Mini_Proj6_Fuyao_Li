"""
Transforms and Loads data into the local SQLite3 database
"""
import os
from databricks import sql
from dotenv import load_dotenv
import pandas as pd


# load the csv file and insert into a new table
def load(dataset1="data/trump.csv"):
    """ "Transforms and Loads data into the local SQLite3 database"""
    df1 = pd.read_csv(dataset1, delimiter=",", skiprows=1)

    load_dotenv()
    databricks_key = os.getenv("DATABRICKS_KEY")
    sever_host_name = os.getenv("SERVER_HOST_NAME")
    sql_http = os.getenv("SQL_HTTP")
    with sql.connect(
        access_token=databricks_key,
        server_hostname=sever_host_name,
        http_path=sql_http,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES FROM default LIKE 'FL_citydb*'")
            result = cursor.fetchall()
            if result:
                cursor.execute("DROP TABLE fl_citydb")
            
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS FL_citydb (
                    date string,
                    location string,
                    city string,
                    state string,
                    lat float,
                    lng float
                )
            """
            )
            # insert
            values_list = [tuple(row) for _, row in df1.iterrows()]
            insert_query = (
                f"INSERT INTO FL_citydb VALUES {','.join(str(x) for x in values_list)}"
            )
            cursor.execute(insert_query)
            result = cursor.fetchall()

            cursor.close()
            connection.close()
        
    return result
