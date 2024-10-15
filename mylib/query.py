"""Query the database"""

from databricks import sql
from dotenv import load_dotenv
import os


def join_sql():
    """Joins a time zone column based on latitude using Databricks."""
    load_dotenv()
    databricks_key = os.getenv("DATABRICKS_KEY")
    server_host_name = os.getenv("SEVER_HOST_NAME")
    sql_http = os.getenv("SQL_HTTP")

    with sql.connect(
        access_token=databricks_key,
        server_hostname=server_host_name,
        http_path=sql_http,
    ) as connection:
        with connection.cursor() as cursor:
            # Add a new timezone column if it does not exist
            cursor.execute(
                "ALTER TABLE FL_citydb ADD COLUMN timezone STRING"
            )

            # Use latitude to determine timezone and update rows
            update_query = """
            UPDATE FL_citydb
            SET timezone = CASE
                WHEN lat BETWEEN 24.396308 AND 31.000968 THEN 'Eastern'
                WHEN lat BETWEEN 31.000969 AND 49.384358 THEN 'Central'
                ELSE 'Unknown'
            END
            """
            cursor.execute(update_query)
            result = cursor.fetchall()

        cursor.close()
        connection.close()
    return result


def agg_order_sql():
    load_dotenv()
    databricks_key = os.getenv("DATABRICKS_KEY")
    server_host_name = os.getenv("SEVER_HOST_NAME")
    sql_http = os.getenv("SQL_HTTP")

    with sql.connect(
        access_token=databricks_key,
        server_hostname=server_host_name,
        http_path=sql_http,
    ) as connection:
        with connection.cursor() as cursor:
            query = """
            WITH city_lat AS (
                SELECT 
                    state,
                    AVG(lat) AS avg_latitude,
                    COUNT(*) AS city_count
                FROM FL_citydb
                GROUP BY state
            )

            SELECT 
                FL_citydb.*,
                city_lat.avg_latitude,
                city_lat.city_count
            FROM 
                FL_citydb
            JOIN
                city_lat ON FL_citydb.state = city_lat.state
            ORDER BY 
                city_lat.avg_latitude ASC
            LIMIT 10;
            """
            cursor.execute(query)
            result = cursor.fetchall()

        cursor.close()
        connection.close()
    return result

if __name__ == "__main__":
    # result = join_sql()
    result = agg_order_sql()
    print(result)
