## Mini_Project6

Author: Fuyao Li

### Requirements:
+ Design a complex SQL query involving joins, aggregation, and sorting
+ Provide an explanation for what the query is doing and the expected results

### Project Structure:
`.devcontainer/`: Contains files for development container setup.

`.github/workflows/`: Includes CI/CD automation via GitHub Actions.
+ install: install requirements.txt
+ test: runs pytest for functions; runs all test files matching the pattern test_*.py
+ format: format using black formatter
+ lint: using Ruff for testing, which makes the process faster

`mylib`:
+ `extract.py`: extract the dataset from URL and save the contents to the specified file path.
+ `transform_load.py`:
    - `load()`: read a CSV file, connect to a Databricks SQL instance, check if the FL_citydb table exists, and drop it if necessary.
+ `query.py`: 
    - `join_sql()`: add a timezone column to the FL_citydb table and updates its values based on latitude ranges.
    - `agg_order_sql()`: use a CTE (Common Table Expression) to calculate average latitude and city counts by state, joins these results with FL_citydb, and returns the top 10 ordered by average latitude.

`main.py`: A script interacting with the SQLite database by creating tables and inserting user records.

`test_main.py`: Tests database operations to ensure correctness.

`Makefile`: Provides commands for managing the project.

`Dockerfile`: Ensures containerization.

### Preparation:
+ Built virtual environment: `pip install -r requirements.txt`

### Description of the Query：
1. Load .csv file to connect to a Databricks SQL instance
> This step was excuted in `transform_load.py`.
```python
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
```
+ Use three keys to connect to Databricks: `DATABRICKS_KEY`, `SERVER_HOST_NAME`, `SQL_HTTP`.
+ Create `FL_citydb` table which has 6 columns: 
    - date,, location, city, state, lat, lng.
+ Inserting Data:
    - A list of tuples (values_list) is created by iterating through the rows of the CSV (df1).
    - The INSERT INTO FL_citydb VALUES query dynamically inserts these values into the table in one batch, matching the order of the columns.

2. Update the table
> This step was excuted in `query.py`.
```python
def join_sql():
    """Joins a time zone column based on latitude using Databricks."""
    load_dotenv()
    databricks_key = os.getenv("DATABRICKS_KEY")
    server_host_name = os.getenv("SERVER_HOST_NAME")
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

```

+ Use three keys to connect to Databricks: `DATABRICKS_KEY`, `SERVER_HOST_NAME`, `SQL_HTTP`.
+ The UPDATE query modifies the timezone column in the FL_citydb table based on the latitude (lat) value:
    - Eastern timezone for latitudes between 24.396308 and 31.000968.
    - Central timezone for latitudes between 31.000969 and 49.384358.
    - Any other latitude is set to Unknown.

3. A complex SQL query involving joins, aggregation, and sorting
> This step was excuted in `query.py`.

```python
def agg_order_sql():
    load_dotenv()
    databricks_key = os.getenv("DATABRICKS_KEY")
    server_host_name = os.getenv("SERVER_HOST_NAME")
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
```

+ Use three keys to connect to Databricks: `DATABRICKS_KEY`, `SERVER_HOST_NAME`, `SQL_HTTP`.
+ Create a Common Table Expression (CTE) city_lat that calculates the average latitude (avg_latitude) and the number of cities (city_count) grouped by state.
+ Join the city_lat CTE with the FL_citydb table on the state column.
+ Select all columns from FL_citydb, along with the calculated avg_latitude and city_count.
+ Order the results by avg_latitude in ascending order and limits the output to the top 10 results.


### References:
https://github.com/nogibjj/sqlite-lab
### Data resource:
https://github.com/fivethirtyeight/data/blob/master/presidential-campaign-trail/trump.csv