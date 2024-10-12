## Mini_Project5

Author: Fuyao Li

### Requirements:
+ Connect to a SQL database
+ Perform CRUD operations (create, read, update, and delete)

### Preparation:
+ Built virtual environment: `pip install -r requirements.txt`
+ Extract a dataset from a URL: `extract.py`
+ Loads the transformed data: `transform_load.py`, load the transformed data into a SQLite database table using Python's sqlite3 module.

### Sample CRUD Operations:
+ Create
    ``` python 
    create_query1 = ("6/27/2018", "Durham South", "Durham", "NC", 35.99, 78.90)
    create_subject(create_query1)
    ```
+ Read
    ``` python
    read_data()
    ```
+ Update
    ``` python 
    update_query = (107, "8/29/2018", "Durham South", "Durham", "NC", 35.99, 78.90)
    update_subject(update_query)
    ```
+ Delete
    ``` python
    delete_subject(99)
    ```
### Result:
All CRUD Operations are shown in `db_log.md`.

### References:
https://github.com/nogibjj/sqlite-lab
