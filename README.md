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
+ `query.py`: 
    - `join_sql()`: add a timezone column to the FL_citydb table and updates its values based on latitude ranges.
    - `agg_order_sql()`: use a CTE (Common Table Expression) to calculate average latitude and city counts by state, joins these results with FL_citydb, and returns the top 10 ordered by average latitude.
+ `transform_load.py`:
    - `load()`: read a CSV file, connects to a Databricks SQL instance, checks if the FL_citydb table exists, and drops it if necessary.

`main.py`: A script interacting with the SQLite database by creating tables and inserting user records.

`test_main.py`: Tests database operations to ensure correctness.

`Makefile`: Provides commands for managing the project.

`Dockerfile`: Ensures containerization.

### Preparation:
+ Built virtual environment: `pip install -r requirements.txt`

### Description of the Query：
```python

```


### References:
https://github.com/nogibjj/sqlite-lab
### Data resource:
https://github.com/fivethirtyeight/data/blob/master/presidential-campaign-trail/trump.csv