"""
ETL-Query script
"""
from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import (
    join_sql,
    agg_order_sql,
)


if __name__ == "__main__":
    url1 = "https://github.com/fivethirtyeight/data/blob/master/presidential-campaign-trail/trump.csv?raw=true"
    file_path1 = "data/trump.csv"

    print("Extact the database:")
    extract(url1, file_path1)

    load(file_path1)
    
    result = join_sql()
    print(result)

    result = agg_order_sql()
    print(result)
    