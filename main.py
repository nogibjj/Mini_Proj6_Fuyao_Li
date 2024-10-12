"""
ETL-Query script
"""
from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import (
    update_subject,
    delete_subject,
    create_subject,
    read_data,
)


if __name__ == "__main__":
    url = "https://github.com/fivethirtyeight/data/blob/master/presidential-campaign-trail/trump.csv?raw=true"
    file_path = "data/trump.csv"
    database = 'CityDB.db'

    print("Extact the database:")
    extract(url, file_path)

    print("Load data")
    load(file_path)

    data = read_data()
    print(len(data))

    create_query1 = ("6/27/2018", "Durham South", "Durham", "NC", 35.99, 78.90)
    create_subject(create_query1)
    
    create_query2 = ("10/22/2018", "Charlotte North", "Charlotte", "NC", 35.22, 80.84)
    create_subject(create_query2)

    update_query = (107, "8/29/2018", "Durham South", "Durham", "NC", 35.99, 78.90)
    update_subject(update_query)

    delete_subject(99)

    data = read_data()
    print(len(data))
    