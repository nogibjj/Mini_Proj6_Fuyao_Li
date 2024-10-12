"""
Extract a dataset from a URL like Kaggle or data.gov. 
JSON or CSV formats tend to work well
"""
import requests


def extract(
        url="https://github.com/fivethirtyeight/data/blob/master/presidential-campaign-trail/trump.csv?raw=true", 
        file_path="data/trump.csv"
    ):
    """"Extract a url to a file path"""
    response = requests.get(url)

    with open(file_path, "wb") as f:
        f.write(response.content)
    print(f"File successfully downloaded to {file_path}")

    return file_path
