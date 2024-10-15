"""
Extract a dataset from a URL like Kaggle or data.gov. 
JSON or CSV formats tend to work well
"""
import requests
import os


def extract(
        url1="https://github.com/fivethirtyeight/data/blob/master/presidential-campaign-trail/trump.csv?raw=true", 
        file_path1="data/trump.csv",
    ):
    """Extract urls to a file path"""
    os.makedirs(os.path.dirname(file_path1), exist_ok=True)

    response1 = requests.get(url1)
    if response1.status_code == 200:
        with open(file_path1, "wb") as f:
            f.write(response1.content)
        print(f"File successfully downloaded to {file_path1}")
    else:
        print(
            f"Failed to retrieve the file from {url1}."
        )

    return file_path1


if __name__ == "__main__":
    extract()