import requests
import os
from dotenv import load_dotenv

load_dotenv()

URL = os.getenv("DOWNLOAD_URL")

def download_csv():
    response = requests.get(URL)
    if response.status_code == 200:
        with open("./data/border_crossing_data.csv","wb") as file:
            file.write(response.content)
        print("CSV Downloaded Successfully!")
    else:
        raise Exception("Failed to Download CSV File.")

download_csv()