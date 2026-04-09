import urllib.request
import gtfs_realtime_pb2 as gtfs
import pandas as pd
import dotenv
import os
import time

from google.protobuf import json_format
from datetime import datetime

dotenv.load_dotenv('.env')
API_KEY = os.environ["API_KEY"]

url = "https://gtfsapi.translink.ca/v3/gtfsposition?apikey=" + API_KEY

def fetch_feed():
    feed_message = gtfs.FeedMessage()

    with urllib.request.urlopen(url) as response:
        feed_message.ParseFromString(response.read())

    json_dict = json_format.MessageToDict(feed_message)
    return json_dict

def flatten_feed(json_dict):
    location_dict = []
    for item in json_dict['entity']:
        del item["id"]
        del item['vehicle']['vehicle']['label']
        location_dict.append(item['vehicle'])

    flattened = pd.json_normalize(location_dict)
    return flattened

def save_feed_stamped(flattened):
    date = datetime.today().strftime('%Y-%m-%d')
    current_time = datetime.today().strftime('%H-%M-%S')
    path = 'location_pulled_data/'+ date
    os.makedirs(path, exist_ok = True)
    flattened.to_csv('location_pulled_data/' + date + '/location_' + current_time + '.csv')
    
def check_time(minimum, maximum):
    current_hour = int(datetime.today().strftime('%H'))
    return current_hour >= minimum and current_hour <= maximum

# Maximum pull requests: 1000/day; 4 hours * 60 minutes * 3 pulls/min = 720 pulls/day
# File size: 33 kb/pull * 720 pulls/day * 7 days = 166 mb
if __name__ == "__main__":
    while True:
        json_feed = fetch_feed()
        flattened = flatten_feed(json_feed)
        if check_time(8, 12):    
            save_feed_stamped(flattened)
        time.sleep(20)