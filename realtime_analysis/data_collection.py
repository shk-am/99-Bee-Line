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
URL = "https://gtfsapi.translink.ca/v3/gtfsposition?apikey=" + API_KEY

def fetch_feed():
    feed_message = gtfs.FeedMessage()

    with urllib.request.urlopen(URL) as response:
        feed_message.ParseFromString(response.read())

    json_feed = json_format.MessageToDict(feed_message)
    return json_feed

def flatten_and_parse_feed(json_feed):
    location_dict = []
    for item in json_feed['entity']:
        del item['id']
        del item['vehicle']['vehicle']['label']
        location_dict.append(item['vehicle'])

    flattened_feed = pd.json_normalize(location_dict)
    return flattened_feed

def save_feed_stamped(flattened):
    date = datetime.today().strftime('%Y-%m-%d')
    current_time = datetime.today().strftime('%H-%M-%S')
    
    data_location = 'location_pulled_data/' + date
    os.makedirs(data_location, exist_ok = True)
    
    file_path = data_location + '/location_' + current_time + '.csv'
    flattened.to_csv(file_path)
    
# min_hr inclusive, max_hr inclusive
def hour_within(min_hr, max_hr):
    current_hour = int(datetime.today().strftime('%H'))
    return current_hour >= min_hr and current_hour <= max_hr

# Maximum pull requests: 1000/day; 4 hours * 60 minutes * 3 pulls/min = 720 pulls/day
# File size: 33 kb/pull * 720 pulls/day * 7 days = 166 mb
if __name__ == "__main__":
    while True:
        json_feed = fetch_feed()
        flattened_feed = flatten_and_parse_feed(json_feed)
        if hour_within(8, 12):    
            save_feed_stamped(flattened_feed)
        time.sleep(20)