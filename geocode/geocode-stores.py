import csv
from time import sleep
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Read in Dataframe containing TDLinx Number and Address for all Stores
df = pd.read_csv('vipsam_brand_stores_geocode.csv')
df = df[["tdlinx_number", "store_address"]]

# Geocode addresses
geolocator = Nominatim()

with open('geocoded_address.csv','w') as f1:
    writer=csv.writer(f1, delimiter=',',lineterminator='\n',)
    writer.writerow(['tdlinx_number', 'address', 'latitude', 'longitude'])
    for row in df['store_address']:
        try:
            lat = geolocator.geocode(row, timeout = 60).latitude
        except:
            lat = 0
        sleep(1)
        try:
            lon = geolocator.geocode(row, timeout = 60).longitude
        except:
            lon = 0
        sleep(1)
        writer.writerow([row, lat, lon])
        print(row, lat, lon)