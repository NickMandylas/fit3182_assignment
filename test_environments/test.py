import pymongo
import matplotlib.pyplot as plt
from datetime import datetime
import folium

# Required for Jupyter inline display
# %matplotlib notebook

m = folium.Map(location=[-37.8409, 144.9464], zoom_start=8)

client = pymongo.MongoClient()
db = client.fit3182_assignment_db
collection = db.climate

# Collect after this date, as it has the cause data associated with hotspot.
result = collection.aggregate([
    {'$unwind': '$hotspots'},
    {'$match': {'hotspots.time': {'$gte': datetime(2019, 1, 1)}}}
])

for each in result:
    cause = each['hotspots']['cause']
    longitude = each['hotspots']['longitude']
    latitude = each['hotspots']['latitude']
    surface_temperature = each['hotspots']['surface_temperature_celcius']
    air_temperature = each['air_temperature_celcius']
    relative_humidity = each['relative_humidity']

    information = "<b>Cause:</b> %s</br><b>Surface Temperature:</b> %d</br><b>Air Temperature:</b> %d<b>Relative Humidity:</b> %d" % (
        cause, surface_temperature, air_temperature, relative_humidity)

    if cause == 'natural':
        folium.Marker(
            location=[latitude, longitude],
            popup=folium.Popup(html=information, max_width='100%'),
            icon=folium.Icon(color="blue", icon="info-sign"),
        ).add_to(m)
    else:
        folium.Marker(
            location=[latitude, longitude],
            popup=folium.Popup(html=information, max_width='100%'),
            icon=folium.Icon(color="red", icon="info-sign"),
        ).add_to(m)

m
