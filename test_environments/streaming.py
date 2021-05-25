import datetime
import os
import json
import time
import pprint
import pygeohash as pgh
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark import SparkContext, SparkConf
from pymongo import MongoClient

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 pyspark-shell'


def geohash_handler(latitude, longitude):
    return pgh.encode(latitude, longitude, precision=3)


def hotspots_handler(hotspots_aqua, hotspots_terra):
    # If there are no hotspots from either satellite, return empty array.
    if len(hotspots_aqua) == 0 and len(hotspots_terra) == 0:
        return []
    # If there are only hotspots from a single satellite, then there is opportunity for a location
    elif len(hotspots_aqua) > 0 and len(hotspots_terra) == 0:
        # to be close to another satellite, so return only the statellite that containes data.
        return hotspots_aqua
    elif len(hotspots_aqua) == 0 and len(hotspots_terra) > 0:
        return hotspots_terra
    else:
        hotspots = []

        for aqua in hotspots_aqua:
            count = 0

            # Loop through both arrays, to see if there's a location that is close by.
            # If an aqua satellite location is close to a terra satellite location, take the average for confidence and surface_temp,
            # I then append the new hotspot data to the hotspots final array, then pop the terra satellite data from the hotspots_terra array,
            # So there's no duplicate matches in the future. (Task was only to match TWO satellite locations).
            while count < len(hotspots_terra):
                terra = hotspots_terra[count]
                if aqua['geo_hash'] == terra['geo_hash']:
                    avg_hotspot = aqua
                    avg_hotspot['confidence'] = (
                        aqua['confidence'] + terra['confidence']) / 2
                    avg_hotspot['surface_temperature_celcius'] = (
                        aqua['surface_temperature_celcius'] + terra['surface_temperature_celcius']) / 2
                    hotspots_terra.pop(count)
                    hotspots.append(avg_hotspot)
                    break
                else:
                    # If no close satellites, append aqua to final array.
                    hotspots.append(aqua)
                count += 1

        # If there are terra satellites that haven't popped, we append them to the final array.
        if len(hotspots_terra) > 0:
            for terra in hotspots_terra:
                hotspots.append(terra)

        return hotspots


def climate_handler(climate, hotspots):
    # If there was no climate, return empty dictionary.
    if len(hotspots) > 0 and climate != {}:
        for hotspot in hotspots:
            # Check if climate & hotspot are close.
            if climate['geo_hash'] == hotspot['geo_hash']:
                # Check if natural or other
                if climate['air_temperature_celcius'] > 20 and climate['ghi'] > 180:
                    hotspot['cause'] = 'natural'
                else:
                    hotspot['cause'] = 'other'

                if 'hotspots' in climate:  # Append hotspot
                    climate['hotspots'].append(hotspot)
                else:
                    climate['hotspots'] = [hotspot]

    climate['station'] = 948700  # Station number required for DB data model.

    return climate


def stream_handler(iter):

    hotspots_aqua = []
    hotspots_terra = []
    climate = {}

    for each in iter:  # For each item from the data batch.
        # Deserialise the data from the Kafka stream.
        data = json.loads(each[1])
        # Calculate and set the geo-hash.
        data['geo_hash'] = geohash_handler(data['latitude'], data['longitude'])
        producer_id = data['producer_id']

        # Sort data depending on the producer_id (i.e. where it came from.)
        if producer_id == 'producer_climate':
            climate = data
        elif producer_id == 'producer_hotspot_aqua':
            hotspots_aqua.append(data)
        elif producer_id == 'producer_hotspot_terra':
            hotspots_terra.append(data)

    # Analyse hotspots data, find if any are close by & merge.
    hotspots = hotspots_handler(hotspots_aqua, hotspots_terra)
    # Merge hotspots with climate (depending if close & label if natural or other)
    climate = climate_handler(climate, hotspots)

    return climate


def prepareForDB(data):
    # Create new document dictionary (final version for DB) and clean up variables.
    document = {}

    document['date'] = datetime.datetime.fromisoformat(data['created_date'])
    document['station'] = data['station']
    document["air_temperature_celcius"] = data['air_temperature_celcius']
    document['relative_humidity'] = data['relative_humidity']
    document['windspeed_knots'] = data['windspeed_knots']
    document['max_wind_speed'] = data['max_wind_speed']
    document['precipitation'] = data['precipitation']
    document['precipitation_type'] = data['precipitation_type']
    document['ghi'] = data['ghi']

    if 'hotspots' in data:
        document['hotspots'] = []
        for each in data['hotspots']:
            hotspot = {}
            hotspot['time'] = datetime.datetime.fromisoformat(
                each['created_time'])
            hotspot['cause'] = each['cause']
            hotspot['confidence'] = each['confidence']
            hotspot['latitude'] = each['latitude']
            hotspot['longitude'] = each['longitude']
            hotspot['surface_temperature_celcius'] = each['surface_temperature_celcius']
            document['hotspots'].append(hotspot)

    return document


def sendDataToDB(iter):

    data_batch = iter.collect()  # Returns all elements as an array.
    # Send stream data to be transformed & analysed.
    climate_data = stream_handler(data_batch)

    # Sometimes batches may have no data, so we ensure that it isn't saved to database.
    if len(climate_data) > 1:
        # Send to remove key values that aren't in data-model, such as 'geo_hash'
        database_data = prepareForDB(climate_data)

        client = MongoClient()
        db = client.fit3182_assignment_db
        collection = db.climate

        # Insert climate data into database.
        collection.insert_one(database_data)
        pprint.pprint(database_data)

        client.close()


batch_interval = 10
topic = ["Climate", "Hotspot_AQUA", "Hotspot_TERRA"]

conf = SparkConf().setAppName("KafkaStreamProcessor").setMaster("local[2]")
sc = SparkContext.getOrCreate()
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, batch_interval)

kafkaStream = KafkaUtils.createDirectStream(ssc, topic, {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'climate_report',
    'fetch.message.max.bytes': '15728640',
    'auto.offset.reset': 'largest'})

# In batches every 10 seconds, set data to sendDatatoDB function.
lines = kafkaStream.foreachRDD(lambda rdd: sendDataToDB(rdd))

ssc.start()
# Run stream for 10 minutes just in case no detection of producer
time.sleep(600)
# ssc.awaitTermination()
ssc.stop(stopSparkContext=True, stopGraceFully=True)
