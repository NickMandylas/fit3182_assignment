from time import sleep
from json import dumps
from kafka import KafkaProducer
import random
import datetime as dt
import pandas


def readCSV():
    climate_streaming_data = pandas.read_csv('climate_streaming.csv')
    streaming_data = []
    for _, row in climate_streaming_data.iterrows():
        data_point = {}
        data_point['latitude'] = float(row['latitude'])
        data_point['longitude'] = float(row['longitude'])
        data_point['air_temperature_celcius'] = float(
            row['air_temperature_celcius'])
        data_point['relative_humidity'] = float(row['relative_humidity'])
        data_point['windspeed_knots'] = float(row['windspeed_knots'])
        data_point['max_wind_speed'] = float(row['max_wind_speed'])

        # Unncessary space at beginning of value is removed.
        # We also split precipation type and amount, to make it easier for sorting/searching later.
        precipitation = str(row['precipitation ']).replace(" ", "")
        data_point['precipitation_type'] = precipitation[-1]
        data_point['precipitation'] = float(precipitation[0:-1])

        data_point['ghi'] = float(row['GHI_w/m2'])

        streaming_data.append(data_point)

    return streaming_data


def publish_message(producer_instance, topic_name, data):
    try:
        value_bytes = bytes(data, encoding='utf-8')
        producer_instance.send(topic_name, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully. Data: ' + str(data))
    except Exception as ex:
        print('Exception in publishing message.')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka.')
        print(str(ex))
    finally:
        return _producer


if __name__ == '__main__':

    data = readCSV()
    topic = 'Climate'
    producer = connect_kafka_producer()
    created_date = dt.datetime(2018, 12, 31)

    while True:
        random_number = random.randrange(0, len(data))
        selected_data = data[random_number]
        created_date += dt.timedelta(days=1)
        selected_data['created_date'] = created_date
        selected_data['producer_id'] = 'producer_climate'

        transport_data = str(selected_data)
        publish_message(producer, topic, transport_data)

        sleep(10)
