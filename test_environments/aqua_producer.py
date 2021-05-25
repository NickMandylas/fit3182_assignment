from time import sleep
from json import dumps
from kafka import KafkaProducer
import random
import datetime as dt
import pandas


def readCSV():
    climate_streaming_data = pandas.read_csv('hotspot_AQUA_streaming.csv')
    streaming_data = []
    for _, row in climate_streaming_data.iterrows():
        data_point = {}
        data_point['latitude'] = float(row['latitude'])
        data_point['longitude'] = float(row['longitude'])
        data_point['confidence'] = float(row['confidence'])
        data_point['surface_temperature_celcius'] = float(
            row['surface_temperature_celcius'])

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
    topic = 'Hotspot_AQUA'
    producer = connect_kafka_producer()
    created_date = dt.datetime(2019, 1, 1)

    count = 0

    while True:
        count += 4

        random_number = random.randrange(0, len(data))
        selected_data = data[random_number]

        if count > 16:
            created_date += dt.timedelta(days=1)
            created_date.replace(hour=0, minute=0, second=0)
            count = 0

        selected_data['created_time'] = created_date + dt.timedelta(
            hours=(random.randrange(count - 4, count)),
            minutes=(random.randrange(0, 60)),
            seconds=(random.randrange(0, 60)))
        # print(selected_data['created_time'].strftime("%m/%d/%Y, %H:%M:%S"))
        selected_data['producer_id'] = 'producer_hotspot_aqua'

        transport_data = str(selected_data)
        publish_message(producer, topic, transport_data)

        sleep(2)
