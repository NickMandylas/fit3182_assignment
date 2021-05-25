from kafka import KafkaConsumer
from json import loads
import datetime as dt
import matplotlib.pyplot as plt
import statistics

# Required for Jupyter inline display
# %matplotlib notebook

topic = "Climate"


def connect_kafka_consumer():
    _consumer = None
    try:
        _consumer = KafkaConsumer(
            topic,
            # consumer_timeout_ms=10000, # stop iteration if no message after 10 sec
            # comment this if you don't want to consume earliest available message
            auto_offset_reset='earliest',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: loads(
                x.decode('ascii')),
            api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _consumer


def init_plot():
    try:
        width = 10
        height = 7

        fig = plt.figure(figsize=(width, height))
        fig.suptitle(
            'Realtime Stream of Air Temperature (Celcius) and Arrival Time')

        ax = fig.add_subplot(221)
        ax.set_xlabel('Arrival Time')
        ax.set_ylabel('Air Temperature (Celcius)')
        ax.set_yticks([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50])
        ax.set_ylim(0, 50)

        fig.show()
        fig.canvas.draw()

        return fig, ax

    except Exception as ex:
        print(str(ex))


def consume_messages(consumer, fig, asx):
    try:
        x, y = [], []

        for message in consumer:
            message = message.value
            # print(message)

            message_date = dt.datetime.fromisoformat(message['created_date'])
            clean_date = message_date.strftime('%-d/%m/%y')
            x.append(clean_date)
            y.append(message['air_temperature_celcius'])

            # Start plotting when we have 10 data points
            if len(y) > 10:
                ax.clear()
                ax.plot(x, y)
                ax.set_xlabel('Arrival Time')
                ax.set_ylabel('Air Temperature (Celcius)')
                ax.set_yticks([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50])
                ax.set_ylim(0, 50)
                ax.tick_params(axis='x', labelrotation=45)

                y_min = min(y)
                x_min_pos = y.index(y_min)
                x_min = x[x_min_pos]
                y_max = max(y)
                x_max_pos = y.index(y_max)
                x_max = x[x_max_pos]
                min_label = 'Minimum (Date: ' + x_min + \
                    ' & Temp: ' + str(y_min) + ')'
                max_label = 'Maximum (Date: ' + x_max + \
                    ' & Temp: ' + str(y_max) + ')'
                ax.annotate(min_label, xy=(x_min, y_min), xytext=(
                    x_min, y_min), arrowprops=dict(facecolor='yellow', shrink=0.04))
                ax.annotate(max_label, xy=(x_max, y_max), xytext=(
                    x_max, y_max), arrowprops=dict(facecolor='red', shrink=0.04))

                fig.canvas.draw()
                x.pop(0)
                y.pop(0)

        plt.close('all')
    except Exception as ex:
        print(str(ex))


if __name__ == '__main__':

    consumer = connect_kafka_consumer()
    fig, ax = init_plot()
    consume_messages(consumer, fig, ax)
