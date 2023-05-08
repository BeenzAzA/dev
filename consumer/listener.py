import pika, sys, os, time
import json
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

def resv_data():
    connection = pika.BlockingConnection(pika.ConnectionParameters('my-rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='hallo', durable=True)

    def callback(ch, method, properties, body):
        time.sleep(10)
        print(" [x] Received %r" % body)
        data = json.loads(body)
        print("TYPE: {}".format(data['type']))
        print("NAME: {}".format(data['name']))
        print("SPEED: {}". format(data['speed']))
        bucket = "buck"
        org = "iot"
        token = "x_jWIGc3EkaLiqdg9zSLT73Aubz3PzKPc3w9UoPeA7HHi77fNxQXEZVhe8-8HfYDuKfehJbslzrCLzXZZRasMA=="
        #lW_CsMTDtFttr7N8jKvTj9dpToC90tzrN7-ey9IneEN03sqkbzwdMELNcvBTRxeHF2iWNf9X_UeOh09XrThQ_g==
        url="http://my-influxdb:8086"
        
        client = influxdb_client.InfluxDBClient(
                                    url=url,
                                    token=token,
                                    org=org
                                    )

        # Write script
        write_api = client.write_api(write_options=SYNCHRONOUS)

        p = influxdb_client.Point("my_measurement").tag("location", "Prague").field(data['type'], float(data['speed']))
        write_api.write(bucket=bucket, org=org, record=p)
        
    time.sleep(10)
    channel.basic_consume(queue='hallo', on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
    

if __name__ == '__main__':
    try:
        resv_data()
        time.sleep(10)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

#


# bucket = "buck"
# org = "iot"
# token = "x_jWIGc3EkaLiqdg9zSLT73Aubz3PzKPc3w9UoPeA7HHi77fNxQXEZVhe8-8HfYDuKfehJbslzrCLzXZZRasMA=="
# # Store the URL of your InfluxDB instance
# url="http://localhost:8086"

# client = influxdb_client.InfluxDBClient(
#     url=url,
#     token=token,
#     org=org
# )

# # Write script
# write_api = client.write_api(write_options=SYNCHRONOUS)

# p = influxdb_client.Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)
# write_api.write(bucket=bucket, org=org, record=p)