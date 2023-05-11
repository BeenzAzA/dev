# import pika, os, logging, time



# url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost')
# params = pika.URLParameters(url)
# params.socket_timeout = 10

# connection = pika.BlockingConnection(params)  # Connect to CloudAMQP
# channel = connection.channel()  # start a channel
# for x in range(10):
#     # Message to send to rabbitmq
#     bodys = 'data ke ' + str(x + 1)

#     channel.basic_publish(exchange='', routing_key='hallo', body=bodys)
#     print("[x] Message sent to consumer = " + bodys)
#     time.sleep(10)
# connection.close()
import pika, time
import json
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672))
channel = connection.channel()
channel.queue_declare(queue='hallo', durable=True)

for i in range (10):
#     body_ = 'data ' + str(i+1)
    data = {
    "type": "Bus",
    "name": "M178OK",
    "speed": i*16.79 + 1000
    }
    message = json.dumps(data)
    channel.basic_publish(exchange='', routing_key='hallo', body=message)
    print("[x] Sent %s", message)
    time.sleep(10)
connection.close()