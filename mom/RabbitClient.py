import threading

import pika

message_broker_ip = "127.0.0.1"
message_broker_user = "bunny"
message_broker_pwd = "bunny1234"
message_broker_virtual_host = "bunny_host"
exchange_name = "bunny_exchange"


def main():
    cred = pika.PlainCredentials(message_broker_user, message_broker_pwd)
    params = pika.ConnectionParameters(host=message_broker_ip,
                                       credentials=cred,
                                       virtual_host=message_broker_virtual_host)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange_name, 'fanout')

    name = input('What ... is your name: ')
    headers = {'sender_id': name}
    properties = pika.spec.BasicProperties(headers=headers)

    Consumer(name).start()

    while True:
        print("Message to send: ")
        msg = input()
        channel.basic_publish(exchange=exchange_name,
                              routing_key='',
                              body=msg,
                              properties=properties)


class Consumer:
    def __init__(self, name):
        self.name = name

    def start(self):
        consumer_thread = threading.Thread(target=self.consume)
        consumer_thread.start()

    def consume(self):
        cred = pika.PlainCredentials(message_broker_user, message_broker_pwd)
        params = pika.ConnectionParameters(host=message_broker_ip,
                                           credentials=cred,
                                           virtual_host=message_broker_virtual_host)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        q = channel.queue_declare(queue='', exclusive=True)
        queue_name = q.method.queue
        channel.queue_bind(exchange=exchange_name, queue=queue_name)
        channel.basic_consume(queue=queue_name,
                              on_message_callback=self.callback)
        channel.start_consuming()

    def callback(self, ch, method, properties, body):
        sender_id = properties.headers.get('sender_id', 'UNKNOWN')
        msg = body.decode('UTF-8')
        if sender_id != self.name:
            print(" -> Received {} from {}".format(msg, sender_id))


if __name__ == '__main__':
    main()
