import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        # https://www.rabbitmq.com/tutorials/tutorial-one-python
        # Sin argumentos crea una cola clasica (en el ejemplo crea una quorum)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.queue = self.channel.queue_declare(queue=queue_name)
        pass

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            on_message_callback(message=body,ack=ack,nack=nack)

        self.channel.basic_consume(queue=self.queue.method.queue, on_message_callback=callback, auto_ack=False)
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        try: 
            self.channel.basic_publish(
                exchange='',            # TODO: Default exchange 
                routing_key=self.queue.method.queue,    
                body=message
            )
        except pika.exceptions.ChannelClosed(): # https://pika.readthedocs.io/en/stable/modules/exceptions.html
            raise MessageMiddlewareDisconnectedError()
        except Exception() as e:
            print("Error ", e)
            raise MessageMiddlewareMessageError()

    def close(self):
        try:
            self.connection.close()
        except Exception() as e:
            print(f"Error {e}")
            raise MessageMiddlewareCloseError()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.exchange_name = exchange_name
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct', durable=True)

        # TODO: no deberian ser dict[routing_key] = [queue] ? queue='' esta bien ?
        # https://www.rabbitmq.com/tutorials/tutorial-four-python
        self.queues = []
        for key in routing_keys: 
            self.queues.append(self.channel.queue_declare(queue=key))
            self.channel.queue_bind(exchange=exchange_name, queue=key, routing_key=key)
        print("[exchange] INIT FIN")

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            on_message_callback(message=body,ack=ack,nack=nack)

        for queue in self.queues:
            self.channel.basic_consume(queue=queue.method.queue, on_message_callback=callback, auto_ack=False)
        print("[exchange] START CONSUMING")
        self.channel.start_consuming()

    def stop_consuming(self):
        print("[exchange] STOP CONSUMING")
        self.channel.stop_consuming()

    def send(self, message):
        print(f"[exchange] SEND START")
        for key in self.queues:
            print(f"[exchange] SEND {key} {message}")
            self.channel.basic_publish(exchange=self.exchange_name, routing_key=key.method.queue, body=message)

    def close(self):
        self.connection.close()
