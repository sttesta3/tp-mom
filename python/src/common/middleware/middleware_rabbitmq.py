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
        self.queue_name = queue_name
        self.queue = self.channel.queue_declare(queue=self.queue_name)

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
                exchange='',            # TODO: Default exchange y routing key hello ?
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
            print("Error ", e)
            raise MessageMiddlewareCloseError()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self, on_message_callback):
        pass    

    def send(self, message):
        pass

    def close(self):
        pass