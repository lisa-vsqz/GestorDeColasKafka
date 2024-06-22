from kafka import KafkaProducer
import json


def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer


if __name__ == '__main__':
    topic = 'test-topic'
    producer = create_producer()

    while True:
        message = input("Enter message: ")
        producer.send(topic, {'message': message})
