from kafka import KafkaConsumer
import json

def create_consumer(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

if __name__ == '__main__':
    topic = 'test-topic'
    consumer = create_consumer(topic)

    for message in consumer:
        print(f"Received message: {message.value}")
