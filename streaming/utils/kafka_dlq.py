from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)

def send_to_dlq(topic, error):
    producer.send(f"dlq.{topic}", error)
    producer.flush()