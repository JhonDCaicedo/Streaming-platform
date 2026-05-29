from kafka import KafkaConsumer
import time
import json
from config.settings import KAFKA_BROKER, GROUP_ID

def create_consumer():
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKER,
            group_id=GROUP_ID,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None
        )
            
        consumer.subscribe(pattern="dbserver1.sourcedb.*")

        print("✅ Conectado a Kafka (consumer)")
        return consumer
    
    except Exception as e:
        print("⏳ Esperando Kafka...", e)
        time.sleep(5)
