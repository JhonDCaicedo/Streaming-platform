from datetime import datetime
import json
import time

from kafka import KafkaConsumer
from transform.cdc_parser import parse_event
from mapping.registry import MAPPERS
from load.mariadb_loader import MariaLoader
from utils.kafka_dlq import send_to_dlq

loader = MariaLoader()

consumer = KafkaConsumer(
    bootstrap_servers='kafka:9092',
    group_id='dlq_reprocessor-group',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
)
consumer.subscribe(pattern=r'^dlq\..*')

class DummyMsg:
    def __init__(self, value, topic):
        self.value = value
        self.topic = topic


def process_message(msg):
    dlq_data = msg.value

    if not dlq_data:
        print('⚠️ mensaje DLQ vacío, descartado')
        return True

    retries = dlq_data.get('retries', 0)

    if retries >= 3:
        print('🚫 descartado:', dlq_data)
        return True

    if dlq_data.get('stage') == 'MAPPING':
        print('⛔ no reprocesar mapping:', dlq_data)
        return True

    original_event = dlq_data.get('event')
    original_topic = dlq_data.get('original_topic')

    if not original_event or not original_topic:
        print('⚠️ datos DLQ incompletos:', dlq_data)
        return True

    try:
        dummy_msg = DummyMsg(original_event, original_topic)
        event = parse_event(dummy_msg)

        if not event:
            print('⚠️ no se pudo parsear el evento original:', original_event)
            return True

        mapper = MAPPERS.get(event['table'])
        if not mapper:
            print('⛔ no mapper:', event['table'])
            return True

        mapped = mapper.apply(event)
        if not mapped or not mapped.get('data'):
            print('⚠️ evento inválido:', event)
            return True

        if mapped['operation'] in ['c', 'u', 'r']:
            loader.upsert(mapped['table'], mapped['data'])

        elif mapped['operation'] == 'd':
            pk_name = mapped.get('primary_key')
            if not pk_name or pk_name not in mapped['data']:
                print('⚠️ clave primaria inválida en mapped:', mapped)
                return True
            pk_value = mapped['data'][pk_name]
            loader.delete(mapped['table'], pk_name, pk_value)

        else:
            print('⚠️ operación no soportada en mapped:', mapped['operation'])
            return True

        print('✅ REPROCESADO:', mapped)
        return True

    except Exception as e:
        retries += 1
        dlq_data['retries'] = retries
        dlq_data['error'] = str(e)
        dlq_data['timestamp'] = datetime.utcnow().isoformat()
        send_to_dlq(original_topic, dlq_data)
        print(f'❌ fallo reproceso; re-enviado DLQ (retry {retries}):', e)
        time.sleep(min(2 ** retries, 30))
        return True


print('🚨 DLQ Worker activo...')

while True:
    try:
        for msg in consumer:
            process_message(msg)
            consumer.commit()

    except Exception as e:
        print('❌ error global:', e)
        time.sleep(5)

