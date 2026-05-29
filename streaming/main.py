from extract.kafka_consumer import create_consumer
from transform.cdc_parser import parse_event
from load.mariadb_loader import MariaLoader
from mapping.registry import MAPPERS
from utils.kafka_dlq import send_to_dlq
from utils.error_handler import build_error
from validation.validator import validate_all

# ✅ validar YAML al iniciar
validate_all()

consumer = create_consumer()
loader = MariaLoader()

print("🚀 ETL Streaming iniciado...")

for msg in consumer:
    
    # ---------- TRANSFORM ----------
    try:
        event = parse_event(msg)
        if not event:
            continue
    except Exception as e:
        send_to_dlq(msg.topic, build_error("TRANSFORM", e, msg))
        continue

    # ---------- MAPPING ----------
    try:
        mapper = MAPPERS.get(event["table"])
        if not mapper:
            print("⛔ sin mapper:", event["table"])
            continue
        mapped = mapper.apply(event)
        if not mapped or not mapped.get("data"):
            print("⚠️ evento inválido:", event)
            continue
    except Exception as e:
        send_to_dlq(msg.topic, build_error("MAPPING", e, msg))
        continue

    # ---------- LOAD ----------
    try:
        if mapped["operation"] in ["c", "u"]:
            loader.upsert(mapped["table"], mapped["data"])

        elif mapped["operation"] == "d":
            pk_name = mapped["primary_key"]
            pk_value = mapped["data"][pk_name]
            loader.delete(mapped["table"], pk_name, pk_value)

    except Exception as e:
        send_to_dlq(msg.topic, build_error("LOAD", e, msg))
