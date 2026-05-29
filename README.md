# 🚀 Streaming Data Platform (CDC + ETL + DLQ + Auto-Recovery)

## 📌 Overview

This project implements a **real-time streaming ETL platform** using:

- **CDC (Change Data Capture)** with Debezium
- **Kafka** as the event backbone
- **Python ETL Engine** with declarative mappings
- **DLQ (Dead Letter Queue)** for fault isolation
- **Auto-reprocessing (self-healing)** for recovery

---

## 🎯 Goal

Enable reliable **real-time data replication and transformation** from an OLTP database into a target system with:

- ✅ Fault tolerance  
- ✅ Automatic retries  
- ✅ Declarative transformations  
- ✅ Scalable architecture  

---

## 🧠 Architecture

              ┌──────────────┐
              │ Source DB    │
              └──────┬───────┘
                     ↓
                 Debezium
                     ↓
                   Kafka
                     ↓
           ┌─────────┴─────────┐
           │                   │
     ┌──────────────┐┌──────────────┐
     │      main    ││ reporcessor  │
     └──────────────┘└──────────────┘

---

## 🧱 Project Structure

    streaming-platform/
    │
    ├── streaming/
    │   ├── main.py                 # Main ETL pipeline (Kafka → Target)
    │   ├── reprocess_dlq.py        # DLQ reprocessor (auto-recovery)
    │   │
    │   ├── extract/
    │   │   └── kafka_consumer.py   # Kafka consumer (CDC events)
    │   │
    │   ├── transform/
    │   │   └── cdc_parser.py       # Debezium event parser
    │   │
    │   ├── mapping/
    │   │   ├── registry.py         # Mapper registry
    │   │   └── users.yaml          # Declarative mappings
    │   │
    │   ├── engine/
    │   │   └── mapper_engine.py    # Mapping transformation engine
    │   │
    │   ├── load/
    │   │   └── mariadb_loader.py   # Database loader (upsert/delete)
    │   │
    │   ├── utils/
    │   │   ├── kafka_dlq.py        # DLQ producer
    │   │   └── error_handler.py    # Error structuring
    │   │
    │   ├── validation/
    │   │   └── validator.py        # Config & mapping validation
    │   │
    │   └── config/
    │       └── settings.py         # Environment configuration
    │
    ├── Dockerfile                  # Streaming container build
    ├── requirements.txt            # Python dependencies
    └── docker-compose.yml          # Service orchestration

---

## ⚙️ Tech Stack

- Python
- Kafka
- Debezium
- MariaDB / MySQL
- Docker
- YAML (declarative mappings)

---

## 🔄 Streaming Pipeline Flow

Kafka → Consumer → Parser → Mapper → Loader → Target DB

---

## 🧪 Example Flow

### Source Event (Debezium)

    json
    {
      "op": "c",
      "after": {
        "id": 1,
        "name": "John"
      }
    }

    primary_key: customer_id
    
    mappings:
      customer_id: id
      full_name: name
      
    Transformed Output
    {
      "customer_id": 1,
      "full_name": "John"
    }

### 🔁 ETL Processing Logic

    event = parse_event(msg)
    mapped = mapper.apply(event)
    
    if not mapped or not mapped.get("data"):
        return
    
    if mapped["operation"] in ["c", "u", "r"]:
        loader.upsert(mapped["table"], mapped["data"])
    
    elif mapped["operation"] == "d":
        loader.delete(...)

###🧨 Error Handling (DLQ)
Errors are automatically sent to a DLQ topic:
dlq.dbserver1.sourcedb.users

Error Structure

    {
      "stage": "LOAD",
      "error": "...",
      "event": {},
      "retries": 0
    }

### 🔁 Auto-Recovery (DLQ Worker)
Failed events are automatically retried:
    DLQ → Reprocess Worker → ETL → Target ✅

Reprocess Logic

    if not mapped or not mapped.get("data"):
        return
    
    loader.upsert(...)

### 🐳 Deployment
Build and start services

    docker compose up -d --build
    docker ps

### ✅ Expected Behavior

Data replicated to target ✅
Errors routed to DLQ ✅
Failed events automatically reprocessed ✅


### 🔥 Key Features

✅ Real-time ETL
✅ CDC-based ingestion
✅ Declarative mapping (YAML)
✅ Fault isolation (DLQ)
✅ Self-healing pipeline
✅ Dockerized environment


### 🧠 Design Principles

Event-driven architecture
Decoupled components
Configuration over code
Resilience and retryability

### 🎯 Conclusion
This project demonstrates a production-style streaming data platform with:

Real-time processing
Fault tolerance
Automatic recovery
Scalable architecture


# 👨‍💻 Author
Jhon David Caicedo Alvarez

---

# 🔥 RESULTADO

Con este README tienes:

✅ presentación profesional  
✅ arquitectura clara  
✅ código real  
✅ fácil de entender  
✅ listo para GitHub  

---

# 🚀 OPCIONAL (RECOMENDADO)

Si quieres hacerlo aún más pro:

👉 agregar badges:

```markdown
https://img.shields.io/badge/docker-ready-blue
https://img.shields.io/badge/kafka-streaming-orange
https://img.shields.io/badge/python-3.9-blue


