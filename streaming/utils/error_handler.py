from datetime import datetime

def build_error(stage, error, msg):

    return {
        "stage": stage,
        "error": str(error),
        "original_topic": msg.topic,
        "event": msg.value,
        "retries": 0,
        "timestamp": datetime.utcnow().isoformat()
    }
