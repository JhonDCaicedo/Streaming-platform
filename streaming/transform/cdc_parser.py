
def parse_event(msg):

    if msg.value is None:
        return None

    payload = msg.value.get("payload")

    if not payload:
        return None

    return {
        "table": msg.topic.split('.')[-1],
        "operation": payload.get("op"),
        "after": payload.get("after"),
        "before": payload.get("before")
    }
