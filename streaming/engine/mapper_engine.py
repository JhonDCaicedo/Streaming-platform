
import yaml

class MapperEngine:

    def __init__(self, path):
        with open(path) as f:
            self.config = yaml.safe_load(f)

    def apply(self, event):
        after = event["after"]
        op = event["operation"]

        if op in ["c", "u", "r"] and after:
            data = {}

            for target, rule in self.config["mappings"].items():

                if isinstance(rule, str):
                    data[target] = after.get(rule)

                elif rule["type"] == "concat":
                    values = [after.get(f, "") for f in rule["fields"]]
                    data[target] = " ".join(values)

            return {
                "operation": op,
                "table": self.config["target_table"],
                "data": data
            }

        elif op == "d" and event.get("before"):

            pk_target = self.config["primary_key"]

            source_field = None
            for target, rule in self.config["mappings"].items():
                if target == pk_target:
                    if isinstance(rule, str):
                        source_field = rule

            return {
                "operation": op,
                "table": self.config["target_table"],
                "data": {
                    pk_target: event["before"][source_field]
                },
                "primary_key": pk_target
            }

