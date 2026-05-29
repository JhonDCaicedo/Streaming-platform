import os
import yaml

def validate_all():

    for file in os.listdir("mapping"):

        if file.endswith(".yaml"):
            with open(f"mapping/{file}") as f:
                config = yaml.safe_load(f)

                if "mappings" not in config:
                    raise Exception(f"❌ YAML inválido: {file}")

    print("✅ YAML OK")
