"""JSON console presentation shared by command entrypoints."""
import json


def emit(value):
    print(json.dumps(value, ensure_ascii=False, allow_nan=False), flush=True)
