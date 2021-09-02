try:
    import orjson
    json = orjson
except ModuleNotFoundError:
    import json
    json = json
