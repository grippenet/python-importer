import json
import os
settings = {}

def load_config():
    file = 'settings.json'
    env = os.getenv('IFNDB_SETTINGS', '')
    if env != '':
        file = env
    data = json.load(open(file, 'r'))
    for name, param in data.items():
        settings[name] = param
    