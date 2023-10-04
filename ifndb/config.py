import json
import os

settings = {}

def load_config():
    file = 'settings.json'
    env = os.getenv('IFNDB_SETTINGS', '')
    if env != '':
        file = env
    try:
        data = json.load(open(file, 'r'))
    except Exception as e:
        print("Error loading settings in %s" % file)
        print(e)
        
    for name, param in data.items():
        settings[name] = param
    