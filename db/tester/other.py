

import os, time, numpy as np, json, pickle
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Kio:
    @staticmethod
    def LoadJSON(path):
        with open(path, 'r') as f: obj = json.load(f)
        return obj
    @staticmethod
    def SaveJSON(path, obj, indent=4):
        with open(path, 'w') as f: json.dump(obj, f, indent=indent)
    @staticmethod
    def LoadPICK(path):
        with open(path, 'rb') as f: obj = pickle.load(f)
        return obj
    @staticmethod
    def SavePICK(path, obj):
        with open(path, 'wb') as f: pickle.dump(obj, f)
    @staticmethod
    def LoadTEXT(path):
        with open(path, 'r') as f: obj = f.read()
        return obj
    @staticmethod
    def SaveTEXT(path, obj):
        with open(path, 'w') as f: f.write(obj)
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

print("Loading from json file")
i2=(Kio.LoadJSON("t.json"))

with open("t.out", "w") as f: 
    f.write(f'{i2}\\n\\n')
time.sleep(4)
