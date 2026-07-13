

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


print("Loading from txt file")
i1 = (Kio.LoadTEXT("example.txt"))

print("Loading from json file")
i2=(Kio.LoadJSON("example.json"))

print("Loading from pickle file")
i3=(Kio.LoadPICK("data/example.pick"))

print("Loading from numpy file")
i4=(np.load("data/example.npy"))


with open("example.out", "w") as f: 
    f.write(f'{i1}\\n\\n')
    f.write(f'{i2}\\n\\n')
    f.write(f'{i3}\\n\\n')
    f.write(f'{i4}\\n\\n')
time.sleep(4)
with open("example.put", "w") as f: f.write(os.getcwd())
