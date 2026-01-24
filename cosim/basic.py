# ------------------------------------------------------------------------------------------
import json, pickle, datetime
# ------------------------------------------------------------------------------------------

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def timestamp(start:str='', sep:str='', end:str='') -> str:
    return (start + datetime.datetime.strftime(datetime.datetime.now(), sep.join(["%Y", "%m", "%d", "%H", "%M", "%S", "%f"])) + end)
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Kio:
    
    @staticmethod
    def LoadJSON(path):
        with open(path, 'r') as f: obj = json.load(f)
        return obj

    @staticmethod
    def SaveJSON(path, obj):
        with open(path, 'w') as f: json.dump(obj, f, indent=4)

    @staticmethod
    def LoadPICK(path):
        with open(path, 'rb') as f: obj = pickle.load(f)
        return obj

    @staticmethod
    def SavePICK(path, obj):
        with open(path, 'wb') as f: pickle.dump(obj, f)
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# ------------------------------------------------------------------------------------------
