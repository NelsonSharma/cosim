# ------------------------------------------------------------------------------------------
import datetime, os, json, pickle, importlib.util
# ------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------
# Custom Symbols
# ------------------------------------------------------------------------------------------

GTISEP = '_' # task id seperator
FIDSEP = '-' # flow id seperator
MODSEP = "." # module.caller
DEFCALL = "main" # default caller
# ------------------------------------------------------------------------------------------
# Custom Functions
# ------------------------------------------------------------------------------------------

def now(start:str='', sep:str='', end:str='') -> str:
    return (start + datetime.datetime.strftime(datetime.datetime.now(), sep.join(["%Y", "%m", "%d", "%H", "%M", "%S", "%f"])) + end)

def VALIDATE_PATH(base, req):
    target = os.path.abspath(os.path.join(base, req))
    rel = os.path.relpath(target, base)
    if rel.startswith(os.pardir + os.sep) or rel == os.pardir: return None
    else: return target

def str2bytes(size):
    sizes = dict(KB=2**10, MB=2**20, GB=2**30, TB=2**40)
    return int(float(size[:-2])*sizes.get(size[-2:].upper(), 0))

# ------------------------------------------------------------------------------------------

def ImportCustomModule(python_file:str, python_object:list):
    r""" Import a custom module from a python file and optionally initialize it """
    cpath = os.path.abspath(python_file)
    failed=""
    if os.path.isfile(cpath): 
        try: 
            # from https://stackoverflow.com/questions/67631/how-can-i-import-a-module-dynamically-given-the-full-path
            cspec = importlib.util.spec_from_file_location("", cpath)
            cmodule = importlib.util.module_from_spec(cspec)
            cspec.loader.exec_module(cmodule)
            success=True
        except: success=False #exit(f'[!] Could import user-module "{cpath}"')
        if success: 
            if python_object:
                try:
                    for po in python_object: cmodule = getattr(cmodule, po)
                except:         cmodule, failed = None, f'[!] Could not import object {python_object} from module "{cpath}"'
        else:                   cmodule, failed = None, f'[!] Could not import module "{cpath}"'
    else:                       cmodule, failed = None, f"[!] File Not found @ {cpath}"
    return cmodule, failed

# ------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------
# Custom Classes
# ------------------------------------------------------------------------------------------

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

# ------------------------------------------------------------------------------------------
