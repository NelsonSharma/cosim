


def ImportCustomModule(python_file:str, python_object:str='', do_initialize:bool=False):
    r""" Import a custom module from a python file and optionally initialize it """
    import os, importlib.util
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
                    cmodule = getattr(cmodule, python_object)
                    if do_initialize:  cmodule = cmodule()
                except:         cmodule, failed = None, f'[!] Could not import object {python_object} from module "{cpath}"'
        else:                   cmodule, failed = None, f'[!] Could not import module "{cpath}"'
    else:                       cmodule, failed = None, f"[!] File Not found @ {cpath}"
    return cmodule, failed


# ------------------------------------------------------------------------------------------
# arguments parsing
# ------------------------------------------------------------------------------------------
import argparse, os, json, requests, io, pickle
from sys import exit
argp = argparse.ArgumentParser()
argp.add_argument('--info', type=str, default='')
argp.add_argument('--base', type=str, default='')
argp.add_argument('--mods', type=str, default='')
argp.add_argument('--point', type=str, default='')
parsed = argp.parse_args()


WORKDIR = f'{parsed.base}' 
if not WORKDIR: exit(f'Work dir not provided')
WORKDIR=os.path.abspath(WORKDIR)
TASKDIR = os.path.join(WORKDIR, "tasks")
DATADIR = os.path.join(WORKDIR, "data")

MODSDIR = f'{parsed.mods}' 
if not MODSDIR: exit(f'Modules dir not provided')
MODSDIR=os.path.abspath(MODSDIR)
# ------------------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------------------
import logging
LOGF = os.path.join(WORKDIR, 'run.txt')
LOGFILE = None
if LOGF: 
    LOGFILENAME = f'{LOGF}'
    try:# Set up logging to a file # also output to the console
        LOGFILE = os.path.abspath(LOGFILENAME)
        logging.basicConfig(filename=LOGFILE, level=logging.INFO, format='%(asctime)s - %(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger = logging.getLogger()
        logger.addHandler(console_handler)
    except: exit(f'[!] Logging could not be setup at {LOGFILE}')

if LOGFILE is None:
    def sprint(msg): print(msg) 
    def fexit(msg): exit(msg)
else:
    def sprint(msg): logging.info(msg) 
    def fexit(msg):
        logging.error(msg) 
        exit()


print(f'\n\n============================================================\n')

J = parsed.info
if not J: fexit(f'No task information provided')
J = os.path.abspath(J)
if not os.path.isfile(J): fexit(f'No task found at {J}')

with open(J, 'r') as j: task = json.load(j)

sprint(f'New Task: {task["uid"]}')
for k,v in task.items(): sprint(f'{k}: {v}')
sprint('\n')




# get the inputs first
dargs = {}
for itask in task['inputs']:
    iurl = task['inget'][itask]
    sprint(f"checking {iurl=}")
    response = requests.get(url=iurl)
    print(response)
    #filename = response.headers['Content-Disposition'].split(";")[-1].strip().split("=")[-1]
    filename = f'{task["uid"]}_{itask}'
    filepath = os.path.join(DATADIR, filename)
    with open(filepath, 'wb') as p: p.write(response._content)
    with open(filepath, 'rb') as p: dargs[itask] = pickle.load(p)


taskF, failed = ImportCustomModule(
    python_file=os.path.join(MODSDIR, f"{task['name']}.py"), python_object='main'
)

if failed: exit(f'Could not load task from {taskF}, {failed}')

douts = taskF(**dargs)

for oname,iout in zip(task['outputs'], douts):
    filename = f'{task["uid"]}_{oname}'
    filepath = os.path.join(DATADIR, filename)
    with open(filepath, 'wb') as j: pickle.dump(iout, j)
    ntask, nloc, nurl = task['outsend'][oname] # "outsend": {"y1": ["tB", "E", "http://127.0.0.1:9801"]
    epoint = 'send' if ntask else 'out'
    response = requests.post(
        url=f'{nurl}/{epoint}',
        json={
            'uid': f'{task["fid"]}_{ntask}',
            'outputs': {oname: f'{parsed.point}/data/{filename}'},
        }
    )

    sprint(response)

