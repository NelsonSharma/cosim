
import argparse, os, json, requests, io, pickle, time, datetime
from sys import exit
from .basic import GTISEP, MODSEP, DEFCALL, ImportCustomModule
now = datetime.datetime.now
# ------------------------------------------------------------------------------------------
# arguments parsing
# ------------------------------------------------------------------------------------------
argp = argparse.ArgumentParser()
argp.add_argument('--info', type=str, default='')
argp.add_argument('--base', type=str, default='')
argp.add_argument('--mods', type=str, default='')
argp.add_argument('--log', type=str, default='')
parsed = argp.parse_args()

TS = []
# ------------------------------------------------------------------------------------------

# START

# ------------------------------------------------------------------------------------------
TS.append((time.perf_counter(), now(), 'TASK_BEGIN'))
# ------------------------------------------------------------------------------------------
WORKDIR = f'{parsed.base}' 
if not WORKDIR: exit(f'Work dir not provided')
WORKDIR=os.path.abspath(WORKDIR)
#TASKDIR = os.path.join(WORKDIR, "tasks")
DATADIR = os.path.join(WORKDIR, "data")

MODSDIR = f'{parsed.mods}' 
if not MODSDIR: exit(f'Modules dir not provided')
MODSDIR=os.path.abspath(MODSDIR)
# ------------------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------------------
import logging
if not parsed.log: exit(f'log file not specified')
LOGF = os.path.join(WORKDIR, f'{parsed.log}')
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

sprint(f'\n============================================================\n')
J = parsed.info
if not J: fexit(f'No task information provided')
J = os.path.abspath(J)
if not os.path.isfile(J): fexit(f'No task found at {J}')
with open(J, 'r') as j: task = json.load(j)
sprint(f'Start Task:\n{task["uid"]}')
sprint('\n')
#sprint(f'\n{task=}\n')
# ------------------------------------------------------------------------------------------
TS.append((time.perf_counter(), now(), 'LOAD_INPUTS'))
# ------------------------------------------------------------------------------------------
# get the inputs
dargs = {}
for itask in task['inputs']:
    iurl = task['inget'][itask]
    ipath = os.path.join(DATADIR, iurl)
    if not os.path.isfile(ipath): fexit(f'Failed to fetch inputs from {ipath}')
    with open(ipath, 'rb') as j: dargs[itask] = pickle.load(j)
# ------------------------------------------------------------------------------------------
TS.append((time.perf_counter(), now(), 'EXECUTE_TASK'))
# ------------------------------------------------------------------------------------------
# import task to be performed
taskN = f"{task['name']}".split(MODSEP)
modFile = taskN.pop(0)
modCall = taskN if taskN else [DEFCALL]
taskF, failed = ImportCustomModule(python_file=os.path.join(MODSDIR, f"{modFile}.py"), python_object=modCall)
if failed: exit(f'Could not load task from {taskF}, {failed}')
# execute task
sprint(f'Executing task {taskF.__name__}')
douts = taskF(**dargs)
# ------------------------------------------------------------------------------------------
TS.append((time.perf_counter(), now(), 'SEND_OUTPUTS'))
# ------------------------------------------------------------------------------------------
sprint(f'Finished executing task {taskF.__name__} with {len(douts)} outputs')
# send outputs
output_data_size = 0
for oname,iout in zip(task['outputs'], douts):

    ntask, nloc, nurl, durl = task['outsend'][oname] # ["tB", "E", "http://127.0.0.1:nport", "http://127.0.0.1:dport"]
    filename = f'{task["uid"]}{GTISEP}{oname}'

    buffer = io.BytesIO()
    buffer.write(pickle.dumps(iout))
    data_size = buffer.tell()
    output_data_size+=data_size
    sprint(f'Sending data {filename} of size {data_size} Bytes to {durl}')

    buffer.seek(0)
    response=requests.post(f'{durl}/data/{filename}', files={"data":buffer})
    sprint(f'Data-sent, response code is {response.status_code}')
    del buffer
    
    eurl=f'{nurl}/{'notify' if ntask else 'out'}'
    etaskid = f'{task["fid"]}{GTISEP}{ntask}'
    sprint(f'Sending task {etaskid} output {oname} to {eurl}')
    response = requests.post(url=eurl, json={'uid': etaskid, 'outputs': {oname: f'{filename}'},})
    sprint(f'Notification-sent, response code is {response.status_code}')

    if not ntask:  
        sprint(f'Sending FIN.{task["fid"]}.{task["uid"]} notification to offloader')
        offurl = f"{task['offloader']}/fin"
        response = requests.post(url=offurl, json={'uid': etaskid, 'output':f'{filename}', })
        sprint(f'Notification-sent, response code is {response.status_code}, {response.json()}')
# ------------------------------------------------------------------------------------------
TS.append((time.perf_counter(), now(), 'TASK_END'))
# ------------------------------------------------------------------------------------------


DeltaPs, DeltaTs = [], []
for i in range(len(TS)-1):
    DeltaPs.append(TS[i+1][0] - TS[i][0])
    DeltaTs.append(str(TS[i+1][1] - TS[i][1]))
DeltaP =  TS[-1][0] - TS[0][0]
DeltaT =  str(TS[-1][1]-TS[0][1])
sprint(f'{DeltaPs=}')
sprint(f'{DeltaTs=}')
sprint(f'Finished Task:\n{task["uid"]}\n⌛\nCounter [{DeltaP}]\nTime [{DeltaT}]\nData Sent [{output_data_size}] Bytes')
# ------------------------------------------------------------------------------------------
