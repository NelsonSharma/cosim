# ------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------
from sys import exit
if __name__!='__main__': exit(f'[!] can not import {__name__}.{__file__}')
# ------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
# arguments parsing
# ------------------------------------------------------------------------------------------

import argparse, os
argp = argparse.ArgumentParser()
argp.add_argument('--policy',              type=str, default='',        help='abs - location of decision maker script')
argp.add_argument('--infra',           type=str, default='',        help='abs- location of infra.json')
argp.add_argument('--base',            type=str, default='base',    help='base directory to store and serve files')
argp.add_argument('--script',          type=str, default='python3', help='python executable to run tasks')
argp.add_argument('--maxupsize',       type=str, default='500GB',   help='http_body_size for waitress',) 
argp.add_argument('--maxconnect',      type=int, default=1000,      help='maximum number of connections allowed') 
argp.add_argument('--threads',         type=int, default=1,         help='waitress thread count') 
argp.add_argument('--host',            type=str, default='127.0.0.1',      help='waitress server-interface, keep 0.0.0.0 to serve on all interfaces')
argp.add_argument('--port',            type=str, default='9000',           help='waitress server-port')
argp.add_argument('--log',             type=str, default='log.txt',  help='keep blank for no logging')
argp.add_argument('--verbose',         type=int, default=1,         help='set to 0 for no verbose')
argp.add_argument('--secret',          type=str, default='',        help='fask secret, keep blank to generate new on run')
argp.add_argument('--https',           type=int, default=0,         help='set to 1 if reverse proxy with https is used')
argp.add_argument('--node_id',          type=str, default='')
parsed = argp.parse_args()

# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
# Custom Symbols
# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
# imports
# ------------------------------------------------------------------------------------------

import logging, subprocess, datetime, json, random
from sys import exit
from flask import Flask, request, send_file, abort, jsonify # redirect, url_for,
from waitress import serve 
PROXY_FIX=bool(parsed.https)
if PROXY_FIX: from werkzeug.middleware.proxy_fix import ProxyFix
# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
# Directories
# ------------------------------------------------------------------------------------------
WORKDIR = f'{parsed.base}' 
if not WORKDIR: WORKDIR = os.getcwd()
WORKDIR=os.path.abspath(WORKDIR)
try: os.makedirs(WORKDIR, exist_ok=True)
except: exit(f'[!] Workspace directory was not found and could not be created')
TASKDIR = os.path.join(WORKDIR, "tasks")
DATADIR = os.path.join(WORKDIR, "data")
# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------------------
LOGF = f'{parsed.log}' 
LOGFILE = None
if LOGF and parsed.verbose: 
    LOGFILENAME = os.path.join(WORKDIR, LOGF)
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
# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
# verbose level
# ------------------------------------------------------------------------------------------
if not parsed.verbose:
    def sprint(msg): pass
    def fexit(msg): exit(msg)
else:
    if LOGFILE is None:
        def sprint(msg): print(msg) 
        def fexit(msg): exit(msg)
    else:
        def sprint(msg): logging.info(msg) 
        def fexit(msg):
            logging.error(msg) 
            exit()
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
# Globals
# ------------------------------------------------------------------------------------------


from .basic import DEFCALL, ValidatePath, str2bytes, Kio, ImportCustomModule
from .flow import Flow
from .man import Manager
from . import db

FLOWS = db.Flows(Creator=Flow)
TASKQ = {} # maintains a dict of pending tasks

# ------------------------------------------------------------------------------------------
# Initialization
# ------------------------------------------------------------------------------------------
sprint(f'Starting...')
if PROXY_FIX: sprint(f'↪ PROXY_FIX is True, assume that reverse proxy engine is running ... ')
sprint(f'↪ Logging @ {LOGFILE}')
sprint(f'↪ Work directory is {WORKDIR}')

try: os.makedirs(TASKDIR, exist_ok=True)
except: fexit(f'[!] Tasks directory was not found and could not be created at {TASKDIR}')
sprint(f'↪ Task directory is {TASKDIR}')

try: os.makedirs(DATADIR, exist_ok=True)
except: fexit(f'[!] Data directory was not found and could not be created at {DATADIR}')
sprint(f'↪ Data directory is {DATADIR}')

EXESCRIPT = f'{parsed.script}' 
if not EXESCRIPT: fexit(f'[!] Tasks executor was not specified')
sprint(f'↪ Tasks executor is {EXESCRIPT}')

INFRAJSON = f'{parsed.infra}' 
if not INFRAJSON: fexit(f'[!] Infra Json was not specified')
INFRAJSON = os.path.abspath(INFRAJSON)
INFRA = Kio.LoadJSON(INFRAJSON)
sprint(f'↪ Infra loaded from {INFRAJSON}')
# ------------------------------------------------------------------------------------------


if parsed.policy:   # nodes, locs = newflow.NODES, set(INFRA.keys())
    policy_file = os.path.abspath(parsed.policy)
    Decide, failed = ImportCustomModule(python_file=policy_file, python_object=DEFCALL)
    if failed: fexit(f'↪ policy could not be loaded from {policy_file}.{DEFCALL}')
    else: sprint(f'↪ Decision Maker is {Decide.__name__}/{Decide.__code__.co_filename}')
else:
    Decide = lambda n, l: {k:l[0] for k in n}
    sprint(f'↪ Decision Maker not specified, using no-offloading scheme.')




# ------------------------------------------------------------------------------------------
# Flask App Define
# ------------------------------------------------------------------------------------------

app = Flask(
    __name__,
    static_folder=WORKDIR,
    template_folder=WORKDIR,
    instance_relative_config = True,
    instance_path = WORKDIR,
)
if PROXY_FIX: app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
app.secret_key = parsed.secret if parsed.secret else f'{random.randint(11111, 99999)}'

# ------------------------------------------------------------------------------------------



# ------------------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------
@app.route("/add", methods=["POST"])
@app.route("/add/", methods=["POST"])
def route_add():
    """ adds task to device's TASKQ """
    global TASKQ
    taskinfo = request.get_json()
    taskname = taskinfo['flow_name']
    taskuid = taskinfo['uid']
    sprint(f'Recived Task {taskname}/{taskuid}')
    TASKQ[taskuid] = {**taskinfo}
    TASKQ[taskuid]["inget"] = {} # initialize waiting queue
    #Kio.SaveJSON(os.path.join(WORKDIR, f'TASKQ_STA_{taskuid}.json'), TASKQ)
    return {"received": taskuid}, 200
# ------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------
@app.route("/notify", methods=["POST"])
@app.route("/notify/", methods=["POST"])
def route_note():
    """ signals outputs/inputs """
    datainfo = request.get_json()
    taskname = datainfo['flow_name']
    taskuid =  datainfo['uid']
    global TASKQ
    sprint(f'Recived data for {taskname}/{taskuid} - Current Queue len is {len(TASKQ)}')
    for o,url in datainfo['outputs'].items(): TASKQ[taskuid]["inget"][o] = url
    launched = [] # check which task can be launched
    for uid, taskinfo in TASKQ.items():
        can_launch = not (False in [ i in taskinfo["inget"] for i in taskinfo['inputs'] ])
        if can_launch: 
            taskpath = os.path.join(TASKDIR, f'{uid}.json')
            with open(taskpath, 'w') as f: json.dump(taskinfo, f)
            sprint(f'Starting task {uid} using {taskpath} executable is {os.path.abspath(EXESCRIPT)}')
            subprocess.Popen([
                f"{EXESCRIPT}", "-m", "cosim.run", 
                "--base", f"{WORKDIR}", 
                "--info", f'{taskpath}', 
                "--log", f'{uid}.log',
                "--node_id", f'{parsed.node_id}',
                ])
            launched.append(uid)
    sprint(f'Lanch {len(launched)} Tasks: {launched=}')
    if launched:
        for uid in launched: del TASKQ[uid]
    return {"received": f"{taskname}/{taskuid}", "data": list(datainfo['outputs'].keys())}, 200
# ------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------
@app.route("/out", methods=["POST"])
@app.route("/out/", methods=["POST"])
def route_out():
    """ signals avavilability of final output """
    datainfo = request.get_json()
    taskname = datainfo['flow_name']
    taskuid =  datainfo['uid']
    sprint(f'Recived output for {taskname}/{taskuid}')
    for o,url in datainfo['outputs'].items(): 
        outfile = os.path.join(DATADIR, url)
        sprint(f'Result {taskname}/{taskuid}.{o}\n{url} @ [{outfile}] [ 🟢 ]')
    return {"received": f"{taskname}/{taskuid}", "data": list(datainfo['outputs'].keys())}, 200
# ------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------
@app.route("/fin", methods=["POST"])
@app.route("/fin/", methods=["POST"])
def route_fin():
    """ fin task
        json={'uid': etaskid, 'output':f'{filename}', }
    """
    fintask = request.get_json()
    sprint(f'Task Finished {fintask}')
    #Kio.SaveJSON(os.path.join(WORKDIR, f'TASKQ_FIN_{fintask["uid"]}.json'), TASKQ)
    return jsonify(fintask), 200
# ------------------------------------------------------------------------------------------


@app.route("/new", methods=["POST"])
@app.route("/new/", methods=["POST"])
def route_new():
    """ admit new task
        dict(
            node_id = ,
            flow_name = ,
            input = , 
        )
    """
    newtask = request.get_json()
    node_id = newtask['node_id']
    flow_name =  newtask['flow_name']
    sprint(f'Admit New Flow {flow_name} from {node_id}')
    newflow = FLOWS[flow_name]

    nodes, locs = newflow.NODES, list(INFRA.keys())
    if locs[0]!=node_id: 
        sprint(f'Error: First node in location list should be the {node_id} instead of {locs[0]}')
        return jsonify(dict(type='mismatch', arg=(locs[0], node_id))), 400
    decision = Decide(nodes, locs) 
    for n in newflow.NODES:
        if n not in decision: return jsonify(dict(type='decision', arg=(n, list(decision.keys())))), 400
        if decision[n] not in locs: return jsonify(dict(type='location', arg=(decision[n], locs))), 400
        if decision[newflow.ENTRY] != node_id: 
            sprint(f'Warning: Setting entry task to offload locally {node_id} instead of {decision[newflow.ENTRY]}')
            decision[newflow.ENTRY] = node_id
    
    offloader = f"{'https' if PROXY_FIX else 'http'}://{parsed.host}:{parsed.port}"
    newflow = Manager.PrepareFlow(
        flow = newflow,
        decision = decision,
        infra = INFRA,
        offloader = offloader,
    )

    offloading_status = Manager.Offload(newflow, decision, INFRA)
    rstatus = Manager.StartFlow(newflow, decision, INFRA, initial_input_name = newtask['input'])

    return jsonify(dict(nodeid=node_id, decision=decision, offloading_status=offloading_status, rstatus=rstatus)), 200
# ------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------
@app.route('/data', methods =['GET', 'POST'], defaults={'req_path': ''})
@app.route('/data/<path:req_path>', methods =['GET', 'POST'],)
def route_data(req_path):
    if not req_path: return abort(404)
    abs_path = ValidatePath(DATADIR, req_path)
    if abs_path is None: return abort(404)
    if request.method=='GET':
        if not os.path.isfile(abs_path): return abort(404)
        sprint(f'downloading from {req_path} via {request.remote_addr}')
        return send_file(abs_path, as_attachment=("?" in request.args)) 
    elif request.method=='POST':
        success=0
        for fk, fv in request.files.items(): 
            try: 
                fv.save(abs_path)
                sprint(f'uploaded at {req_path} via {request.remote_addr}')
                success=1
            except: pass
            break # only one file allowed
        return f'{success}'
    else: return abort(404)
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
# Serve
# ------------------------------------------------------------------------------------------
start_time = datetime.datetime.now()
serve(app, # https://docs.pylonsproject.org/projects/waitress/en/stable/runner.html
    host = parsed.host,          
    port = parsed.port,          
    url_scheme = 'http',     
    threads = parsed.threads,    
    connection_limit = parsed.maxconnect,
    max_request_body_size = str2bytes(parsed.maxupsize) ,
)
end_time = datetime.datetime.now()
#Kio.SaveJSON(os.path.join(WORKDIR, 'TASKQ.json'), TASKQ)
sprint('◉ server up-time was [{}]'.format(end_time - start_time))
sprint(f'...Finished!')
# ------------------------------------------------------------------------------------------
