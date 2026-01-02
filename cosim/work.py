from sys import exit
if __name__!='__main__': exit(f'[!] can not import {__name__}.{__file__}')


# ------------------------------------------------------------------------------------------
# arguments parsing
# ------------------------------------------------------------------------------------------
import argparse, os
argp = argparse.ArgumentParser()

argp.add_argument('--mods',            type=str, default='',        help='location of external modules (for custom tasks)')
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
parsed = argp.parse_args()

# ------------------------------------------------------------------------------------------
# imports
# ------------------------------------------------------------------------------------------

import logging, subprocess, time, datetime, json, random
from sys import exit
from flask import Flask, request, send_file, abort # redirect, url_for,
from waitress import serve 

PROXY_FIX=bool(parsed.https)
if PROXY_FIX: from werkzeug.middleware.proxy_fix import ProxyFix

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
EXESCRIPT=os.path.abspath(EXESCRIPT)
if not os.path.isfile(EXESCRIPT): fexit(f'[!] Tasks executor was not found')
sprint(f'↪ Tasks executor is {EXESCRIPT}')

# ------------------------------------------------------------------------------------------
# Globals
# ------------------------------------------------------------------------------------------

TASKQ = {} # maintains a dict of pending tasks
def PrintQ():
    sprint("\n\nTask Queue:")
    for uid, info in TASKQ.items():
        sprint(f'Task-ID: {uid}')
        for k,v in info.items(): sprint(f'\t{k}: {v}')

def VALIDATE_PATH(base, req):
    target = os.path.abspath(os.path.join(base, req))
    rel = os.path.relpath(target, base)
    if rel.startswith(os.pardir + os.sep) or rel == os.pardir: return None
    else: return target

def str2bytes(size):
    sizes = dict(KB=2**10, MB=2**20, GB=2**30, TB=2**40)
    return int(float(size[:-2])*sizes.get(size[-2:].upper(), 0))

# ------------------------------------------------------------------------------------------
# Flask App Define
# ------------------------------------------------------------------------------------------

app = Flask(
    __name__,
    static_folder=WORKDIR,      # Set your custom static folder path here
    template_folder=WORKDIR,   # Set your custom templates folder path here
    instance_relative_config = True,
    instance_path = WORKDIR,
)
if PROXY_FIX: app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
app.secret_key = parsed.secret if parsed.secret else f'{random.randint(11111, 99999)}'


# ------------------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------------------

@app.route("/add", methods=["POST"])
@app.route("/add/", methods=["POST"])
def route_add():
    """ adds task to device's TASKQ """
    global TASKQ
    taskinfo = request.get_json()
    taskid = taskinfo['uid']
    sprint(f'Recived Task {taskid}')
    TASKQ[taskid] = {**taskinfo}
    TASKQ[taskid]["inget"] = {} # initialize waiting queue
    return {"received": taskid}, 200

@app.route("/note", methods=["POST"])
@app.route("/note/", methods=["POST"])
def route_note():
    """ signals availablibity of outputs/inputs """
    datainfo = request.get_json()
    taskid = datainfo['uid']
    sprint(f'Recived data for {taskid}')
    global TASKQ
    for o,url in datainfo['outputs'].items(): TASKQ[taskid]["inget"][o] = url
    launched = [] # check which task can be launched
    for uid, taskinfo in TASKQ.items():
        can_launch = not (False in [ i in taskinfo["inget"] for i in taskinfo['inputs'] ])
        if can_launch: 
            taskpath = os.path.join(TASKDIR, f'{uid}.json')
            with open(taskpath, 'w') as f: json.dump(taskinfo, f)
            sprint(f'Starting task {uid} using {taskpath}')
            subprocess.Popen([
                f"{EXESCRIPT}", "-m", "cosim.run", 
                "--mods", f'{parsed.mods}', 
                "--base", f"{WORKDIR}", 
                "--info", f'{taskpath}', 
                "--log", f'{uid}.log',
                ])
            launched.append(uid)
    if launched:
        for uid in launched: del TASKQ[uid]
    return {"received": taskid, "data": list(datainfo['outputs'].keys())}, 200

@app.route("/out", methods=["POST"])
@app.route("/out/", methods=["POST"])
def route_out():
    """ signals avavilability of final output """
    datainfo = request.get_json()
    taskid = datainfo['uid'][:-1]
    sprint(f'Recived output for {taskid}')
    for o,url in datainfo['outputs'].items(): 
        outfile = os.path.join(DATADIR, url)
        #result = subprocess.run(["curl", "-o", f"{outfile}", f'{url}'],capture_output=True, text=True) 
        sprint(f'Result {taskid}.{o}\n{url} ⬇️ [{outfile}]')
    return {"received": taskid, "data": list(datainfo['outputs'].keys())}, 200

@app.route('/data', methods =['GET', 'POST'], defaults={'req_path': ''})
@app.route('/data/<path:req_path>', methods =['GET', 'POST'],)
def route_data(req_path):
    if not req_path: return abort(404)
    abs_path = VALIDATE_PATH(DATADIR, req_path)
    if abs_path is None: return abort(404)
    if request.method=='GET':
        if not os.path.isfile(abs_path): return abort(404)
        #if ("html" in request.args): 
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

sprint('◉ server up-time was [{}]'.format(end_time - start_time))
sprint(f'...Finished!')
# ------------------------------------------------------------------------------------------
