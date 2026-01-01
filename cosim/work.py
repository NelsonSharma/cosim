from sys import exit
if __name__!='__main__': exit(f'[!] can not import {__name__}.{__file__}')


# ------------------------------------------------------------------------------------------
# arguments parsing
# ------------------------------------------------------------------------------------------
import argparse, os
argp = argparse.ArgumentParser()
argp.add_argument('--mods',            type=str, default='')
argp.add_argument('--base',             type=str, default='base')
argp.add_argument('--script',            type=str, default='python3')
argp.add_argument('--maxupsize',        type=str, default='500GB',) 
argp.add_argument('--maxconnect',       type=int, default=1000,) 
argp.add_argument('--threads',          type=int, default=1,) 
argp.add_argument('--host',             type=str, default='127.0.0.1')
argp.add_argument('--port',             type=str, default='9000',)
argp.add_argument('--verbose',          type=int, default=1)
argp.add_argument('--log',              type=str, default='log.txt')
argp.add_argument('--secret',           type=str, default='')
argp.add_argument('--https',            type=int, default=0, help='set to 1 if reverse proxy with https is used')
parsed = argp.parse_args()

# ------------------------------------------------------------------------------------------
# imports
# ------------------------------------------------------------------------------------------
import logging
import subprocess, time
import datetime, json, random
#import pika
from sys import exit
PROXY_FIX=bool(parsed.https)
from flask import Flask, request, send_file, abort # redirect, url_for,
if PROXY_FIX: from werkzeug.middleware.proxy_fix import ProxyFix
from waitress import serve 

def VALIDATE_PATH(base, req):
    target = os.path.abspath(os.path.join(base, req))
    rel = os.path.relpath(target, base)
    if rel.startswith(os.pardir + os.sep) or rel == os.pardir: return None
    else: return target

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
#def fnow(format): return datetime.datetime.strftime(datetime.datetime.now(), format)
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


#%% [INITIALIZATION] @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 
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


#%% [APP DEFINE] @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ 
app = Flask(
    __name__,
    static_folder=WORKDIR,      # Set your custom static folder path here
    template_folder=WORKDIR,   # Set your custom templates folder path here
    instance_relative_config = True,
    instance_path = WORKDIR,
)
if PROXY_FIX: app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
app.secret_key = parsed.secret if parsed.secret else f'{random.randint(11111, 99999)}'

TASKQ = {}

def PrintQ():
    sprint("\n\nTask Queue:")
    for uid, info in TASKQ.items():
        sprint(f'Task-ID: {uid}')
        for k,v in info.items():
            sprint(f'\t{k}: {v}')
    sprint('\n')

@app.route("/add", methods=["POST"])
@app.route("/add/", methods=["POST"])
def route_add():
    global TASKQ
    taskinfo = request.get_json()
    taskid = taskinfo['uid']
    sprint(f'Recived Task {taskid}')
    TASKQ[taskid] = {**taskinfo}
    TASKQ[taskid]["inget"] = {} # initialize waiting queue
    
    PrintQ()

    return {"received": taskid}, 200

@app.route("/send", methods=["POST"])
@app.route("/send/", methods=["POST"])
def route_send():
    datainfo = request.get_json()
    taskid = datainfo['uid']
    sprint(f'Recived data for {taskid}')
    global TASKQ
    for o,url in datainfo['outputs'].items(): 
        sprint(f'\t {o}: {url}')
        TASKQ[taskid]["inget"][o] = url
        # curl -o myfile.txt https://example.com/file.txt
        #outname = f'{taskid}_{os.path.basename(url)}'
        #outfile = os.path.join(basei, outname)
        #subprocess.Popen(["curl" "-o" f"{outfile}" f'{url}'])

    # check which task can be launched
    launched = []
    for uid, taskinfo in TASKQ.items():
        #sprint(f'\n🟢 check {uid}, {taskinfo}\n')
        can_launch = not (False in [ i in taskinfo["inget"] for i in taskinfo['inputs'] ])
        if can_launch: 
            taskpath = os.path.join(TASKDIR, f'{uid}.json')
            with open(taskpath, 'w') as f: json.dump(taskinfo, f)
            sprint(f'Starting task {uid} using {taskpath}')
            subprocess.Popen([f"{EXESCRIPT}", "-m", "cosim.run", "--mods", f'{parsed.mods}', "--base", f"{WORKDIR}", "--info", f'{taskpath}', "--point", f"{'https' if PROXY_FIX else 'http' }://{parsed.host}:{parsed.port}" ])
            launched.append(uid)
    if launched:
        sprint(f'Removing lanched {launched}')
        for uid in launched: del TASKQ[uid]
    
    PrintQ()
    return {"received": taskid, "data": list(datainfo['outputs'].keys())}, 200



@app.route("/out", methods=["POST"])
@app.route("/out/", methods=["POST"])
def route_out():
    datainfo = request.get_json()
    taskid = datainfo['uid'][:-1]
    sprint(f'Recived output for {taskid}')
    for o,url in datainfo['outputs'].items(): 
        sprint(f'\t {o}: {url}')
        # curl -o myfile.txt https://example.com/file.txt
        #outname = f'{taskid}_{os.path.basename(url)}'
        outfile = os.path.join(DATADIR, o)
        result = subprocess.run(["curl", "-o", f"{outfile}", f'{url}'],capture_output=True, text=True) 
        sprint(f' ⬇️ [{result.returncode}] \n{result.stdout}')

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
                success=1
            except: pass
            break # only one file allowed
        return f'{success}'
    else: return abort(404)


# @app.route("/exe", methods=["POST"])
# @app.route("/exe/", methods=["POST"])
# def route_exe():
#     Px = subprocess.Popen([f"{EXESCRIPT}", "--info", f'"{json.dumps(request.get_json())}"'])
#     return {"received": Px.pid}, 200

# ------------------------------------------------------------------------------------------
def str2bytes(size):
    sizes = dict(KB=2**10, MB=2**20, GB=2**30, TB=2**40)
    return int(float(size[:-2])*sizes.get(size[-2:].upper(), 0))
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