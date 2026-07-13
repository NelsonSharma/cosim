# ------------------------------------------------------------------------------------------
from sys import exit
if __name__!='__main__': exit(f'[!] can not import {__name__}.{__file__}')
# ------------------------------------------------------------------------------------------
import argparse
# ------------------------------------------------------------------------------------------
argp = argparse.ArgumentParser()

argp.add_argument('--base',             type=str, default='',                help='base directory for app')
argp.add_argument('--data',             type=str, default='',                help='data directory for app')
argp.add_argument('--host',             type=str, default='127.0.0.1',       help='waitress server-interface, keep 0.0.0.0 to serve on all interfaces')
argp.add_argument('--port',             type=str, default='8080',            help='waitress server-port')
argp.add_argument('--source',           type=str, default='',                help='enpoint for file transfer - leave blank for same as host port')
argp.add_argument('--maxupsize',        type=str, default='1MB',             help='http_body_size for waitress') 
argp.add_argument('--maxconnect',       type=int, default=1000,              help='maximum number of connections allowed') 
argp.add_argument('--threads',          type=int, default=8,                 help='waitress thread count') 

argp.add_argument('--log',              type=str, default='',                help='specify a file or keep blank for no logging')
argp.add_argument('--verbose',          type=int, default=0,                 help='set to 0 for no verbose')

argp.add_argument('--caller',           type=str, default='',                help='UEFS, OMFS, CNFS, CNEXE')
argp.add_argument('--limit',            type=int, default=0,                 help='state history limit for OMFS, set to false for no limit')
argp.add_argument('--om',               type=str, default='',                help='om-source for CNFS, UEFS')
argp.add_argument('--id',               type=str, default='',                help='nid for CNFS or uid for UEFS')
argp.add_argument('--tasks',            type=str, default='',                help='task directory UEFS')


argp.add_argument('--pyexe',            type=str, default='python3.12',      help='python executable for CNEXE')
argp.add_argument('--cleanup',          type=int, default=1,                 help='cleanup for CNEXE')
argp.add_argument('--lock',             type=str, default='.lock',           help='lock file for CNEXE')
argp.add_argument('--state_interval',   type=float, default=5.0,             help='state send interval')
argp.add_argument('--queue_interval',   type=float, default=0.5,             help='queue check interval')
argp.add_argument('--policy_interval',  type=float, default=0.5,             help='policy check interval')
argp.add_argument('--policy_module',           type=str, default='',                help='policy module to use')
argp.add_argument('--policy_class',            type=str, default='',                help='policy class to use')
parsed = argp.parse_args()
# ------------------------------------------------------------------------------------------






# ------------------------------------------------------------------------------------------
# Init
# ------------------------------------------------------------------------------------------
if not parsed.caller: exit(f'Caller not specified, use --caller')
from .fs import Requestor
from . core import FS # UEFS, OMFS, CNFS
if not hasattr(FS, parsed.caller): exit(f'Caller [{parsed.caller}] not found')
# ------------------------------------------------------------------------------------------
import logging, subprocess, os, time
from . import SetupLogging, FileConfig
sprint = SetupLogging(logging, parsed.log, parsed.verbose)
LOCKER = os.path.abspath(f'{parsed.lock}')
assert not os.path.isdir(LOCKER)
POCKER = os.path.abspath(f'pie{parsed.lock}')
assert not os.path.isdir(POCKER)
# ------------------------------------------------------------------------------------------
FSargs = dict(
    name=__name__, 
    base=parsed.base, 
    data=parsed.data,                     
    printer=sprint, 
    source=parsed.source, 
    host=parsed.host, 
    port=parsed.port, 
    threads=parsed.threads, 
    connection_limit=parsed.maxconnect, 
    max_request_body_size=parsed.maxupsize,

    limit = parsed.limit, 
    om = parsed.om,
    id = parsed.id,
    tasks = parsed.tasks,
)
# ------------------------------------------------------------------------------------------
# Create FS instance
# ------------------------------------------------------------------------------------------
FScaller = getattr(FS, parsed.caller)
FSinstance = FScaller(**FSargs)
# ------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------
#COMMON: name, base, data, printer, source, host, port, threads, connection_limit, max_request_body_size
#OM limit
#CN om, id,
#UE om, id, tasks
 # ------------------------------------------------------------------------------------------


















# ------------------------------------------------------------------------------------------
# Pre Service
# ------------------------------------------------------------------------------------------
if FScaller is FS.CNFS:    
    if not Requestor.NodeJoin(FSinstance.om, FSinstance.nid, FSinstance.source): exit(f'Could not join OM at {FSinstance.om}')
    with open(LOCKER, "w") as f: f.write("_")
    # node -sends system state at regular intervals to OM
    subprocess.Popen([
        parsed.pyexe, "-m", f"{FileConfig.EYE_NAME}", 
        f"--om={FSinstance.om}", 
        f"--id={FSinstance.nid}", 
        f"--queue={FSinstance.queue}",
        f"--interval={parsed.state_interval}",
        f"--lock={LOCKER}",
        ])
    # node - executes available tasks in queue
    subprocess.Popen([
        parsed.pyexe, "-m", f"{FileConfig.EXE_NAME}", 
        f"--base={FSinstance.base}", 
        f"--pyexe={parsed.pyexe}", 
        f"--cleanup={parsed.cleanup}",
        f"--interval={parsed.queue_interval}",
        f"--lock={LOCKER}",
        ])

    time.sleep(max(parsed.state_interval, parsed.queue_interval)+0.5)
        
elif FScaller is FS.OMFS:
    with open(POCKER, "w") as f: f.write("_")
    # policy - offloads available tasks in queue
    subprocess.Popen([
        parsed.pyexe, "-m", f"{FileConfig.PIE_NAME}", 
        f"--om={FSinstance.source}", 
        f"--base={FSinstance.base}", 
        f"--policy_module={parsed.policy_module}", 
        f"--policy_class={parsed.policy_class}", 
        f"--interval={parsed.policy_interval}",
        f"--lock={POCKER}",
        ]) 
else: pass
# ------------------------------------------------------------------------------------------





# ------------------------------------------------------------------------------------------
# Start Service
# ------------------------------------------------------------------------------------------
import signal
signal.signal(signal.SIGINT, signal.default_int_handler) # needed when launched with &
uptime = FSinstance()
sprint(f'Uptime was {uptime}')
# ------------------------------------------------------------------------------------------
# End Service
# ------------------------------------------------------------------------------------------




# ------------------------------------------------------------------------------------------
# Post Service
# ------------------------------------------------------------------------------------------
if FScaller is FS.CNFS:    
    os.remove(LOCKER)
    time.sleep(max(parsed.state_interval, parsed.queue_interval)+0.5)
    if not Requestor.NodeLeave(FSinstance.om, FSinstance.nid): print(f'Could not leave OM at {FSinstance.om}')
elif FScaller is FS.OMFS:
    os.remove(POCKER)
    time.sleep(parsed.policy_interval+0.5)
else: pass

# ------------------------------------------------------------------------------------------


