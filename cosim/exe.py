

# ------------------------------------------------------------------------------------------
from sys import exit
if __name__!='__main__': exit(f'[!] can not import {__name__}.{__file__}')
# ------------------------------------------------------------------------------------------
import argparse
# ------------------------------------------------------------------------------------------
argp = argparse.ArgumentParser()

argp.add_argument('--base',             type=str, default='',                help='base directory for app')
argp.add_argument('--pyexe',            type=str, default='python3.12',      help='python executable for CNEXE')
argp.add_argument('--cleanup',          type=int, default=1,                 help='cleanup for CNEXE')
argp.add_argument('--lock',             type=str, default='.lock',           help='lock file')
argp.add_argument('--interval',         type=float, default=0.5,             help='queue check interval')
parsed = argp.parse_args()
# ------------------------------------------------------------------------------------------
import os
import datetime, time, subprocess, shutil
from . import Kio, TaskStatus, FileConfig, UnZipped, OpsConfig
from .fs import Requestor
# ------------------------------------------------------------------------------------------
BASEDIR = os.path.abspath(parsed.base)
if not os.path.isdir(BASEDIR): exit(f'Cannot find base directory at {BASEDIR}')
QUEUEDIR=os.path.join(BASEDIR, OpsConfig.QUEUE_DIR_NAME)
if not os.path.isdir(QUEUEDIR): exit(f'Cannot find queue directory at {QUEUEDIR}')
TRACESDIR=os.path.join(BASEDIR, OpsConfig.TRACES_DIR_NAME)
if not os.path.isdir(TRACESDIR): exit(f'Cannot find traces directory at {TRACESDIR}')
# ------------------------------------------------------------------------------------------


#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def Executor(tid): return LoadExecuteTask(tid, BASEDIR, QUEUEDIR, TRACESDIR, parsed.pyexe, parsed.cleanup)
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def LoadExecuteTask(tid, base, queue, traces, pyexe, cleanup):

    if not (tid): return False    
    metapath = os.path.join(queue, tid)
    if not os.path.isfile(metapath): return False
    meta = Kio.LoadJSON(metapath)
    
    
    # ~META~
    meta[TaskStatus.STARTED] = TaskStatus.TS()
    log, result = ExecuteTask(base, meta, pyexe, cleanup)
    meta['result'] = result 
    meta['log'] = log 
    meta[TaskStatus.FINISH] = TaskStatus.TS()

    tracepath = os.path.join(traces, f'{tid}')
    Kio.SaveJSON(tracepath, meta)
    os.remove(metapath)


    return True
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def ExecuteTask(base, meta, pyexe, cleanup): 
    log, result = [], {}

    # Execute a given task
    name, tid, source, exe = meta['name'], meta['tid'], meta['source'], meta['exe']
    log.append(f'[EXE] 🔘 Executing Task {tid}')

    # create task dir to store code and data
    taskdir = os.path.join(base, tid)
    os.makedirs(taskdir, exist_ok=True)
    
    # Get Input Files
    log.append(f'[EXE] ⚡ Getting input files ')
    zippath = os.path.join(taskdir, FileConfig.ZIP_NAME)
    if not Requestor.Get(source, f"{tid}/{FileConfig.ZIP_NAME}", zippath, chunk_size = None):
        log.append(f'[EXE] ❗ Failed to get inputs')
        return log, result
    
    UnZipped(zippath, taskdir)# unzip in task dir  

    outpath = os.path.join(taskdir, FileConfig.TASK_OUT)
    out = open(outpath, 'wb')
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=        
    # Task Execution Start
    # None                 # show in terminal
    # file handle          # write to file 
    # subprocess.PIPE      # capture in Python
    # subprocess.DEVNULL   # discard
    # subprocess.STDOUT    # merge stderr into stdout
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    log.append(f'[EXE] ⚪ Task Start: {tid}')
    t_start, ts_start = time.perf_counter(), datetime.datetime.now()
    returncode = subprocess.run([pyexe,  os.path.join(taskdir, name, exe),], 
            cwd= os.path.join(taskdir, name), 
            stdout=out, 
            stderr=subprocess.STDOUT, 
            text=False).returncode
    t_end, ts_end = time.perf_counter(), datetime.datetime.now()
    prefs, elapsed = t_end - t_start, ts_end - ts_start
    log.append(f'[EXE] ⚫ Task End: {tid}')
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=        
    # Task Execution End
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    out.close()
    
    # Send Output Files
    log.append(f'[EXE] ⚡ Sending output file ')
    if not Requestor.Post(source, f"{tid}/{FileConfig.TASK_OUT}", outpath): 
        log.append(f'[EXE] ❗ Failed to send outputs')
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    if cleanup: shutil.rmtree(taskdir) # delete input and output files  # ⭕
    result = dict(returncode=returncode,  prefs=prefs,  elapsed=str(elapsed)) # stats dict
    return log, result
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# while Q is not empty, keep executing
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
while os.path.isfile(parsed.lock):
    time.sleep(parsed.interval)
    pending_tids = sorted(os.listdir(QUEUEDIR))
    while pending_tids:
        pending_tid = pending_tids.pop(0)
        res = Executor(pending_tid)
        if not res: print(f'Task [{pending_tid}] cannot be executed, probably not found!')

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
