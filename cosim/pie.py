

# ------------------------------------------------------------------------------------------
from sys import exit
if __name__!='__main__': exit(f'[!] can not import {__name__}.{__file__}')
# ------------------------------------------------------------------------------------------
import argparse
# ------------------------------------------------------------------------------------------
argp = argparse.ArgumentParser()

argp.add_argument('--base',             type=str, default='',                help='base dir for OM')
argp.add_argument('--om',               type=str, default='',                help='source for om')
argp.add_argument('--lock',             type=str, default='.lock',           help='policy lock file')
argp.add_argument('--interval',         type=float, default=0.5,             help='queue check interval')
argp.add_argument('--policy_module',           type=str, default='',                help='policy module to use')
argp.add_argument('--policy_class',            type=str, default='',                help='policy class to use')
parsed = argp.parse_args()
# ------------------------------------------------------------------------------------------
import os
import time
from . import Kio, OpsConfig, ImportCustomModule
from .fs import Requestor
# ------------------------------------------------------------------------------------------
BASEDIR = os.path.abspath(parsed.base)
if not os.path.isdir(BASEDIR): exit(f'Cannot find base directory at {BASEDIR}')
QUEUEDIR=os.path.join(BASEDIR, OpsConfig.QUEUE_DIR_NAME)
if not os.path.isdir(QUEUEDIR): exit(f'Cannot find queue directory at {QUEUEDIR}')
STATESDIR=os.path.join(BASEDIR, OpsConfig.STATES_DIR_NAME)
if not os.path.isdir(STATESDIR): exit(f'Cannot find states directory at {STATESDIR}')
NODESDIR=os.path.join(BASEDIR, OpsConfig.NODES_DIR_NAME)
if not os.path.isdir(NODESDIR): exit(f'Cannot find nodes directory at {NODESDIR}')
# ------------------------------------------------------------------------------------------


if not os.path.isfile(parsed.policy_module): exit(f'Not a file - {parsed.policy_module}')
Policy, failed = ImportCustomModule(parsed.policy_module, parsed.policy_class, True, queue=QUEUEDIR, states=STATESDIR, nodes=NODESDIR)
if failed: exit(f'Failed to Load Policy from {parsed.policy_module}/{parsed.policy_class}')
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# while Q is not empty, keep offloading
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
while os.path.isfile(parsed.lock):
    time.sleep(parsed.interval)
    
    # check if nodes are available
    nodes = os.listdir(NODESDIR)
    if not nodes: continue
    else:
        pending_tids = sorted(os.listdir(QUEUEDIR))
        while pending_tids: 
            tid = pending_tids.pop(0)
            
            # load task
            task = Kio.LoadJSON(os.path.join(QUEUEDIR, tid))
            if task['tid'] != tid: continue # mismatch task-information, skip

            # check requiested node
            requested_node = task['requested_node']
            decision = requested_node if (bool(requested_node) and (requested_node in nodes)) else Policy(task)
            if decision: 
                success = Requestor.Offload(parsed.om, tid, decision)
                if not success: print(f'Failed to offload {tid}')
            else: print(f'Failed to take decision for {tid}')
        

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=


