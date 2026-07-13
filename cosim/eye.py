

# ------------------------------------------------------------------------------------------
from sys import exit
if __name__!='__main__': exit(f'[!] can not import {__name__}.{__file__}')
# ------------------------------------------------------------------------------------------
import argparse
# ------------------------------------------------------------------------------------------
argp = argparse.ArgumentParser()

argp.add_argument('--om',               type=str, default='',                help='om-source ')
argp.add_argument('--id',               type=str, default='',                help='nid ')
argp.add_argument('--queue',               type=str, default='',                help='queue dir ')
argp.add_argument('--interval',         type=float, default=5.0,             help='state send interval')
argp.add_argument('--lock',             type=str, default='',                help='lock file for CNEXE')
parsed = argp.parse_args()
# ------------------------------------------------------------------------------------------
import os, time
from .fs import Requestor
# ------------------------------------------------------------------------------------------

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
while os.path.isfile(parsed.lock):
    time.sleep(parsed.interval)
    success = Requestor.State(parsed.om, parsed.id, sorted(os.listdir(parsed.queue)))

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
