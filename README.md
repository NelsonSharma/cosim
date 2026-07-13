
# to-do

* State Space bulding - ~~write to json instead of omlog~~
* ~~how much trace to store?~~

* Broadcast tasks - counterfactual - problem - task-id duplicated - notifactions owerwrite
    * added `predecision` to task attributes where decision can be pre defined in the task itself
    * will use this to generate as many tasks as there are nodes then send them all at once.

* Policy implement
* Task-db bulding
* implement as docker image / vm-run ready app - have to include task dependencies too
* ~~Organize logs~~


Broker connections are unstable

```sh


curl http://127.0.0.1:8600?stream=2
curl http://127.0.0.1:8600?send
curl http://127.0.0.1:8600?send=example
curl http://127.0.0.1:8600?send=tester\&exe=main.py
curl http://127.0.0.1:8600?send=tester\&exe=other.py


curl http://127.0.0.1:8600?send=\&cn=CN0
curl http://127.0.0.1:8600?send=example\&cn=CN1
curl http://127.0.0.1:8600?send=tester\&exe=main.py\&cn=CN0
curl http://127.0.0.1:8600?send=tester\&exe=other.py\&cn=CN1



```



```



# python3.12 -m cosim \
# --caller=xxxx \
# --base=xxxx \
# --data=xxxx \
# --host=xxxx \
# --port=xxxx \
# --source=xxxx \
# --maxupsize=xxxx \
# --maxconnect=xxxx \
# --threads=xxxx \
# --om=xxxx \
# --id=xxxx \
# --tasks=xxxx \
# --pyexe=xxxx \
# --cleanup=xxxx \
# --log=xxxx --verbose=xxxx 
# 192.168.122.41:8000

# set UEID CNID OMID 

COSIMDIR="/home/ava/Server/app"
UEID=""
CNID=""
OMID=""


# OM
python3.12 -m cosim \
--caller=OMFS \
--base=base_OM \
--data=base_OM \
--host=0.0.0.0 \
--port=8500 \
--source= \
--maxupsize='10MB' \
--maxconnect=1000 \
--threads=8 \
--limit=0 \
--log=OM.log --verbose=1 &


# CN-0
python3.12 -m cosim \
--caller=CNFS \
--base=base_CN0 \
--data=base_CN0 \
--host=0.0.0.0 \
--port=8600 \
--source= \
--maxupsize='10GB' \
--maxconnect=1000 \
--threads=2 \
--om=0.0.0.0:8500 \
--id=CN0 \
--log=CN0.log --verbose=1 &

# EX-0
python3.12 -m cosim \
--caller=EXFS \
--base=base_CN0 \
--data=base_CN0 \
--host=0.0.0.0 \
--port=8610 \
--source= \
--maxupsize='10GB' \
--maxconnect=1000 \
--threads=1 \
--pyexe=python3.12 \
--cleanup=1 \
--log=EX0.log --verbose=1 &

# UE-0
python3.12 -m cosim \
--caller=UEFS \
--base=base_UE0 \
--data=base_UE0 \
--host=0.0.0.0 \
--port=8700 \
--source= \
--maxupsize='10GB' \
--maxconnect=1000 \
--threads=2 \
--om=0.0.0.0:8500 \
--id=UE0 \
--tasks=db \
--log=UE0.log --verbose=1 &

```