
# CN
OM='127.0.0.1:8000'
ID='CN0'
CALLID="CNFS"
NAMED="$ID$CALLID"
python3.12 -m cosim \
--caller=$CALLID \
--base=$NAMED \
--data=$NAMED \
--host=0.0.0.0 \
--port=8800 \
--source= \
--maxupsize=10GB \
--maxconnect=1000 \
--threads=4 \
--om=$OM \
--id=$ID \
--cleanup=1 \
--state_interval=3.0 \
--queue_interval=0.5 \
--lock=$NAMED.lock \
--log=$NAMED.log --verbose=1 &
