
# UE
OM='127.0.0.1:8000'
ID='UE'
CALLID="UEFS"
NAMED="$ID$CALLID"
python3.12 -m cosim \
--caller=$CALLID \
--base=$NAMED \
--data=$NAMED \
--host=0.0.0.0 \
--port=8888 \
--source= \
--maxupsize=10GB \
--maxconnect=1000 \
--threads=8 \
--om=$OM \
--id=$ID \
--tasks=db \
--log=$NAMED.log --verbose=1 &
