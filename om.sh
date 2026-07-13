
# OM
ID='OM'
CALLID="OMFS"
NAMED="$ID$CALLID"
python3.12 -m cosim \
--caller=$CALLID \
--base=$NAMED \
--data=$NAMED \
--host=0.0.0.0 \
--port=8000 \
--source= \
--maxupsize=10MB \
--maxconnect=1000 \
--threads=8 \
--limit=20 \
--policy_module=custom.py \
--policy_class=Robin \
--lock=$NAMED.lock \
--log=$NAMED.log --verbose=1 &

