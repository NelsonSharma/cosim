
import random, os, subprocess, time, re

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def int2base(num, base, digs):
    """ convert base-10 integer to base-n array of fixed no. of digits 
    return array (of length = digs)"""
    import numpy as np
    from math import floor
    res = np.zeros(digs, dtype=np.int32)
    q = num
    for i in range(digs):
        res[i]=q%base
        q = floor(q/base)
    return res
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def base2int(arr, base):
    """ convert array from given base to base-10  --> return integer"""
    res = 0
    for i in range(len(arr)):
        res+=(base**i)*arr[i]
    return res
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

class Utility:

    @staticmethod
    def Solutions(flow, infra, fix=0): return len(infra.NODES)**(len(flow.NODES)-fix)
        
    @staticmethod
    def Enumerate(flow, infra, fixl=0, fixr=0):
        fixed=fixl+fixr
        S = __class__.Solutions(flow, infra, fixed)
        B = len(infra.NODES)
        N = len(flow.NODES)-fixed
        return [ (  [ infra.NODES[0] for _ in range(fixl) ] + [ infra.NODES[int(i)] for i in int2base(s, B, N) ] + [ infra.NODES[0] for _ in range(fixr) ]  ) for s in range(S) ]

    @staticmethod
    def Cost(flow, infra):

        for n in flow.NODES:
            d = flow.nodes[n]['decision']
            size = flow.nodes[n]['size'] # cycles
            rate = infra.nodes[d]['rate'] # seconds per cycle
            flow.nodes[n]['delay'] = float(size*rate) # seconds
            #flow.nodes[n]['loc'] = (d, di) # seconds

        for e in flow.edges: 
            e1, e2 = e
            d1, d2 = flow.nodes[e1]['decision'], flow.nodes[e2]['decision']
            size = flow.edges[e]['size'] # bits
            rate = infra.edges[d1, d2]['rate'] #  seconds per bit
            flow.edges[e]['delay'] = float(size*rate) 
            flow.edges[e]['weight'] = -1.0 * (flow.edges[e]['delay']  + flow.nodes[e1]['delay'])
            
        import networkx as nx
        path = nx.shortest_path(flow, source=flow.ENTRY, target=flow.EXIT, weight='weight')
        cost = 0.0
        for n in range(len(path)-1): cost+= -1.0*(flow.edges[(path[n], path[n+1])]['weight'])
        return cost, path

class Policy:

    @staticmethod
    def Random(flow, infra):
        tasks, locations = flow.NODES, infra.NODES
        return {t:(locations[0] if (i==0 or i==len(tasks)-1) else random.choice(locations)) for i,t in enumerate(tasks)}
    
    @staticmethod
    def NoOffload(flow, infra): return __class__.FixedNode(flow, infra, 0)

    @staticmethod
    def RandomOffload(flow, infra): 
        tasks, locations = flow.NODES, infra.NODES[1:]
        return {t:(infra.NODES[0] if (i==0 or i==len(tasks)-1) else random.choice(locations)) for i,t in enumerate(tasks)}
    
    @staticmethod
    def FixedNode(flow, infra, node):
        tasks, locations = flow.NODES, infra.NODES
        return {t:(locations[0] if (i==0 or i==len(tasks)-1) else locations[node]) for i,t in enumerate(tasks)}

def Executor(flow, pyexe, codepath, datapath):

    #pyexe = '/home/ava/.pyenv/versions/3.12.11/bin/python'
    codepath = os.path.abspath(codepath)
    datapath = os.path.abspath(datapath)

    dinfo = {k: {kk: 0.0 for kk in flow.nodes[k]['outputs']} for k in flow.NODES}
    for k in flow.NODES: dinfo[k][''] = 0 # blank to specify perf_counter

    # initialize
    taskQ = {k:[*flow.nodes[k]['inputs']] for k in flow.NODES}

    # trigger
    taskQ[flow.ENTRY].remove(flow.nodes[flow.ENTRY]['inputs'][0])

    # check which tasks can be run
    while taskQ:
        canrun=[]
        for k,v in taskQ.items():
            if not v: canrun.append(k)
        for taskname in canrun:
            t_start = time.perf_counter()
            result = subprocess.run(
                [
                    "perf", "stat", "-e", "cycles,instructions",
                    f'{pyexe}', 
                    os.path.join(codepath, f'{taskname}.py'),
                ], cwd=datapath, stderr=subprocess.PIPE, text=True)
            prefs = time.perf_counter() - t_start

            perf_output = result.stderr
            # Regex patterns
            cycles_match = re.search(r'([\d,]+)\s+cycles', perf_output)
            instr_match  = re.search(r'([\d,]+)\s+instructions', perf_output)
            elapsed_match = re.search(r'([\d.]+)\s+seconds time elapsed', perf_output)

            cycles = int(cycles_match.group(1).replace(',', '')) if cycles_match else None
            instructions = int(instr_match.group(1).replace(',', '')) if instr_match else None
            elapsed = float(elapsed_match.group(1)) if elapsed_match else None

            dinfo[taskname][''] = (cycles, instructions, elapsed, prefs)
            datasent = []
            for e1,e2 in flow.out_edges(taskname): 
                assert taskname == e1
                dfname = flow.edges[e1,e2]['data']
                dfsize = os.path.getsize(os.path.join(datapath, dfname))
                dinfo[e1][dfname] = dfsize
                datasent.append(dfsize)
                taskQ[e2].remove(dfname)

            del taskQ[taskname]
            print(f'Task {taskname}: {cycles=}, {instructions=}, {elapsed=}, {prefs=}, {datasent=}, bytes={sum(datasent)}')
            
            

        if not canrun: break

    #outpath = os.path.join(datapath, flow.nodes[flow.EXIT]['outputs'][0])
    return  dinfo
