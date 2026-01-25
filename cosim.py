# ------------------------------------------------------------------------------------------
import random, os, subprocess, time, re, json, pickle, datetime, math
import networkx as nx
import matplotlib.pyplot as plt
# ------------------------------------------------------------------------------------------

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def int2base(num, base, digs):
    """ convert base-10 integer to base-n array of fixed no. of digits 
    return array (of length = digs)"""    
    res = [0 for _ in range(digs)]
    q = num
    for i in range(digs):
        res[i]=q%base
        q = math.floor(q/base)
    return res
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def base2int(arr, base):
    """ convert array from given base to base-10  --> return integer"""
    res = 0
    for i in range(len(arr)):
        res+=(base**i)*arr[i]
    return res
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def timestamp(start='', sep='', end=''): return (start + datetime.datetime.strftime(datetime.datetime.now(), sep.join(["%Y", "%m", "%d", "%H", "%M", "%S", "%f"])) + end)
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Kio:
    
    @staticmethod
    def LoadJSON(path):
        with open(path, 'r') as f: obj = json.load(f)
        return obj

    @staticmethod
    def SaveJSON(path, obj):
        with open(path, 'w') as f: json.dump(obj, f, indent=4)

    @staticmethod
    def LoadPICK(path):
        with open(path, 'rb') as f: obj = pickle.load(f)
        return obj

    @staticmethod
    def SavePICK(path, obj):
        with open(path, 'wb') as f: pickle.dump(obj, f)
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Flow(nx.DiGraph):

    def __init__(self, name, info): 
        assert len(info) > 2, f"must have atleast three tasks (nodes) - inclding one entry(1) and one exit(-1)"
        super().__init__()
        self.NAME = name
        self.INFO = {**info}
        for n, d in self.INFO.items(): self.add_node(n, **d)
        self.NODES = list(self.nodes)
        self.ENTRY, self.EXIT = self.NODES[0], self.NODES[-1] # first task is entry and last is exit
        self.NAME = name
        self.DATA = set()
        for n in self.NODES:
            self.nodes[n]['size'] = 0.0 # task size
            self.nodes[n]['decision'] = None # execution location (decision)
            #for x in self.nodes[n]['outputs']: self.DATA.add(x)
            if n != self.EXIT:
                for o in self.nodes[n]['outputs']:
                    self.DATA.add(o)
                    for g in self.NODES:
                        if g != self.ENTRY:
                            if o in self.nodes[g]['inputs']: 
                                assert not self.has_edge(n,g), f'Cannot add edge {(n,g)} - already exists. All outputs must be added to single out-going edge'
                                self.add_edge(n, g, data=o, size=0.0) # data size

        self.LAYERS = tuple(nx.topological_generations(self))
        assert len(self.LAYERS[0])==1, f'There requires only one entry task'
        assert self.LAYERS[0][0]==self.ENTRY, f'First task must be specified as entry task'
        assert len(self.LAYERS[-1])==1, f'There requires only one exit task'
        assert self.LAYERS[-1][0]==self.EXIT, f'Last task must be specified as exit task'
        for layer, nodes in enumerate(self.LAYERS):
            for node in nodes: self.nodes[node]["subset"] = layer

    def __str__(self): return '\n'.join([ f'Node {n}:{self.nodes[n]}' for n in self.nodes] + [ f'Edge {e}:{self.edges[e]}' for e in self.edges]) 

    def __mul__(self, other): # set (nodes) task size
        if isinstance(other, dict): 
            for n,v in other.items(): self.nodes[n]['size'] = float(v)
        elif isinstance(other, (list, tuple)): 
            assert len(other) == len(self.NODES)
            for n,v in zip(self.NODES, other): self.nodes[n]['size'] = float(v)
        elif isinstance(other, (int, float)):
            for n in self.NODES: self.nodes[n]['size'] = float(other)
        else: raise ValueError
        return self

    def __truediv__(self, other): # set (edges) data size
        if isinstance(other, dict): 
            for e,v in other.items(): self.edges[e]['size'] = float(v)
        elif isinstance(other, (list, tuple)): 
            assert len(other) == len(self.edges)
            for e,v in zip(self.edges, other): self.edges[e]['size'] = float(v)
        elif isinstance(other, (int, float)):
            for e in self.edges: self.edges[e]['size'] = float(other)
        else: raise ValueError
        return self

    def __add__(self, other): # set (nodes) decision
        if isinstance(other, dict): 
            for n,v in other.items(): self.nodes[n]['decision'] = str(v)
        elif isinstance(other, (list, tuple)): 
            assert len(other) == len(self.NODES)
            for n,v in zip(self.NODES, other): self.nodes[n]['decision'] = str(v)
        elif isinstance(other, str):
            for n in self.NODES: self.nodes[n]['decision'] = other
        else: raise ValueError
        return self
  
    def render(self, drawargs=None, edge_labels=True, figsize=None, layout="multipartite_layout", **layoutargs):
        """
        spring_layout
        circular_layout
        shell_layout
        kamada_kawai_layout
        multipartite_layout
        bipartite_layout
        planar_layout
        spectral_layout
        random_layout
        """
        plt.figure(figsize=figsize)
        cmap = { 1:"lightgreen", 0:"lightblue", -1:"lightcoral"}
        ln = list(self.nodes())
        flags = [ 1 if n == self.ENTRY else (-1 if n == self.EXIT  else (0)) for n in ln ]
        node_colors = [ cmap[n] for n in flags ]
        pos = getattr(nx, layout)(self, **layoutargs)
        if not drawargs: drawargs={}
        nx.draw(self, pos, with_labels=True, node_color=node_colors, **drawargs)
        if edge_labels: nx.draw_networkx_edge_labels(self, pos, edge_labels= nx.get_edge_attributes(self, "data"))
        plt.show()
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Infra(nx.Graph):

    def __init__(self, info):
        super().__init__()
        self.INFO = info
        N = info[0]        # first row is names of locations
        TR = info[-1]      # last row is task rate at locations
        DR = info[1:-1]    # middle part is data rates - device per row
        for i,n in enumerate(N): self.add_node(n, rate=float(TR[i]))
        for i,n in enumerate(N): 
            for j in range(i, len(N)): self.add_edge(n, N[j], rate=float(DR[i][j]))
            #for j,k in enumerate(N): self.add_edge(n,k, rate=float(DR[i,j]))
        self.NODES = list(self.nodes)

    def __str__(self): return '\n'.join([ f'Node {n}:{self.nodes[n]}' for n in self.nodes] + [ f'Edge {e}:{self.edges[e]}' for e in self.edges]) 

    def __mul__(self, other): # set (nodes) task rates
        if isinstance(other, dict): 
            for n,v in other.items(): self.nodes[n]['rate'] = float(v)
        elif isinstance(other, (list, tuple)): 
            assert len(other) == len(self.NODES)
            for n,v in zip(self.NODES, other): self.nodes[n]['rate'] = float(v)
        elif isinstance(other, (int, float)):
            for n in self.NODES: self.nodes[n]['rate'] = float(other)
        else: raise ValueError
        return self

    def __truediv__(self, other): # set (edges) data rates
        if isinstance(other, dict): 
            for e,v in other.items(): self.edges[e]['rate'] = float(v)
        elif isinstance(other, (list, tuple)): 
            assert len(other) == len(self.edges)
            for e,v in zip(self.edges, other): self.edges[e]['rate'] = float(v)
        elif isinstance(other, (int, float)):
            for e in self.edges: self.edges[e]['rate'] = float(other)
        else: raise ValueError
        return self

    def render(self, figsize=None, layout="circular_layout", **layoutargs):
        """
        spring_layout
        circular_layout
        shell_layout
        kamada_kawai_layout
        multipartite_layout
        bipartite_layout
        planar_layout
        spectral_layout
        random_layout
        """
        plt.figure(figsize=figsize)
        edge_labels = nx.get_edge_attributes(self, "rate")
        pos = getattr(nx, layout)(self, **layoutargs)
        nx.draw(self, pos, with_labels=True)
        nx.draw_networkx_edge_labels(self, pos, edge_labels=edge_labels)
        plt.show()
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Solutions:

    @staticmethod
    def Count(flow, infra, fix=0): return len(infra.NODES)**(len(flow.NODES)-fix)

    @staticmethod
    def Solution(s, flow, infra, fixl=0, fixr=0, fixi=0):
        B = len(infra.NODES)
        N = len(flow.NODES)-(fixl+fixr)
        return    [ infra.NODES[fixi] for _ in range(fixl) ] + [ infra.NODES[int(i)] for i in int2base(s, B, N) ] + [ infra.NODES[fixi] for _ in range(fixr) ]  
    
    @staticmethod
    def Random(flow, infra, fixl=0, fixr=0, fixi=0): return __class__.Solution(random.randint(0, __class__.Count(flow, infra, fixl+fixr)-1), flow, infra, fixl=fixl, fixr=fixr, fixi=fixi)

    @staticmethod
    def Fixed(n, flow, infra, fixl=0, fixr=0, fixi=0):
        N = len(flow.NODES)-(fixl+fixr)
        return    [ infra.NODES[fixi] for _ in range(fixl) ] + [ infra.NODES[int(n)] for _ in range(N) ] + [ infra.NODES[fixi] for _ in range(fixr) ]  
    
    @staticmethod
    def Enumerate(flow, infra, fixl=0, fixr=0, fixi=0):
        S = __class__.Count(flow, infra, (fixl+fixr))
        B = len(infra.NODES)
        N = len(flow.NODES)-(fixl+fixr)
        return [ (  [ infra.NODES[fixi] for _ in range(fixl) ] + [ infra.NODES[int(i)] for i in int2base(s, B, N) ] + [ infra.NODES[fixi] for _ in range(fixr) ]  ) for s in range(S) ]
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def Cost(flow, infra):
    # Note: ignores the final output task execution and data transfer
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
    
    for e in flow.in_edges(flow.EXIT):
        flow.edges[e]['weight'] += (-1.0 * (flow.nodes[flow.EXIT]['delay']))

    path = nx.shortest_path(flow, source=flow.ENTRY, target=flow.EXIT, weight='weight')
    cost = 0.0
    for n in range(len(path)-1): cost+= -1.0*(flow.edges[(path[n], path[n+1])]['weight'])
    return cost, path
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def Executor(flow, codepath, datapath, pyexe='python3', verbose=False):

    codepath = os.path.abspath(codepath)
    datapath = os.path.abspath(datapath)
    
    if verbose: 
        print(f'Executor start\n\t{codepath=}\n\t{datapath=}')
        print(f'Flow:\n{flow}')

    einfo = {k: {'outputs':{kk: 0.0 for kk in flow.nodes[k]['outputs']}} for k in flow.NODES}
    tsize = {k: None for k in flow.NODES}
    dsize = {k: None for k in flow.edges}

    # initialize
    taskQ = {k:[*flow.nodes[k]['inputs']] for k in flow.NODES}

    # trigger
    taskQ[flow.ENTRY].remove(flow.nodes[flow.ENTRY]['inputs'][0])

    # check which tasks can be run
    while taskQ:

        if verbose: print(f'\nTask in Q: {len(taskQ)}')

        # collect tasks that can be run
        canrun=[]
        for k,v in taskQ.items():
            if not v: canrun.append(k)
        if verbose: print(f'Can Run {len(canrun)} tasks: {canrun}')
        if not canrun: break

        # execute each task
        for taskname in canrun:
            
            if verbose: print(f'Task start: {taskname}')

            # Task Execution
            #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
            t_start = time.perf_counter()
            #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
            result = subprocess.run(
                [
                    "perf", "stat", "-e", "cycles,instructions",
                    f'{pyexe}', 
                    os.path.join(codepath, f'{taskname}.py'),
                ], cwd=datapath, stderr=subprocess.PIPE, text=True)
            #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
            prefs = time.perf_counter() - t_start
            #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

            if verbose: print(f'Task End: {taskname}')

            # Remove from TaskQ
            #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
            del taskQ[taskname]
            #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

            # process results
            #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
            perf_output = result.stderr
            cycles_match = re.search(r'([\d,]+)\s+cycles', perf_output)
            instr_match  = re.search(r'([\d,]+)\s+instructions', perf_output)
            elapsed_match = re.search(r'([\d.]+)\s+seconds time elapsed', perf_output)
            cycles = int(cycles_match.group(1).replace(',', '')) if cycles_match else None
            instructions = int(instr_match.group(1).replace(',', '')) if instr_match else None
            elapsed = float(elapsed_match.group(1)) if elapsed_match else None
            exeinfo = dict(cycles=cycles, instructions=instructions, elapsed=elapsed, prefs=prefs)
            einfo[taskname].update(exeinfo)
            tsize[taskname] = cycles
            if verbose:
                for k,v in exeinfo.items(): print(f'\t{k}: {v}')
            #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

            # process outputs
            #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
            for e1,e2 in flow.out_edges(taskname): 
                assert taskname == e1
                dfname = flow.edges[e1,e2]['data']
                if verbose: print(f'\tProcessing task output: {dfname}')
                dfsize = os.path.getsize(os.path.join(datapath, dfname))
                einfo[e1]['outputs'][dfname] = dfsize
                dsize[(e1,e2)]=dfsize
                taskQ[e2].remove(dfname)
            #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
            

    # optinal - add final task output data
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    outname = flow.nodes[flow.EXIT]['outputs'][0]
    if verbose: print(f'\tProcessing final output: {outname}')
    outpath = os.path.join(datapath, outname)
    outsize = os.path.getsize(outpath)
    einfo[flow.EXIT]['outputs'][outname] = outsize
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    if verbose: print(f'\nExecutor Finished, output @ {outpath}')
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    return  dict(exe=einfo, task=tsize, data=dsize)
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

# ------------------------------------------------------------------------------------------
# Author: Nelson.S
# ------------------------------------------------------------------------------------------