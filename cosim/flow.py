import networkx as nx
import matplotlib.pyplot as plt

class Flow(nx.DiGraph):

    def __init__(self, name, **info): 
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

