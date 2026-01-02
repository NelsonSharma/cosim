
import networkx as nx
import matplotlib.pyplot as plt

class Flow(nx.DiGraph):

    def __init__(self, **info): 
        super().__init__()
        self.INFO = {**info}

        assert len(self.INFO) > 2, f"must have atleast three tasks (nodes) - inclding one entry(1) and one exit(-1)"
        for n, d in self.INFO.items(): self.add_node(n, **d)
        self.NODES = list(self.nodes)
        self.ENTRY, self.EXIT = self.NODES[0], self.NODES[-1] # first task is entry and last is exit
        
        for n in self.NODES:
            if n != self.EXIT:
                for o in self.nodes[n]['outputs']:
                    for g in self.NODES:
                        if g != self.ENTRY:
                            if o in self.nodes[g]['inputs']: self.add_edge(n, g, data=o)


        self.LAYERS = tuple(nx.topological_generations(self))
        assert len(self.LAYERS[0])==1, f'There requires only one entry task'
        assert self.LAYERS[0][0]==self.ENTRY, f'First task must be specified as entry task'
        assert len(self.LAYERS[-1])==1, f'There requires only one exit task'
        assert self.LAYERS[-1][0]==self.EXIT, f'Last task must be specified as exit task'
        for layer, nodes in enumerate(self.LAYERS):
            for node in nodes: self.nodes[node]["subset"] = layer
        
    def render(self, layout="spectral_layout", **layoutargs):
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
        edge_labels = nx.get_edge_attributes(self, "data")
        cmap = { 1:"lightgreen", 0:"lightblue", -1:"lightcoral"}
        ln = list(self.nodes())
        flags = [ 1 if n == self.ENTRY else (-1 if n == self.EXIT  else (0)) for n in ln ]
        node_colors = [ cmap[n] for n in flags ]
        pos = getattr(nx, layout)(self, **layoutargs)
        nx.draw(self, pos, with_labels=True, node_color=node_colors)
        nx.draw_networkx_edge_labels(self, pos, edge_labels=edge_labels)
        plt.show()

