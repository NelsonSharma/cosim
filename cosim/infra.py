import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
from scipy.sparse.csgraph import floyd_warshall


#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
def EffectiveBandwidth(B):
    """ function for finding shortest path and effective Data Rate (bandwidth) b/w edge or cloud servers 
        return dist_matrix """
    M = np.array(B, dtype='float')
    lm = len(M)
    for i in range(lm):
        for j in range(i+1, lm):
            if M[i,j]!=0: M[i,j] = 1/M[i,j]
            M[j,i] = M[i,j]
    dist_matrix, predecessors = floyd_warshall(csgraph=M, 
                directed=False, return_predecessors=True)
    #for i in range(lm): dist_matrix[i,i] = np.inf
    return dist_matrix #<--- this turns the diagonals to inf  #print(dist_matrix)
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

class Infra(nx.Graph):


    def __init__(self, L, Effective=True):
        super().__init__()
        N = L[0]
        TR = L[-1]
        DR = L[1:-1]
        for i,n in enumerate(N): self.add_node(n, rate=float(TR[i]))
        BW = EffectiveBandwidth(DR) if Effective else DR
        for i,n in enumerate(N): 
            for j,k in enumerate(N): self.add_edge(n,k, rate=float(BW[i,j]))
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



    def render(self, layout="circular_layout", **layoutargs):
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
        edge_labels = nx.get_edge_attributes(self, "rate")
        pos = getattr(nx, layout)(self, **layoutargs)
        nx.draw(self, pos, with_labels=True)
        nx.draw_networkx_edge_labels(self, pos, edge_labels=edge_labels)
        plt.show()


