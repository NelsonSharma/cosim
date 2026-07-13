
import random, os, json
def RandomChoice(obj): return obj[random.randint(0, len(obj)-1)]
def LoadJSON(path):
    with open(path, 'r') as f: obj = json.load(f)
    return obj
# ------------------------------------------------------------------------------------------
# init signature is: def __init__(self, queue, states, nodes): 
# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
class BasePolicy:
    def __init__(self, queue, states, nodes): self.queue, self.states, self.nodes = queue, states, nodes
    def __call__(self, task): return ... # return an item from self.nodes for task tid

    @property
    def ListNodes(self): return os.listdir(self.nodes) # success, nodes = Requestor.NodeList(parsed.om)

    def ListStates(self): return [os.listdir(os.path.join(self.states, node)) for node in self.ListNodes]
    def GetTask(self, tid): return LoadJSON(os.path.join(self.queue, tid))

# ------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------
class Random(BasePolicy):
    def __call__(self, task): return RandomChoice(self.ListNodes) 

# ------------------------------------------------------------------------------------------


# ------------------------------------------------------------------------------------------
class Robin(BasePolicy):
    def __init__(self, queue, states, nodes):
        super().__init__(queue, states, nodes)
        self.counter = -1
    def __call__(self, task): 
        nodes = self.ListNodes
        self.counter = (self.counter+1) % len(nodes)  
        return nodes[self.counter]
    
# ------------------------------------------------------------------------------------------

