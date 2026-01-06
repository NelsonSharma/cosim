

import os

def List():
    path = os.path.dirname(__file__)
    return [f for f in os.listdir(path) if (os.path.isdir(os.path.join(path, f)) and (not f.startswith("__")) and (not f.endswith("__")))]

def Flows(Creator=None):
    res = {}
    path = os.path.dirname(__file__)
    flowlist = [(f, os.path.join(path, f, '__init__.py')) for f in List()]
    if Creator is None:
        for f, d in flowlist:
            with open(d, 'r') as c: res[f] = eval(c.read())
    else:
        for f, d in flowlist:
            with open(d, 'r') as c: res[f] = Creator(f, **eval(c.read()))
    return res




__doc__="""


# Workflow
 
A workflow is a DAG of Tasks. It must contain at least 3 tasks:

1. Exactly one entry task
    * this is the root of the DAG
    * *must have one input - can have multiple outputs*
    * this would usually be a task that loads the inputs from a user equipment (UE) 
    * by convention this task is always offloaded on the UE

2. Exactly one exit task
    * this is the leaf of the DAG
    * *can have multiple inputs - must have single output*
    * this would usually be a task that aggregates output data of previous tasks 
    * it collects the relevant output data that is required to be sent back the UE that generated this task

3. at least one 'normal task i.e, a task that is neither entry or exit
    * these are intermediate nodes of the DAG
    * *can have multiple inputs and outputs*
    * these consists of the core tasks in the workflow

---

The simplest possible workflow is shown below

```python

    from cosim import Flow

    flow = Flow(
        
        # Entry Task
        E = dict(
            inputs = ('x',),    #<--- must have single input
            outputs = ('y',),
        ),
        
        # Intermediate Task
        I = dict(
            inputs = ('y',),
            outputs = ('z',),
        ),

        # Exit Task
        X = dict(
        inputs = ('z',),
        outputs = ('o',),   #<--- must have single output
        ),
    )

    flow.render(layout='kamada_kawai_layout')
    
```


# Database
 
* The db directory contains multiple workflows each within its own folder containing:
    * the tasks as python scripts
    * the DAG definition in a dict in the `__init__.py` file

"""
