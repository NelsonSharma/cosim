
from .basic import Kio, now
import os, shutil, requests, numpy as np

class FlowGen:

    def __init__(self, node_id, datadir, flowdir, seed=None):
        self.node_id = node_id
        self.datadir = os.path.abspath(datadir)
        self.flowdir = os.path.abspath(flowdir)
        self.flowpaths = [os.path.join(self.flowdir, fi) for fi in os.listdir(self.flowdir) if os.path.isdir(os.path.join(self.flowdir, fi))] 
        self.rng = np.random.default_rng(seed=seed)

    def generate_flow(self, offloaderURL):
        choosen_path = self.rng.choice(self.flowpaths)
        choosen_info = Kio.LoadJSON(os.path.join(choosen_path, "info.json"))
        first_input = choosen_info[list(choosen_info.keys())[0]]['inputs'][0]
        inputs_path = os.path.join(choosen_path, "inputs")
        choosen_inputs = os.path.join(inputs_path,self.rng.choice(os.listdir(inputs_path)))
        choosen_filename = now(start=self.node_id, sep="_", end=first_input)
        shutil.copyfile(choosen_inputs, os.path.join(self.datadir, choosen_filename))
        choosen = dict(
            node = self.node_id,
            info = choosen_info,
            input = choosen_filename, 
            offloader = offloaderURL,
        )

        r = requests.post(f'{offloaderURL}/new', json=choosen)
        return choosen, choosen_inputs, r.status_code, r.json(), r.url

# ------------------------------------------------------------------------------------------