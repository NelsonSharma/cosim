
import requests
from . basic import GTISEP, FIDSEP, now

class Manager:

    @staticmethod
    def NewInfra(list_of_nodes):
        def defaultnode(node_name): return dict(
            name=node_name,
            nhost="<notify-ip>",
            nport='<notify-port>',
            dhost="<data-ip>",
            dport='<data-port>',
            https = False,
            xy = (0.0, 0.0),
        )
        return {n:defaultnode(n) for n in list_of_nodes} # a dict of name:dict

    @staticmethod
    def NodeUrls(cnode):
        #cnode = infra[decision[node]]
        s = ('s' if cnode['https'] else '')
        nhost, dhost, nport, dport = cnode['nhost'], cnode['dhost'], cnode['nport'], cnode['dport']
        return f'http{s}://{nhost}:{nport}', f'http{s}://{dhost}:{dport}'
    
    @staticmethod
    def GetDecision(flow, infra):
        locations = list(infra.keys())
        decision = {k:locations[0] for k in flow.NODES}
        return decision, locations

    @staticmethod
    def PrepareFlow(flow, decision, infra):
        """
        Prepare a flow for offloading

        :param node_id: Unique id of the node (UE) that generated the flow
        :param flow_id: Unique id of the flow instance, keep blank to generate timestanp based ids
        :param flow: the Flow(nx.DiGraph) object 
        :param decision: a dict of decisions like {task_name: offloading_location}
        :param infra: a dict decribing the available nodes to foofload
        """
        node_id = infra[decision[flow.ENTRY]]['name']
        flow_id = now()
        fid = f'{node_id}{FIDSEP}{flow_id}'
        for n in flow.NODES: 
            flow.INFO[n]['name'] = n
            flow.INFO[n]['offl'] = decision[n]
            flow.INFO[n]['uid'] = f'{fid}{GTISEP}{n}'
            flow.INFO[n]['fid'] = f'{fid}'
            flow.INFO[n]['outsend'] = {
                flow.edges[m]['data'] : (
                    m[-1], 
                    decision[m[-1]], 
                    *__class__.NodeUrls(infra[decision[m[-1]]]),
                    )  for m in flow.out_edges(n)}

        for o in flow.INFO[flow.EXIT]['outputs']:
            flow.INFO[flow.EXIT]['outsend'][o] = (
                    '', 
                    decision[flow.ENTRY],  
                    *__class__.NodeUrls(infra[decision[flow.ENTRY]]),
                    )

        return flow

    @staticmethod
    def Offload(flow, decision, infra):
        res = {}
        for tK in decision:
            nurl, durl = __class__.NodeUrls(infra[decision[tK]])
            response = requests.post(url=f"{nurl}/add", json=flow.INFO[tK])
            res[tK] = response.status_code, response.text, response.url
        return res

    @staticmethod
    def StartFlow(flow, decision, infra, initial_input_name=None):
        
        initial_input_name_default = f"{flow.INFO[flow.ENTRY]['inputs'][0]}"
        if not initial_input_name: initial_input_name = initial_input_name_default

        nurl, durl = __class__.NodeUrls(infra[decision[flow.ENTRY]])
        response = requests.post(
            url=f"{nurl}/notify",
            json={
                'uid': flow.INFO[flow.ENTRY]['uid'],
                'outputs': { initial_input_name_default: initial_input_name,},
            }
        )
        res_note = response.status_code, response.text, response.url

        final_output_name = f"{flow.INFO[flow.EXIT]['uid']}{GTISEP}{flow.INFO[flow.EXIT]['outputs'][0]}"
        data_url = f"{durl}/data/{final_output_name}" # response = requests.get(url=data_url)
        
        return res_note, data_url

    @staticmethod
    def GetResult(data_url):
        out = None
        response = requests.get(url=data_url)
        if response.status_code == 200:
            from io import BytesIO
            buffer = BytesIO()
            buffer.write(response._content)
            buffer.seek(0)
            import pickle
            out = pickle.loads(buffer.getbuffer())
            del buffer
        return out, response.status_code


