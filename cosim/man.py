
import requests, datetime
import json, pickle

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


class Manager:

    @staticmethod
    def Now(year:bool=True, month:bool=True, day:bool=True, 
        hour:bool=True, minute:bool=True, second:bool=True, mirco:bool=True, 
        start:str='', sep:str='', end:str='') -> str:
        form = []
        if year:    form.append("%Y")
        if month:   form.append("%m")
        if day:     form.append("%d")
        if hour:    form.append("%H")
        if minute:  form.append("%M")
        if second:  form.append("%S")
        if mirco:   form.append("%f")
        assert (form), 'format should not be empty!'
        return (start + datetime.datetime.strftime(datetime.datetime.now(), sep.join(form)) + end)

    @staticmethod
    def NewInfra(list_of_nodes):
        def defaultnode(): return dict(
            host="<server-ip>",
            nport='<note-port>',
            dport='<data-port>',
            urlscheme = "http://",
            xy = (0.0, 0.0),
        )
        return {n:defaultnode() for n in list_of_nodes} # a dict of name:dict

    @staticmethod
    def GetDecision(flow, infra):
        # { task_name : infra_node } {'tA': 'I', 'tB': 'E', 'tC': 'C', 'tD': 'C'}
        locations = list(infra.keys())
        decision = {k:locations[0] for k in flow.NODES}
        return decision, locations

    @staticmethod
    def PrepareFlow(alias, flow, decision, infra):
        """
        Prepare a flow for offloading

        :param alias: Unique id of the node (UE) that generated the flow
        :param flow: the Flow(nx.DiGraph) object 
        :param decision: a dict of decisions like {task_name: offloading_location}
        :param infra: a dict decribing the available nodes to foofload
        """
        fid = __class__.Now(start=alias)
        for n in flow.NODES: 
            flow.INFO[n]['name'] = n
            flow.INFO[n]['offl'] = decision[n]
            flow.INFO[n]['uid'] = f'{fid}_{n}'
            flow.INFO[n]['fid'] = f'{fid}'
            flow.INFO[n]['outsend'] = {
                flow.edges[n]['data'] :  (  
                    n[-1], 
                    decision[n[-1]], 
                    f"{infra[decision[n[-1]]]['urlscheme']}{infra[decision[n[-1]]]['host'] }",
                    infra[decision[n[-1]]]['nport'], 
                    infra[decision[n[-1]]]['dport']
                        )  for n in flow.out_edges(n)}

        for o in flow.INFO[flow.EXIT]['outputs']:
            flow.INFO[flow.EXIT]['outsend'][o] = (
                    '', 
                    decision[flow.ENTRY],  
                    f"{infra[decision[flow.ENTRY]]['urlscheme']}{infra[decision[flow.ENTRY]]['host'] }",
                    infra[decision[flow.ENTRY]]['nport'], 
                    infra[decision[flow.ENTRY]]['dport']
                        )

        return flow

    @staticmethod
    def Offload(flow, decision, infra):
        res = {}
        for tK in decision:
            response = requests.post(
                url=f"{infra[decision[tK]]['urlscheme']}{infra[decision[tK]]['host']}:{infra[decision[tK]]['nport']}/add",
                json=flow.INFO[tK]
                )
            res[tK] = response.status_code, response.text, response.url
        return res

    @staticmethod
    def StartFlow(flow, decision, infra, initial_input_name=None):
        
        initial_input_name_default = f"{flow.INFO[flow.ENTRY]['inputs'][0]}"
        if not initial_input_name: initial_input_name = initial_input_name_default
        note_node = f"{infra[decision[flow.ENTRY]]['urlscheme']}{infra[decision[flow.ENTRY]]['host']}:{infra[decision[flow.ENTRY]]['nport']}"
        response = requests.post(
            url=f"{note_node}/note",
            json={
                'uid': flow.INFO[flow.ENTRY]['uid'],
                'outputs': {
                    initial_input_name_default: initial_input_name,
                },
            }
        )
        res_note = response.status_code, response.text, response.url

        data_node = f"{infra[decision[flow.ENTRY]]['urlscheme']}{infra[decision[flow.ENTRY]]['host']}:{infra[decision[flow.ENTRY]]['dport']}"
        final_input_name = f"{flow.INFO[flow.EXIT]['uid']}_{flow.INFO[flow.EXIT]['outputs'][0]}"
        data_url = f"{data_node}/data/{final_input_name}"
        #response = requests.get(url=data_url)
        
        return res_note, data_url

    @staticmethod
    def GetResult(data_url):
        out = None
        response = requests.get(url=data_url)
        res_data = response.status_code, response.text, response.url
        if response.status_code == 200:
            from io import BytesIO
            buffer = BytesIO()
            buffer.write(response._content)
            buffer.seek(0)
            import pickle
            out = pickle.loads(buffer.getbuffer())
            del buffer
        return out, res_data


