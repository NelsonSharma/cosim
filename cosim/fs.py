


#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
import os, datetime, requests, shutil, signal, threading
from urllib.parse import quote
from flask import Flask, request, abort, jsonify, send_file
from waitress import serve
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
from . import FixStateInfo, VarStateInfo, ValidatePath, DirectoryListing, GetIP
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=


#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Requestor:


    # - - - - - - - - - - - - - - - - - - - - - - - - 
    # Flasked
    # - - - - - - - - - - - - - - - - - - - - - - - - 

    @staticmethod
    def Stop(endpoint):
        try:
            response = requests.get(f"http://{endpoint}/stop")
            success = (response.status_code == 200)
        except: success = False
        return success
    # - - - - - - - - - - - - - - - - - - - - - - - - 

    @staticmethod
    def Ping(endpoint):
        try:
            response = requests.get(f"http://{endpoint}")
            success = (response.status_code == 200)
        except: success = False
        return success
    # - - - - - - - - - - - - - - - - - - - - - - - - 


    @staticmethod
    def Index(endpoint, **kwargs):
        argstr = "?" if kwargs else ""
        for k,v in kwargs.items(): argstr+=f'{k}={v}&'
        try:
            response = requests.get(f"http://{endpoint}{argstr}")
            success = (response.status_code == 200)
            if success: result = response.json() if kwargs else response.text
            else: result = None
        except: success, result = False, None
        return success, result
    # - - - - - - - - - - - - - - - - - - - - - - - - 




    # - - - - - - - - - - - - - - - - - - - - - - - - 
    # FlaskFS
    # - - - - - - - - - - - - - - - - - - - - - - - - 

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    @staticmethod
    def Get(endpoint, remote_path, local_path, chunk_size=None):
        do_stream = bool(chunk_size)
        if do_stream: chunk_size = int(chunk_size)
        try:
            response = requests.get(f"http://{endpoint}/data/{quote(remote_path, safe='/')}", stream=do_stream)
            success = (response.status_code == 200)
            if success:
                with open(local_path, "wb") as f:
                    if do_stream:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if chunk: f.write(chunk)
                    else: f.write(response.content)
        except: success = False
        return success
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    @staticmethod
    def List(endpoint, remote_path):
        try:
            response = requests.get(f"http://{endpoint}/data/{quote(remote_path, safe='/')}")
            success = (response.status_code == 200)
            result = response.json()
        except: success, result = False, None
        return success, result
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    @staticmethod
    def Post(endpoint, remote_path, *local_paths):
        files = {}
        try:
            for local_file in local_paths: files[os.path.basename(local_file)] = open(local_file, "rb") 
            response = requests.post(f"http://{endpoint}/data/{quote(remote_path, safe='/')}", files=files)
            success = (response.status_code == 200)
        except: success = False
        finally:
            for handle in files.values(): handle.close()
        return success
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    @staticmethod
    def Put(endpoint, remote_path):
        try:
            response = requests.put(f"http://{endpoint}/data/{quote(remote_path, safe='/')}")
            success = (response.status_code == 200)
        except: success = False
        return success
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    @staticmethod
    def Del(endpoint, remote_path):
        try:
            response = requests.delete(f"http://{endpoint}/data/{quote(remote_path, safe='/')}")
            success = (response.status_code == 200)
        except: success = False
        return success
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=






    # - - - - - - - - - - - - - - - - - - - - - - - - 
    # 
    # - - - - - - - - - - - - - - - - - - - - - - - - 


    @staticmethod
    def Send(endpoint, **exeargs): # ue.route_send
        argstr = "?" if exeargs else ""
        for k,v in exeargs.items(): argstr+=f'{k}={v}&'
        try:
            response = requests.get(f"http://{endpoint}/send{argstr}")
            success = (response.status_code == 200)
            if success: result = response.json() # if kwargs else response.text
            else: result = None
        except: success, result = False, None
        return success, result
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=


    @staticmethod
    def CNTask(endpoint, meta):   # cn.route_task
        try:
            response = requests.post(f"http://{endpoint}/task", json=meta)
            success = (response.status_code == 200)
        except: success = False
        return success
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=


    @staticmethod
    def OMTask(endpoint, meta): # om.route_task
        try:
            response = requests.post(f"http://{endpoint}/task", json=meta)
            success = (response.status_code == 200)
        except: success = False
        return success
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=



    @staticmethod
    def NodeJoin(endpoint, nid, source): # om.route_node
        try:
            response = requests.post(f"http://{endpoint}/node?join", json=FixStateInfo.ALL(source=source, nid=nid))
            success = (response.status_code == 200)
        except: success = False
        return success
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    @staticmethod
    def NodeLeave(endpoint, nid): # om.route_node
        try:
            response = requests.post(f"http://{endpoint}/node?leave", json=dict(nid=nid))
            success = (response.status_code == 200)
        except: success = False
        return success
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    @staticmethod
    def NodeList(endpoint): # om.route_node.get
        try:
            response = requests.get(f"http://{endpoint}/node")
            success = (response.status_code == 200)
            if success: result = response.json()
            else: result = None
        except: success, result = False, None
        return success, result

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    @staticmethod
    def Offload(endpoint, tid, nid): # om.route_offload
        try:
            response = requests.get(f"http://{endpoint}/offload?tid={tid}&nid={nid}")
            success = (response.status_code == 200)
        except: success = False
        return success
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    @staticmethod
    def QueueList(endpoint): # om.route_queue.get
        try:
            response = requests.get(f"http://{endpoint}/queue")
            success = (response.status_code == 200)
            if success: result = response.json()
            else: result = None
        except: success, result = False, None
        return success, result
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    @staticmethod
    def QueuePop(endpoint, index=0): # om.route_queue.get
        try:
            response = requests.get(f"http://{endpoint}/queue??={index}")
            success = (response.status_code == 200)
            if success: result = response.json()
            else: result = None
        except: success, result = False, None
        return success, result
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=



    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    @staticmethod
    def State(endpoint, nid, queue): # om.route_state
        try:
            response = requests.post(f"http://{endpoint}/state", json=VarStateInfo.ALL(nid=nid, queue=queue))
            success = (response.status_code == 200)
        except: success = False
        return success
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    @staticmethod
    def StateList(endpoint): # om.route_state.get
        try:
            response = requests.get(f"http://{endpoint}/state")
            success = (response.status_code == 200)
            if success: result = response.json()
            else: result = None
        except: success, result = False, None
        return success, result
    
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=


#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class FlaskBase:
    r"""
    Base Flask App Template. 
    Underlying Flask App is defined in `self.app`
    Each app has a base directory defined in `self.base`
    No routes are defined by default
    New routes can be defined in `self.register()` using `self.app.add_url_rule()`
    App can be launched using waitress by calling the instance
    """

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def register(self): pass # add routes here using self.app.add_url_rule
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def __init__(self, name:str, base:str):
        if not base: base = os.getcwd()
        base=os.path.abspath(base)
        os.makedirs(base, exist_ok=True)
        self.base = base
        self.app = Flask(name, 
                instance_relative_config = True, 
                static_folder=self.base, 
                template_folder=self.base, 
                instance_path = self.base,)
        self.register()
        self.lock = threading.Lock()

    def __call__(self, host:str, port:str, threads:int, connection_limit:int, max_request_body_size:str):
        # ===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++
        def str2bytes(size):
            if isinstance(size, str):
                sizes = dict(BB=2**0, KB=2**10, MB=2**20, GB=2**30, TB=2**40)
                return int(float(size[:-2])*sizes.get(size[-2:].upper(), 0))
            else: return max(1024, int(abs(size)))
        # ===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++
        start_time = datetime.datetime.now()
        # ===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++
        serve(self.app, 
            host = host, 
            port = port, 
            url_scheme = 'http', 
            threads = threads, 
            connection_limit = connection_limit,
            max_request_body_size = str2bytes(max_request_body_size),
            ) # https://docs.pylonsproject.org/projects/waitress/en/stable/runner.html
        # ===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++
        end_time = datetime.datetime.now()
        # ===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++
        uptime = end_time - start_time
        uptimestr = '{}'.format(uptime)
        # ===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++
        return uptimestr

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=


#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class Flasked(FlaskBase):

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    DEFAULT_STOP_DELAY = 1.0
    ALL_HOST = "0.0.0.0"
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def __init__(self, name, base, printer, source:str, host:str, port:str, threads:int, connection_limit:int, max_request_body_size:str):
        super().__init__(name, base)
        self.printer, self.host, self.port, self.threads, self.connection_limit, self.max_request_body_size = \
            printer, host, port, threads, connection_limit, max_request_body_size
        self.source = source if source else f'{(GetIP()[-1] if self.host == self.ALL_HOST else self.host)}:{self.port}' 
    def __call__(self): 
        self.printer(f'[START] 🟢 Access via http://{self.source}')
        return super().__call__(self.host, self.port, self.threads, self.connection_limit, self.max_request_body_size)
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def register(self): 
        super().register()
        self.app.add_url_rule("/", view_func=self.route, methods=["GET"])
        self.app.add_url_rule("/stop", view_func=self.route_stop, methods=["GET"])
        self.app.add_url_rule("/stop/", view_func=self.route_stop, methods=["GET"])
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def route(self):
        if not (request.method=='GET'):     return abort(405) # no other method allowed
        if not (request.args):              return self.route_ping()  # ping
        else:                               return self.route_index() 
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def route_stop(self): 
        self.printer(f'[STOP] 🛑 received via {request.remote_addr}')
        threading.Timer(float(request.args.get("?", self.DEFAULT_STOP_DELAY)), lambda :  os.kill(os.getpid(), signal.SIGINT)).start()
        return "OK", 200
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def route_ping(self): return "OK", 200
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
 
    def route_index(self): return jsonify(request.args)
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=


#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class FlaskFS(Flasked):

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def __init__(self, name, base, data, printer, source:str, host:str, port:str, threads:int, connection_limit:int, max_request_body_size:str):
        super().__init__(name, base, printer, source, host, port, threads, connection_limit, max_request_body_size)
        if not data: data = os.getcwd()
        data=os.path.abspath(data)
        os.makedirs(data, exist_ok=True)
        self.data = data
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def register(self):
        super().register()
        # route_data
        self.app.add_url_rule("/data",
            view_func=self.route_data,
            methods=["GET", "POST", "PUT", "DELETE"],
            defaults={"req_path": ""} )
        self.app.add_url_rule("/data/",
            view_func=self.route_data,
            methods=["GET", "POST", "PUT", "DELETE"],
            defaults={"req_path": ""} )
        self.app.add_url_rule("/data/<path:req_path>",
            view_func=self.route_data,
            methods=["GET", "POST", "PUT", "DELETE"] )
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def route_data(self, req_path):

        #-------------------------------------
        # INDEX BEHAVIOUR
        #-------------------------------------
        if not req_path: return jsonify(DirectoryListing(self.data)) 
        #-------------------------------------
        
        #-------------------------------------
        #PATH BEHAVIOR
        #-------------------------------------
    
        abs_path = ValidatePath(self.data, req_path)
        if abs_path is None: return abort(404)

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        if request.method=='GET': # download files
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            if os.path.isfile(abs_path): 
                self.printer(f'[GET] ⚡ Downloading file {req_path} via {request.remote_addr}')
                return send_file(abs_path, as_attachment=True) 
            elif os.path.isdir(abs_path):
                return jsonify(DirectoryListing(abs_path))
            else: return abort(404)
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        elif request.method=='POST': # upload files to existing folders
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            #if not os.path.exists(abs_path): return abort(404)
            if os.path.isdir(abs_path): 
                success=False
                for fk, fv in request.files.items(): 
                    try: 
                        fv.save(os.path.join(abs_path, fk))
                        self.printer(f'[POST] ✅ Upload to dir {req_path, fk} via {request.remote_addr}')
                    except: self.printer(f'[POST] ❌ Upload to dir {req_path, fk} via {request.remote_addr}')
                success=True
                return f'{success}', (200 if success else 500)
            else:
                success=False
                for fk, fv in request.files.items(): 
                    try: 
                        fv.save(abs_path)
                        self.printer(f'[POST] ✅ Upload File {req_path} via {request.remote_addr}')
                        success=True
                    except: self.printer(f'[POST] ❌ Upload File {req_path} via {request.remote_addr}')
                    break # only one file allowed
                return f'{success}', (200 if success else 500)
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        elif request.method=='PUT': # create dir
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            success=False
            try: 
                os.makedirs(abs_path, exist_ok=True)
                success=True
                self.printer(f'[PUT] ✅ Create dir {req_path} via {request.remote_addr}')
            except: self.printer(f'[PUT] ❌ Create dir {req_path} via {request.remote_addr}')
            return f'{success}', (200 if success else 500)
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        elif request.method=='DELETE': # delete file or folders
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            if not os.path.exists(abs_path): return f"Does not exist", 200
            if os.path.isdir(abs_path):
                success=False
                try: 
                    shutil.rmtree(abs_path)
                    success=True
                    self.printer(f'[DELETE] ✅ Remove dir {req_path} via {request.remote_addr}')
                except: self.printer(f'[DELETE] ❌ Remove dir {req_path} via {request.remote_addr}')
                return f'{success}', (200 if success else 500)
            else:
                success=False
                try:
                    os.remove(abs_path)
                    success=True
                    self.printer(f'[DELETE] ✅ Remove file {req_path} via {request.remote_addr}')
                except: self.printer(f'[DELETE] ❌ Remove file {req_path} via {request.remote_addr}')
                return f'{success}', (200 if success else 500)
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        else: return abort(405)
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

