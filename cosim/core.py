


#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
import os, shutil
from flask import request, abort, jsonify
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
from . import Kio, TaskStatus, FileConfig, OpsConfig, RandomChoice, Scan, Zipped
from .fs import FlaskFS, Requestor
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=



#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class UEFS(FlaskFS):
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def __init__(self, name, base, data, printer, source, host, port, threads, connection_limit, max_request_body_size, om, id, tasks, **kwargs):
        super().__init__(name, base, data, printer, source, host, port, threads, connection_limit, max_request_body_size)
        assert id, f'ID not provided'
        assert om, f'OM not provided'
        self.uid = id 
        self.om = om # om-fs as source
        if not tasks: tasks = os.getcwd()
        tasks=os.path.abspath(tasks)
        os.makedirs(tasks, exist_ok=True)
        self.tasks = tasks
        self.TASKS = {name:tuple([f for f in os.listdir(path) if f.endswith(FileConfig.TASK_EXT)]) for name, path, isdir, isfile, islink, size in Scan(self.tasks, exclude_hidden=True, include_size=False, include_extra=False) if (isdir and not islink )}
        self.TASKL = list(self.TASKS.keys())

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def register(self):
        super().register()
        # route_task
        self.app.add_url_rule("/send", view_func=self.route_send, methods=["GET"])
        self.app.add_url_rule("/send/", view_func=self.route_send, methods=["GET"])

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def route_send(self):
        if request.method!='GET': return abort(405) # method allowed
        #-------------------------------------
        try:
            # send?name=tester
            # send?name=tester&exe=main.py
            # send?name=tester&exe=other.py
            # send?name=tester&exe=other.py&node=CN0

            taskname = request.args.get('name', None)
            taskexe = request.args.get('exe', None)
            tasknode = request.args.get('node', None)
            

            if not taskname: taskname = RandomChoice(self.TASKL)
            else: 
                if taskname not in self.TASKS: return abort(405)

            taskexes = self.TASKS[taskname]

            if not taskexe: taskexe = RandomChoice(taskexes)
            else:
                if taskexe not in taskexes: return abort(405)
            
            taskpath = os.path.join(self.tasks, taskname)
            meta =  self.method_generate(taskpath, taskexe)
            success = self.method_send(meta, requested_node=tasknode)
            return jsonify(meta) if success else abort(500)

        except: return abort(500)

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def method_send(self, meta, requested_node=None):
        r"""
        requested_node     
            a node to send this task to, filled as `requested_node` filed in meta-information
            if prefilled, om will send it there
        """

        # ~META~
        meta['requested_node'] = ('' if requested_node is None else f'{requested_node}')
        meta[TaskStatus.SENT] = TaskStatus.TS()


        metadir = os.path.join(self.data, meta['tid']) # make task directory on UE
        # os.makedirs(metadir, exist_ok=True) <-- will be created at generate
        Kio.SaveJSON(os.path.join(metadir, FileConfig.META_NAME), meta) # ⭕ # save sent task
        # post "meta" to OM's endpoint
        return Requestor.OMTask(self.om, meta)
        
    def method_generate(self, exedir, exename):    
        r"""
        :params:
        -------------------------------------
        exedir      path of folder that contains task data and executables
        exename     name of the file directly inside exedir that is the executable
        -------------------------------------
        """
        assert os.path.isdir(exedir)
        exepath = os.path.join(exedir, exename)
        assert os.path.isfile(exepath)

        tid = TaskStatus.ID(self.uid) # assign an id
        taskname = os.path.basename(exedir) # name is the dir name
        taskdir = os.path.join(self.data, tid) # make task directory on UE
        os.makedirs(taskdir, exist_ok=True)
        zippath = os.path.join(taskdir, FileConfig.ZIP_NAME) # put code and data as zip file 
        Zipped(exedir, zippath) 

        # ~META~
        return {
            'name': taskname, 
            'exe':  exename,
            'tid':  tid,
            'uid':  self.uid,
            'size': os.path.getsize(zippath), 
            'source': self.source,
        }
        

    # ===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++===+++

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=



#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class OMFS(FlaskFS):
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def __init__(self, name, base, data, printer, source, host, port, threads, connection_limit, max_request_body_size, limit, **kwargs):
        super().__init__(name, base, data, printer, source, host, port, threads, connection_limit, max_request_body_size)
        self.limit = int(limit) if limit else 0

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def register(self):
        super().register()
        # route_task
        self.app.add_url_rule("/task", view_func=self.route_task, methods=["POST"])
        self.app.add_url_rule("/task/", view_func=self.route_task, methods=["POST"])

        # route_offload
        self.app.add_url_rule("/offload", view_func=self.route_offload, methods=["GET"])
        self.app.add_url_rule("/offload/", view_func=self.route_offload, methods=["GET"])

        # route_queue
        self.app.add_url_rule("/queue", view_func=self.route_queue, methods=["GET"])
        self.app.add_url_rule("/queue/", view_func=self.route_queue, methods=["GET"])

        # route_node
        self.app.add_url_rule("/node", view_func=self.route_node, methods=["POST", "GET"])
        self.app.add_url_rule("/node/", view_func=self.route_node, methods=["POST", "GET"])

        # route_state
        self.app.add_url_rule("/state", view_func=self.route_state, methods=["POST", "GET"])
        self.app.add_url_rule("/state/", view_func=self.route_state, methods=["POST", "GET"])

        queue=os.path.abspath(os.path.join(self.base, OpsConfig.QUEUE_DIR_NAME))
        os.makedirs(queue, exist_ok=True)
        self.queue = queue
        self.QUEUE=[]

        states=os.path.abspath(os.path.join(self.base, OpsConfig.STATES_DIR_NAME))
        os.makedirs(states, exist_ok=True)
        self.states = states
        self.STATES={}

        nodes=os.path.abspath(os.path.join(self.base, OpsConfig.NODES_DIR_NAME))
        os.makedirs(nodes, exist_ok=True)
        self.nodes = nodes
        self.NODES={}

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def route_task(self):
        if request.method!='POST': return abort(405) # method allowed
        #-------------------------------------
        try:
            meta = request.get_json()
        
            # ~META~
            meta[TaskStatus.RECEIVED] = TaskStatus.TS()
            with self.lock: 
                Kio.SaveJSON(os.path.join(self.queue, meta['tid']), meta)
                self.QUEUE.append(meta["tid"])
            self.printer(f'[TASK] ⚡ Received task {meta["tid"]} from {request.remote_addr}')
            return "OK", 200
        except: return abort(500)

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def route_offload(self):
        if request.method!='GET': return abort(405) # method allowed
        if not self.QUEUE: return "No tasks available to Offload", 405 
        if not self.NODES: return "No nodes available to Offload", 405 
        #-------------------------------------
        try:
            tid = request.args.get("tid", '')
            nid = request.args.get("nid", '')

            if not (nid): return abort(405)
            if nid not in self.NODES: return abort(405)

            if not tid: tid = self.QUEUE[0] # choose from next in queue
            else:
                if tid not in self.QUEUE: return abort(404)

            metapath = os.path.join(self.queue, tid)
            if not os.path.isfile(metapath): return abort(405)
            meta = Kio.LoadJSON(metapath)
            
            # ~META~
            meta[TaskStatus.OFFLOADED] = TaskStatus.TS()
            meta['decision'] = nid
            self.printer(f'[TASK] 🔥 Offloading task {meta["tid"]} to node {nid}')
            if Requestor.CNTask(self.NODES[nid], meta):
                with self.lock: 
                    os.remove(metapath)
                    self.QUEUE.remove(meta["tid"])
                return "OK", 200
            else: return abort(500)
        except: return abort(500)

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def route_queue(self):
        if request.method =='GET':  
            if "?" in request.args: 
                try: tid = self.QUEUE[int(request.args.get("?", '0'))]
                except: tid = None
                return abort(404) if tid is None else tid, 200
            else: return jsonify(self.QUEUE)
        else: return abort(405)

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def route_node(self):
        if request.method =='GET':  return jsonify(self.NODES)
        elif request.method =='POST': 
            try:
                meta = request.get_json()
                nid = meta['nid']
                if "join" in request.args: 
                    if nid in self.NODES: return abort(405)
                    else: 
                        with self.lock: 
                            Kio.SaveJSON(os.path.join(self.nodes, nid), meta)
                            os.makedirs(os.path.join(self.states, nid))
                            self.NODES[nid] = meta['source']
                            self.STATES[nid] = [] # states queue
                        self.printer(f'[NODE] ➕ Node {nid} joined from {meta["source"]}')
                elif "leave" in request.args: 
                    if nid not in self.NODES: return abort(404)
                    else:
                        with self.lock: 
                            shutil.rmtree(os.path.join(self.states, nid))
                            os.remove(os.path.join(self.nodes, nid))
                            del self.STATES[nid] # states queue
                            del self.NODES[nid]
                        self.printer(f'[NODE] ➖ Node {nid} left')
                # elif "fin" in request.args: 
                #     if nid not in self.NODES: return abort(404)
                #     else:
                #         with self.lock: 
                #             shutil.rmtree(os.path.join(self.states, nid))
                #             os.remove(os.path.join(self.nodes, nid))
                #             del self.STATES[nid] # states queue
                #             del self.NODES[nid]
                #         self.printer(f'[NODE] ➖ Node {nid} left')
                else: return abort(405)
                return "OK", 200
            except: return abort(500)
        else: return abort(405)

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def route_state(self):
        if request.method=='GET':  return jsonify(self.STATES)
        elif request.method=='POST': 
            #-------------------------------------
            try:
                info = request.get_json()
                nid, ts = info['nid'], info['ts']
                if nid not in self.NODES: return abort(404)
                Kio.SaveJSON(os.path.join(self.states, nid, ts), info)
                with self.lock: 
                    self.STATES[nid].append(ts)
                    if self.limit:
                        if len(self.STATES[nid]) > self.limit: os.remove(os.path.join(self.states, nid, self.STATES[nid].pop(0)))
                return "OK", 200
            except: return abort(500)
        else: return abort(405)

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=



#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class CNFS(FlaskFS):
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def __init__(self, name, base, data, printer, source, host, port, threads, connection_limit, max_request_body_size, om, id, **kwargs):
        super().__init__(name, base, data, printer, source, host, port, threads, connection_limit, max_request_body_size)
        assert id, f'ID not provided'
        assert om, f'OM not provided'
        self.nid = id 
        self.om = om # om-fs as source
        
    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    def register(self):
        super().register()
        # route_task
        self.app.add_url_rule("/task", view_func=self.route_task, methods=["POST"])
        self.app.add_url_rule("/task/", view_func=self.route_task, methods=["POST"])

        queue=os.path.abspath(os.path.join(self.base, OpsConfig.QUEUE_DIR_NAME))
        os.makedirs(queue, exist_ok=True)
        self.queue = queue

        traces=os.path.abspath(os.path.join(self.base, OpsConfig.TRACES_DIR_NAME))
        os.makedirs(traces, exist_ok=True)
        self.traces = traces

    #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

    def route_task(self):
        if request.method!='POST': return abort(405) # method allowed
        #-------------------------------------
        try:
            meta = request.get_json()

            # ~META~
            meta['nid'] = self.nid
            meta[TaskStatus.QUEUED] = TaskStatus.TS()
            Kio.SaveJSON(os.path.join(self.queue, meta['tid']), meta)
            self.printer(f'[TASK] ⚡ Received task {meta["tid"]} from {request.remote_addr}')
            # start exe if not launched
            return "OK", 200
        except: return abort(500)

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=


#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
class FS: UEFS, CNFS, OMFS = UEFS, CNFS, OMFS # Available caller/functions
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=




