
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
# pre-defined infra objects
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class Flowdb:

    def List(): return [k for k,v in __class__.__dict__.items() if not (k.startswith('__') or k.endswith('__') or k=='List' or k=='Get')]
    def Get(name): return getattr(__class__, name)()

    def flow_1():
        return dict(
            
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

    
    def flow_2():
        return dict(
        
        # Entry Task
        E = dict(
            inputs = ('x',),    #<--- must have single input
            outputs = ('y1','y2',),
        ),

        # Intermediate Task
        I1 = dict(
            inputs = ('y1',),
            outputs = ('z1',),
        ),

        # Intermediate Task
        I2 = dict(
            inputs = ('y2',),
            outputs = ('z2',),
        ),

        # Exit Task
        X = dict(
        inputs = ('z1','z2',),
        outputs = ('o',),   #<--- must have single output
        ),
    )
