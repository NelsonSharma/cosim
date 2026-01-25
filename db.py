
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
# pre-defined infra objects
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class Infradb:


    def List(): return [k for k,v in __class__.__dict__.items() if not (k.startswith('__') or k.endswith('__') or k=='List' or k=='Get')]
    def Get(name): return getattr(__class__, name)()
    
    def infra_1():
        return (
                    ( 'i0',        'e1',       'c2'         ), # location names
                    (	0,          5,  	    0,	   		), # i0 data rate from
                    (	0,          0,	        20,		    ), # e1 data rate from
                    (	0,          0,			0,	      	), # c2 data rate from
                    (	5,          2,          1,          ), # task rate
                )

    
    def infra_2():
        return (
                    ( 'i0',       'e1',        'e2',       'e3',        'c4',       'c5'        ),
                    (	0,          5,  	    5, 	   		 5,          0,  	    0,          ), # i0
                    (	0,          0,	        20,		     0,          20,  	    0           ), # e1
                    (	0,          0,	        0,	    	 20,         20,  	    0,          ), # e2
                    (	0,          0,	        0,	    	 0,          0,  	    20,         ), # e3
                    (	0,          0,			0,	      	 0,          0,  	    20,         ), # c4
                    (	0,          0,			0,	      	 0,          0,  	    0,          ), # c5
                    (	5,          2,          2,           2,          1,		    1,		    ), # task rate
                )

    def infra_3():
        return (
                    ( 'i0',       'e1',        'e2',       'e3',        'e4',      'e5',       'c6',       'c7'     ),
                    (	0,          5,  	    5, 	   		 5,          5,  	    5,          0,          0,      ), # i0
                    (	0,          0,	        20,		     0,          0,  	    0,          20,  	    0,      ), # e1
                    (	0,          0,	        0,	    	 20,         0,  	    0,          20,  	    0,      ), # e2
                    (	0,          0,	        0,	    	 0,          20,  	    0,          20,  	    0,      ), # e3
                    (	0,          0,			0,	      	 0,          0,  	    20,         20,  	    0,      ), # e4
                    (	0,          0,			0,	      	 0,          0,  	    0,          0,  	    20,     ), # e5
                    (	0,          0,			0,	      	 0,          0,  	    0,          0,  	    30,     ), # c6
                    (	0,          0,			0,	      	 0,          0,  	    0,          0,  	    0,      ), # c7
                    (	5,          2,          2,           2,          2,         2,          1,		    1,		), # task rate
                )


    def infra_4():
        return (
                (  'i0',       'e1',       'e2',        'e3',       'e4',       'e5',       'e6',           'e7',           'e8',         'c9',       'c10',      'c11'     ),
                (	0,          5,  	    5, 	   		 5,          5,  	    5,          5,              5,              5,              0,          0,          0,      ), # i0
                (	0,          0,	        20,		     0,          0,  	    0,          0,              0,              0,             20,          0,  	    0,      ), # e1
                (	0,          0,	        0,	    	 20,         0,  	    0,          0,              0,              0,             20,          0,  	    0,      ), # e2
                (	0,          0,	        0,	    	 0,          20,  	    0,          0,              0,              0,             20,          0,  	    0,      ), # e3
                (	0,          0,			0,	      	 0,          0,  	    0,          0,              0,              0,              0,  	    20,         0,      ), # e4
                (	0,          0,			0,	      	 0,          0,  	    0,          20,             0,              0,              0,          20,  	    20,     ), # e5
                (	0,          0,			0,	      	 0,          0,  	    0,          0,              20,             0,              0,          0,  	    20,     ), # e6
                (	0,          0,			0,	      	 0,          0,  	    0,          0,              0,              20,              0,         0,  	    20,     ), # e7
                (	0,          0,			0,	      	 0,          0,  	    0,          0,              0,              0,              0,          0,  	    20,     ), # e8
                (	0,          0,			0,	      	 0,          0,  	    0,          0,              0,              0,              0,          30,  	    0,      ), # c9
                (	0,          0,			0,	      	 0,          0,  	    0,          0,              0,              0,              0,          0,  	    30,     ), # c10
                (	0,          0,			0,	      	 0,          0,  	    0,          0,              0,              0,              0,          0,  	    0,      ), # c11
                (	5,          2,          2,           2,          2,         2,          2,              2,              2,              1,		    1,		    1,		), # task rate
                )

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-


# ------------------------------------------------------------------------------------------

#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
# pre-defined flow objects
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
        E0 = dict(
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
        X3 = dict(
        inputs = ('z1','z2',),
        outputs = ('o',),   #<--- must have single output
        ),
    )


#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

# ------------------------------------------------------------------------------------------