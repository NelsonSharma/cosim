

dict(
        
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

