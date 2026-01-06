

dict(
    
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

