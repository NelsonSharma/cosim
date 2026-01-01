# import pickle, os, io


# class State:
    
#     def __init__(self, **attributes):
#         for name,value in attributes.items(): setattr(self, name, value)

#     @staticmethod
#     def Pack(state, buffer=None):
#         if buffer is None:
#             buffer = io.BytesIO()
#             pickle.dump(state, buffer)
#         else:
#             buffer = os.path.abspath(buffer)
#             with open(buffer, 'wb') as f: pickle.dump(state, f)
#         return buffer

#     @staticmethod
#     def Unpack(buffer):
#         import pickle, os
#         if not isinstance(buffer, str):
#             buffer.seek(0)
#             state = pickle.load(buffer)
#         else:
#             buffer = os.path.abspath(buffer)
#             with open(buffer, 'rb') as f: state = pickle.load(f)
#         return state
    

# class DataRetriver:

#     @staticmethod
#     def Read(url, name):
