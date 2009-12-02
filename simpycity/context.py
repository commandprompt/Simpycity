from simpycity.model import SimpleModel
from simpycity import config as dbc
from simpycity.handle import Handle

from simpycity.core import Raw, Function

import logging
log = logging.getLogger(__name__)

class Context(object):
    
    def __init__(self, handle=None, config=None):
        
        self.handle_obj = handle
        
        if config is not None:
            self.config = config
        else:
            self.config = dbc
        
    def Raw(self, *args, **kwargs):
        kwargs['handle'] = self.handle
        return Raw(*args,**kwargs)
    def Function(self, *args, **kwargs):
        kwargs['handle'] = self.handle
        return Function(*args,**kwargs)
        
    def Model(self, *args, **kwargs):
        this_handle = self.handle
        class internalModel(SimpleModel):
            
            def __init__(self, *args, **kwargs):
                if 'handle' in kwargs:
                    del (kwargs['handle'])
		kwargs[handle] = this_handle
                super(internalModel, self).__init__(*args, **kwargs)
        return internalModel
                
    def commit(self):
        self.handle.commit()
        
    def rollback(self):
        self.handle.rollback()
    
    def close(self):
        self.handle.close()
    
    @property
    def handle(self):
        if self.handle_obj is None:
            self.handle_obj = Handle(self.config)
            
        if self.handle_obj.conn.closed:
            self.handle_obj.__reconnect__()
        
        log.debug("Using db user of %s" % self.config.user)
        return self.handle_obj
        
        
# class Context(object):
# 
#     def __init__(self):
# 
#         self.handle = Handle(config)
# 
#     def Raw(self, *args, **kwargs):
#         kwargs['handle'] = self.handle
#         return Raw(*args,**kwargs)
#     def Function(self, *args, **kwargs):
#         kwargs['handle'] = self.handle
#         return Function(*args,**kwargs)
# 
#     def Model(self, *args, **kwargs):
#         this_handle = self.handle
#         class internalModel(SimpleModel):
# 
#             def __init__(self, *args, **kwargs):
#                 if 'handle' in kwargs:
#                     del (kwargs['handle'])
#                 super(internalModel, self).__init__(*args, handle=this_handle, **kwargs)
#         return internalModel
# 
#     def commit(self):
#         self.handle.commit()
# 
#     def rollback(self):
#         self.handle.rollback()
# 
#     def close(self):
#         self.handle.close()
