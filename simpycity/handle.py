import psycopg2
from simpycity import config as g_config
import weakref

def d_out(text):
    
    if g_config.debug:
        print text

class Handle(object):
    
    """
    A base Simpycity handle.
    a Handle is the wrapper object around a 
    """
    
    def __init__(self,config=None,isolation_level=None):
        
        if config is not None:
            self.config = config
        else:
            self.config = g_config
            
        self.isolation_level = None
        if isolation_level is not None and isolation_level in [0,1,2]:
            self.isolation_level = isolation_level
        
        d_out("Handle.__init__: Creating DB connection")
        self.conn = psycopg2.connect(
            "host=%s port=%s dbname=%s user=%s password=%s" % (
                self.config.host,
                self.config.port,
                self.config.database,
                self.config.user,
                self.config.password
            ),
        )
        d_out("Handle.__init__: Connection PID is %s" % self.conn.get_backend_pid() )
        Manager.add(self)
        if self.isolation_level is not None:
            self.conn.set_isolation_level(isolation_level)
        
    def cursor(self,*args,**kwargs):
        d_out("Handle.cursor: Creating cursor..")
        cur = self.conn.cursor(*args,**kwargs)
        return cur
    def commit(self):
        d_out("Handle.commit: Committing transactions.")
        return self.conn.commit()
        
    def __repr__(self):
        return "Handle object: pid %s" % self.conn.get_backend_pid()
    
    def close(self,*args,**kwargs):
        d_out("Handle.close: de-allocating connection" )
        if not self.conn.closed:
            d_out("handle.close: handle open, closing pid %s" % self.conn.get_backend_pid() )
            self.conn.close()
        else:
            d_out("handle.close: handle already closed.")
            
    def rollback(self):
        
        self.conn.rollback()
        
    def __del__(self):
        d_out("Handle.__del__: destroying handle, de-allocating connection")
        self.close()
        
class Manager(object):
    
    """
    The Manager is a process-wide structure that acts as a single point of 
    control for all DB connections.
    The Manager is designed to capture all DB connections, storing them until
    later, and permitting easy reaping later.
    
    The Manager does not hold on to strong references to the Handle objects -
    This allows "unchanged" behaviour to be exhibited by Simpycity, in that 
    Managers are transparent and handles will reap on their original cycles.
    """
    
    handles = []
    
    @classmethod
    def add(cls, handle):
        """
        
        """
        cls.handles.append(weakref.ref(handle))
    
    def __init__(self):
        
        self.mark = len(self.handles)
        
    def __getitem__(self, id):
        
        return self.handles[ self.mark+id ]
        
    def __len__(self):
        
        return len(self.handles[self.mark:])
        
    def __repr__(self):
        return "Simpcity Manager: %i handles in scope, %i total" % (len(self), len(self.handles))
        
    def pop(self):
        return self.handles.pop()
        
    def close(self):
        l = self.handles[self.mark:]
        l.reverse()
        for handle in l:
            self.pop()
            h = handle()
            if h is not None:
                h.close()
            else:
                continue
        # Clear the list of handles.