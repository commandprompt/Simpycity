import psycopg2
from simpycity import config as g_config

def d_out(text):
    
    if g_config.debug:
        print text

class Handle(object):
    
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
        return "Handle object"
    
    def close(self,*args,**kwargs):
        self.conn.close()
    
    def rollback(self):
        
        self.conn.rollback()