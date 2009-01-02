import psycopg2
from simpycity import config as g_config

def d_out(text):
    
    if g_config.debug:
        print text

class Handle(object):
    
    def __init__(self,config=None):
        
        if config is not None:
            self.config = config
        else:
            self.config = g_config
        
        d_out("Creating DB connection")
        self.conn = psycopg2.connect(
            "host=%s port=%s dbname=%s user=%s password=%s" % (
                self.config.host,
                self.config.port,
                self.config.database,
                self.config.user,
                self.config.password
            ),
        )
        
    def cursor(self,*args,**kwargs):
        d_out("Creating cursor..")
        return self.conn.cursor(*args,**kwargs)
    def commit(self):
        d_out("Committing transactions.")
        return self.conn.commit()
        
    def __repr__(self):
        return "Handle object"