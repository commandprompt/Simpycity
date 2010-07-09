import psycopg2
from psycopg2 import extras
from simpycity import config as g_config
import weakref

import simpycity

def d_out(text):

    if g_config.debug:
        print text


class Handle(object):

    """
    A base Simpycity handle.
    a Handle is the wrapper object around a
    """

    def __init__(self,dsn=None,isolation_level=None):

        self.conn = None
        self.dsn = dsn

        self.config = g_config

        self.isolation_level = None
        if isolation_level is not None and isolation_level in [0,1,2]:
            self.isolation_level = isolation_level

        d_out("Handle.__init__: Creating DB connection")
        self.__reconnect__()
        d_out("Handle.__init__: Connection PID is %s" % self.conn.get_backend_pid() )

        if self.isolation_level is not None:
            self.conn.set_isolation_level(isolation_level)

    def __reconnect__(self):

        if not self.dsn:
            self.dsn = "host=%s port=%s dbname=%s user=%s password=%s" % (
                    self.config.host,
                    self.config.port,
                    self.config.database,
                    self.config.user,
                    self.config.password
            )
        self.conn = psycopg2.connect(self.dsn)

    def cursor(self,*args,**kwargs):
        d_out("Handle.cursor: Creating cursor..")
        if self.conn.closed:
            self.conn = None
            self.__reconnect__()

        kwargs["cursor_factory"] = extras.DictCursor

        cur = self.conn.cursor(*args,**kwargs)
        # Test for liveliness.
        try:
            cur.execute("SELECT 1;")
        except psycopg2.DatabaseError:
            # DB has died.
            self.conn = None
            self.__reconnect__()
            cur = self.conn.cursor(*args,**kwargs)
            
        return cur

    def commit(self):
        d_out("Handle.commit: Committing transactions.")

        if self.conn.closed:
            # That's weird, and bad.
            raise Exception("Attempting to commit a closed handle.")

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

        if not self.conn.closed:
            self.conn.rollback()

    def __del__(self):
        d_out("Handle.__del__: destroying handle, de-allocating connection")
        if not self.conn.closed:
            self.close()