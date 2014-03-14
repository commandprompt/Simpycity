import psycopg2
from psycopg2 import extras
from simpycity import config as g_config
import weakref
from contextlib import contextmanager

import simpycity

def d_out(text):

    if g_config.debug:
        print text


class Handle(object):

    """
    A base Simpycity handle.
    a Handle is the wrapper object around a
    """

    def __init__(self, dsn=None, config=None, isolation_level=None):

        self.conn = None

        self.config = config or g_config
        self.dsn = dsn or self.config.dsn()

        self.isolation_level = None
        if isolation_level is not None and isolation_level in [0,1,2]:
            self.isolation_level = isolation_level

        d_out("Handle.__init__: Creating DB connection")
        self.__reconnect__()
        d_out("Handle.__init__: Connection PID is %s" % self.conn.get_backend_pid() )

        if self.isolation_level is not None:
            self.conn.set_isolation_level(isolation_level)

    def __reconnect__(self):
        if self.conn and not self.conn.closed:
            self.close()

        self.conn = psycopg2.connect(self.dsn)

    def cursor(self,*args,**kwargs):
        d_out("Handle.cursor: Creating cursor..")
        if not self.open:
            raise Exception("Connection isn't open.")

        kwargs["cursor_factory"] = extras.DictCursor
        return self.conn.cursor(*args,**kwargs)

    def execute(self, *args, **kwargs):
        return self.cursor().execute(*args, **kwargs)

    @property
    def autocommit(self):
        # We trust the user not to run SQL SET commands directly to
        # alter the value of default_transaction_isolation setting.
        return self.isolation_level == psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT

    def begin(self):
        d_out("Handle.begin: Begin transaction.")

        if not self.open:
            raise Exception("Connection isn't open.")

        if self.autocommit:
            # In non-autocommit mode, the first statement executed on
            # cursor is prepended with BEGIN automatically by
            # psycopg2.  We only need to run it in autocommit mode.
            self.execute("BEGIN")

    def commit(self):
        d_out("Handle.commit: Commit transaction.")

        if self.conn.closed:
            # That's weird, and bad.
            raise Exception("Attempting to commit a closed handle.")

        if self.autocommit:
            # Connection object's commit/rollback() funcs has no
            # effect in autocommit mode.  Run direct SQL statement
            # through the default cursor.
            self.execute("COMMIT")
        else:
            self.conn.commit()

    def rollback(self):
        d_out("Handle.rollback: Abort transaction.")

        if not self.conn.closed:

            if self.autocommit:
                # See commit()
                self.execute("ROLLBACK")
            else:
                self.conn.rollback()

    @contextmanager
    def transaction(self):
        try:
            self.begin()
            yield
            self.commit()
        except:
            self.rollback()
            raise

    def __repr__(self):
        if not self.conn.closed:
            return "Handle object: pid %s" % self.conn.get_backend_pid()
        else:
            return "Handle object: no pid (closed)"

    def close(self,*args,**kwargs):
        d_out("Handle.close: de-allocating connection" )

        if self.conn is None:
            return

        if not self.conn.closed:
            d_out("handle.close: handle open, closing pid %s" % self.conn.get_backend_pid() )
            self.conn.close()
        else:
            d_out("handle.close: handle already closed.")

    def __del__(self):
        d_out("Handle.__del__: destroying handle, de-allocating connection")
        if self.conn:
            self.close()
            
    @property
    def open(self):
        try:
            if self.conn.get_backend_pid():
                return True
        except psycopg2.InterfaceError, e:
            if str(e) == "connection already closed":
                # We already lost our connection. Attempt to reforge it.
                self.__reconnect__()
                if self.conn.get_backend_pid() >= 0:
                    return True # Resets it.
            else:
                raise e # Another error. Don't try to trap it.
