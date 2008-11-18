import psycopg2
from psycopg2 import extras
import re

#from pylons import config

class meta_query(object):
    def __init__(self, *in_args, **kwargs):
        self.rs = None
        try:
            if not self.query:
                self.query = ''
        except AttributeError:
            self.query = ''
        self.call_list = []
        self.args = in_args
        self.keyargs = kwargs
        if 'returns' in kwargs:
            self.columns = kwargs['returns']
        else:
            self.columns = [] # an empty set.
        
        if len(self.columns) >= 1:
            # we are limiting the return type.
            # Eventually, we'll check it against the return object.
            # Until then, we just assume the user knows what they're 
            # doing.
            self.cols = ",".join(["%s" for x in range(0,len(self.columns))])
            
        else:
            self.cols = "*"
            
        if self.arg_count >= 1:
            if len(self.args) < len(self.creation_args) \
                and len(self.keyargs) < len(self.creation_args) \
                and len(self.keyargs) + len(self.args) < len(self.creation_args):
                    raise Exception("Insufficient arguments.")
                
            if len(self.args) > len(self.creation_args) \
                or len(self.keyargs) > len(self.creation_args) \
                or len(self.keyargs) + len(self.args) > len(self.creation_args):
                    raise Exception("Too many arguments.")
            self.call_list = ['' for x in xrange(len(self.creation_args))]
            if kwargs:
                # we have to do some magic.
                try:
                    for arg in self.keyargs.iterkeys():
                        self.call_list[ self.creation_args.index(arg) ] = self.keyargs[arg]
                except ValueError:
                    raise Exception("Spurious keyword argument passed.")
            for index,arg in enumerate(self.args):
                self.call_list[index] = arg
        
        self.__execute__()
        
    def __repr__(self):
        if self.query:
            return self.query % self.call_list
            
    def __iter__(self):
        if self.rs is not None:
            return self.rs.__iter__()
        else:
            raise StopIteration()
            
    def __len__(self):
        if self.rs is not None:
            return self.rs.__len__()
        else:
            return 0
        
    def next(self):
        if self.rs is not None:
            return self.rs.fetchone()
        else:
            return None
            
    def __execute__(self):
        self.form_query()
        app_conf = config['app_conf']
        conn = psycopg2.connect(
            "host=%s port=%s dbname=%s user=%s password=%s" % (
                app_conf['db.host'],
                app_conf['db.port'],
                app_conf['db.database'],
                app_conf['db.user'],
                app_conf['db.password']
            )
        )
        cursor = conn.cursor(cursor_factory=extras.DictCursor)
        print "Simpycity Query: %s" % (self.query)
        print "Simpycity Call List: %s" % (self.call_list)
        rs = cursor.execute(self.query, self.call_list)
        self.rs = SimpleResultSet(cursor)
        self.rs.conn = conn

    def form_query(self):
        # This needs to be overridden
        pass 
        
    def commit(self):
        if self.rs is not None:
            self.rs.commit()
    def rollback(self):
        if self.rs is not None:
            self.rs.rollback()

def Raw(query, args=[],return_type=None):
    # Just let us do raw SQL, kthx.
    
    class sql_function(meta_query):
        query = query
        creation_args = args
        r_type = return_type
        def __init__(self):
            # Disable the standard init
            pass
    return sql_function()
    
def Query(name, where=[], return_type=None):
    where_list = None
    if len(where) >= 1:
        where_list = [x+"=%s" for x in where]
    
    class sql_function(meta_query):
        name = table_name
        r_type = return_type
        w_list = where_list
        arg_count = len(where)
        creation_args = where
        r_type = return_type
        
        def form_query(self):
            
            self.query = "SELECT " + self.cols + " FROM " + self.name 
            if self.w_list:
                self.query += " WHERE " + " AND ".join(self.w_list)

def Function(name, args=[], return_type=None):
    if len(args) >= 1:
        replace = ['%s' for x in xrange(len(args))]
        query = "FROM %s(" % name + ",".join(replace) + ")"
    else:
        query = "FROM %s()" % name
        
    class sql_function(meta_query):
        function_name = name
        func_query = query
        r_type = return_type
        arg_count = len(args)
        creation_args = args
        
        def form_query(self):

            self.query = "SELECT "+ self.cols + " " + self.func_query

    return sql_function
    
class SimpleResultSet(object):
    
    def __init__(self, cursor):
        self.cursor = cursor
    def __iter__(self):
        row = self.cursor.fetchone()
        while row:
            yield row
            row = self.cursor.fetchone()
            if row is None:
                raise StopException()
    def __len__(self):
        if self.cursor is not None:
            return self.cursor.rowcount
        else:
            return 0
    def next(self):
        return self.cursor.fetchone()
    def fetchone(self):
        return self.cursor.fetchone()
    def commit(self):
        return self.conn.commit()
    def rollback(self):
        return self.conn.rollback()

class TypedResultSet(object):
    
    def __init__(self,cursor,i_type):
        self.cursor=cursor
        self.type=i_type
    def __iter__(self):
        row = self.cursor.fetchone()
        while row:
            o = i_type(row)
            yield o
            row = self.cursor.fetchone()
            if row is None:
                raise StopException()
            o = i_type(row)

class InstanceMethod(object):
    """
    A basic object that the SimpleModel uses to determine raw functions from 
    functions that should be mapped by SimpleModel.
    """
    def __init__(self, func, args=[], return_type=None):
        self.name = func
        self.args = args
        self.return_type = return_type
        self.function = Function(self.name,args,return_type)


class SimpleModel(object):
    
    """
    The basic simple model class.
    Implements the barest minimum of Model functionality to operate.
    
    The SimpleModel expects a table=[] to declare its columns.
    SimpleModel expects __load__ to be a function that loads an instance, based
    on primary key, from the database.
    
    """
    
    def __init__(self, key=None, *args,**kwargs):
        """
        Sets up the objects' internal column.
        Tests for the presence of a primary key, and attempts to load a
        description using it.
        """
        self.col = {}    
        if key is not None:
            self.__load_by_key__(key, *args, **kwargs)
    
    def __load_by_key__(self, key=None, *args,**kwargs):
        """
        Private method. 
        Using the primary key from __init__, load the database row by primary
        key.
        """
        # check for an __get function
        if key is not None:
            try:
                rs = self.__load__(key)
                row = rs.next()
                for item in self.table:
                    print "Simpycity __load_by_key__: %s during load is %s" % (item, row[item])
                    self.col[item] = row[item]
                print self.col
            except AttributeError, e:
                #pass
                print "Caught an AttributeError: %s" % e
            except psycopg2.InternalError, e:
                raise ProceduralException(e)
    
    def __getattribute__(self,name):
        
        """
        Private method.
        
        This function tests all attributes of the SimpleModel if they are 
        InstanceMethods.
        If they are an InstanceMethod, columns from the Model are mapped
        to the InstanceMethods' arguments, as appropriate.
        
        """

        attr = object.__getattribute__(self,name)
        # This uses a try/catch because non-instance attributes (like 
        # __class__) will throw a TypeError if you try to use type(attr).mro().
        
        try:
            if 'InstanceMethod' in [x.__name__ for x in type(attr).mro()]:
                def instance(*args,**kwargs):
                    if args:
                        raise FunctionError("This function can only take keyword arguments.")
                    my_args = {}
                    for arg in kwargs:
                        my_args[arg] = kwargs[arg]
                    for arg in attr.args:
                        print "Simpycity InstanceMethod: checking arg %s" %arg
                        print self.col
                        if arg in self.col:
                            print "Simpycity InstanceMethod: found %s in col.." %arg
                            my_args[arg] = self.col[arg]
                    return attr.function(**my_args)
                return instance
            return attr
        except TypeError:
            # not an Instance Method, just return
            return attr

class FunctionError(BaseException):
    """
    Bare exception, used for naming purposes only.
    """
    pass

class ProceduralException(BaseException):
    
    pass