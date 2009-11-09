from simpycity.core import Function, FunctionError, ProceduralException, Raw, Query
from simpycity.handle import Handle
from simpycity import config as g_config
import psycopg2

def d_out(text):
    
    if g_config.debug:
        print text

class Construct(object):
    
    def __init__(self, config=None, handle=None, *args,**kwargs):
        """
        A Construct is a basic datatype for Simpycity - basically providing
        a framework that allows for all queries in the Construct to operate
        under a single logical transaction.
        """
        self.col = {}    
        if key is not None:
            self.__load_by_key__(key, *args, **kwargs)
            
        if handle is not None:
            d_out("Simpycity __init__: Found handle.")
            self.handle = handle
        else:
            if config is not None:
                self.config = config
            else:
                self.config = g_config
            d_out("Simpycity __init__: Did not find handle - forging.")
            self.handle = Handle(self.config)
    
    def commit(self):
        if self.handle is not None:
            self.handle.commit()
        else:
            raise AttributeError("Cannot call commit without localized handle.")
    def close(self):
        self.handle.close()
        
    def rollback(self):
        self.handle.rollback()

class InstanceMethod(object):

    """InstanceMethods are a requirement for the SimpleModel object. 
    Instancemethods are used to map internal arguments for a 
    """
    def __init__(self, func, args=[], return_type=None, *posargs, **kwargs):
        self.name = func
        self.args = args
        self.return_type = return_type
        self.function = Function(self.name,args,return_type,*posargs,**kwargs)
        
class InstanceQuery(object):

    """InstanceMethods are a requirement for the SimpleModel object. 
    Instancemethods are used to map internal arguments for a 
    """
    def __init__(self, func, args=[], return_type=None, *posargs, **kwargs):
        self.name = func
        self.args = args
        self.return_type = return_type
        self.function = Query(self.name,args,return_type,*posargs,**kwargs)
        
class InstanceRaw(object):

    """InstanceMethods are a requirement for the SimpleModel object. 
    Instancemethods are used to map internal arguments for a 
    """
    def __init__(self, func, args=[], return_type=None, *posargs, **kwargs):
        self.name = func
        self.args = args
        self.return_type = return_type
        self.function = Raw(self.name,args,return_type,*posargs,**kwargs)


class SimpleModel(Construct):
    
    """
    The basic simple model class.
    Implements the barest minimum of Model functionality to operate.
    
    The SimpleModel expects a table=[] to declare its columns.
    SimpleModel expects __load__ to be a function that loads an instance, based
    on primary key, from the database.
    
    """
    
    def __init__(self, key=None, config=None, handle=None, *args,**kwargs):
        """
        Sets up the objects' internal column.
        Tests for the presence of a primary key, and attempts to load a
        description using it.
        """
        self.col = {}
            
        if handle is not None:
            d_out("SimpleModel __init__: Found handle.")
            self.handle = handle
        else:
            if config is not None:
                self.config = config
            else:
                self.config = g_config
            d_out("SimpleModel __init__: Did not find handle - forging.")
            self.handle = Handle(self.config)
        
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
                rs = self.__load__(key,options=dict(handle=self.handle))
                row = rs.next()
                for item in self.table:
                    d_out("Simpycity __load_by_key__: %s during load is %s" % (item, row[item]))
                    self.col[item] = row[item]
                print self.col
            except AttributeError, e:
                #pass
                d_out("Simpycity __load_by_key: Caught an AttributeError: %s" % e)
                raise 
            except psycopg2.InternalError, e:
                raise ProceduralException(e)
    
    def __getattribute__(self,name):
        
        """
        Private method.
        
        This function tests all attributes of the SimpleModel if they are 
        InstanceMethods.
        If they are an InstanceMethod, columns from the Model are mapped
        to the InstanceMethods' arguments, as appropriate.
        
        It then tests if an attribute is a Function, Query, or Raw - the base
        primitives of Simpycity. If it is, then handle= is added
        to the argument list.
        
        """

        attr = object.__getattribute__(self,name)
        # This uses a try/catch because non-instance attributes (like 
        # __class__) will throw a TypeError if you try to use type(attr).mro().
        
        mro = None
        try:
            mro = [x.__name__ for x in type(attr).mro()]
            d_out("SimpleModel __getattribute__: Found a conventional attribute")
        except TypeError:
            d_out("SimpleModel __getattribute__: Found an uninstanced attribute")
            mro = [x.__name__ for x in type(attr).mro(attr)]
            
        
        if "sql_function" in mro:
                
            d_out("SimpleModel __getattribute__: Found sql_function %s" % name)
            def instance(*args,**kwargs):
                my_args = kwargs
                
                if 'options' not in kwargs:
                    d_out("SimpleModel __getattribute__: Didn't find options. Setting..")
                    my_args['options'] = {}
                    my_args['options']['handle'] = self.handle
                return attr(*args,**my_args)
            return instance
            
        elif 'InstanceMethod' in mro or 'InstanceRaw' in mro or 'InstanceQuery' in mro:
            d_out("SimpleModel __getattribute__: Found an InstanceMethod %s " % name)
            def instance(*args,**kwargs):
                if args:
                    raise FunctionError("This function can only take keyword arguments.")
                my_args = {}
                my_args = kwargs
                for arg in attr.args:
                    d_out("Simpycity __getattribute__ InstanceMethod: checking arg %s" % arg)
                    d_out(self.col)
                    if arg in self.col:
                        d_out("Simpycity __getattribute__ InstanceMethod: found %s in col.." %arg)
                        my_args[arg] = self.col[arg]
                if 'options' not in kwargs:
                    d_out("SimpleModel __getattribute__: Didn't find options. Setting..")
                    my_args['options'] = {}
                    my_args['options']['handle'] = self.handle
                return attr.function(**my_args)
            return instance
            
        else:
            return attr

    def set_col(self,col,val):
        self.col[col] = val