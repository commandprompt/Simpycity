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
            
        if handle is not None:
            d_out("Construct.__init__: Found handle.")
            self.handle = handle
        else:
            if config is not None:
                self.config = config
            else:
                self.config = g_config
            d_out("Construct.__init__: Did not find handle - forging.")
            self.handle = Handle(self.config)
    
    def commit(self):
        if self.handle is not None:
            self.handle.commit()
        else:
            raise AttributeError("Cannot call commit without localized handle.")
    def close(self):
        self.handle.close()
        
    def rollback(self):
        if self.handle is not None:
            self.handle.rollback()
        else:
            raise AttributeError("Cannot call rollback without localized handle.")
        
    def __getattribute__(self,name):
        
        """
        Private method.
        
        This function tests all attributes of the Construct if they are 
        Simpycity base objects.
        If they are a Simpycity object, the handle is forcibly overriden
        with the Constructs' handle, creating a logically grouped transaction
        state.
        
        """
        
        attr = object.__getattribute__(self,name)
        # This uses a try/catch because non-instance attributes (like 
        # __class__) will throw a TypeError if you try to use type(attr).mro().
        
        mro = None
        try:
            mro = [x.__name__ for x in type(attr).mro()]
            d_out("Construct.__getattribute__: Found a conventional attribute")
        except TypeError:
            d_out("Construct.__getattribute__: Found an uninstanced attribute")
            mro = [x.__name__ for x in type(attr).mro(attr)]
            
            
        if "meta_query" in mro:
            
            d_out("Construct.__getattribute__: Found meta_query %s" % name)
            def instance(*args,**kwargs):
                my_args = kwargs.copy()
                if 'options' not in kwargs:
                    d_out("Construct __getattribute__: Didn't find options.")
                    my_args['options'] = {}
                d_out("Construct __getattribute__: Setting handle.")
                my_args['options']['handle'] = self.handle
                return attr(**my_args)
            return instance
            
        else:
            return attr


class SimpleModel(Construct):
    
    """
    The basic simple model class.
    Implements the barest minimum of Model functionality to operate.
    
    The SimpleModel expects a table=[] to declare its columns.
    SimpleModel expects __load__ to be a function that loads an instance, based
    on primary key, from the database.
    
    """
    
    def __init__(self, *args,**kwargs):
        """
        Sets up the objects' internal column.
        Tests for the presence of a primary key, and attempts to load a
        description using it.
        """
        self.col = {}
        
        if 'config' in kwargs:
            config = kwargs['config']
            del(kwargs['config'])
        else:
            config = None
            
        if 'handle' in kwargs:
            handle = kwargs['handle']
            del(kwargs['handle'])
        else:
            handle = None
        
        # should automatically pick up config= and handle=
        super(SimpleModel, self).__init__(config, handle, *args, **kwargs)
        
        # config and handle have been dealt with, now.
        
        if args or kwargs:
            try:
                d_out("SimpleModel.__init__: Got args of %s" % args)
            except TypeError:
                pass
            d_out("SimpleModel.__init__: Got kwargs of %s" % kwargs)
            try:
                if self.__load__ is not None:
                    self.__load_by_key__(*args, **kwargs)
            except AttributeError, e:
                pass
    
    def __load_by_key__(self, *args, **kwargs):
        """
        Private method. 
        Using the primary key from __init__, load the database row by primary
        key.
        """
        # check for an __load__ function
        
        if 'options' in kwargs:
            opts = kwargs['options']
            del(kwargs['options'])
        else:
            opts = {}
        
        opts['handle'] = self.handle
        opts['reduce'] = True
        
        kwargs['options'] = opts
        
        try:
            rs = self.__load__(*args, **kwargs)
            try:
                for item in self.table:
                    d_out("SimpleModel.__load_by_key__: %s during load is %s" % (item, rs[item]))
                    self.col[item] = rs[item]
                d_out("SimpleModel.__load_by_key__: self.col is %s" % self.col)
            except TypeError, e:
                # We can assume that we've been given a single record that 
                # cannot be subscripted. Therefore, we'll set it to the first 
                # value in self.table
                d_out("Simplemodel.__load_by_key__: Got %s during load." % e)
                self.set_col(self.table[0], rs)
            
        except AttributeError, e:
            #pass
            d_out("SimpleModel.__load_by_key: Caught an AttributeError: %s" % e)
            raise 
        except psycopg2.InternalError, e:
            raise ProceduralException(e)
    
    def __getattribute__(self,name):
        
        """
        Private method.
        
        This function tests all attributes of the SimpleModel if they are 
        Simpycity base objects.
        If they are a Simpycity object, columns from the Model are mapped
        to the InstanceMethods' arguments, as appropriate.
        
        Similar to Construct, a SimpleModel will also enforce all Simpycity
        objects to use its Handle state, creating a singular transactional
        entity.
        """

        attr = object.__getattribute__(self,name)
        # This uses a try/catch because non-instance attributes (like 
        # __class__) will throw a TypeError if you try to use type(attr).mro().
        
        mro = None
        try:
            mro = [x.__name__ for x in type(attr).mro()]
            d_out("SimpleModel.__getattribute__: Found a conventional attribute")
        except TypeError:
            d_out("SimpleModel.__getattribute__: Found an uninstanced attribute")
            mro = [x.__name__ for x in type(attr).mro(attr)]
            
        if name == '__load__':
            
            return attr
        
        if "meta_query" in mro:
                
            d_out("SimpleModel.__getattribute__: Found meta_query %s" % name)
            def instance(*args,**kwargs):
                
                if args:
                    raise FunctionError("This function can only take keyword arguments.")
                my_args = kwargs.copy()
                for arg in attr.args:
                    d_out("SimpleModel.__getattribute__ InstanceMethod: checking arg %s" % arg)
                    d_out("SimpleModel.__getattribute__: %s" % self.col)
                    if arg in self.col:
                        d_out("SimpleModel.__getattribute__ InstanceMethod: found %s in col.." %arg)
                        my_args[arg] = self.col[arg]
                        
                if 'options' not in kwargs:
                    d_out("SimpleModel.__getattribute__: Didn't find options. Setting handle.")
                    my_args['options'] = {}
                d_out("SimpleModel.__getattribute__: Setting handle.")
                my_args['options']['handle'] = self.handle
                rs = attr(*args, **my_args)
                d_out("SimpleModel.__getattribute__: attr returned rs of %s" %rs)
                return rs
            return instance
            
        else:
            return attr

    def set_col(self,col,val):
        self.col[col] = val
