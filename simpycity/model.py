import psycopg2

class InstanceMethod(object):

    """InstanceMethods are a requirement for the SimpleModel object. 
    Instancemethods are used to map internal arguments for a 
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
    def set_col(self,col,val):
        self.col[col] = val

class FunctionError(BaseException):
    """
    Bare exception, used for naming purposes only.
    """
    pass

class ProceduralException(BaseException):
    
    pass
