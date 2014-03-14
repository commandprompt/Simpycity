from simpycity import NotFoundError
from simpycity.core import Function, FunctionError, ProceduralException, Raw, Query
from simpycity.handle import Handle
from simpycity import config as g_config
import psycopg2

def d_out(text):

    if g_config.debug:
        print text

class Construct(object):
    config = None
    handle = None

    def __init__(self, config=None, handle=None, *args,**kwargs):
        """
        A Construct is a basic datatype for Simpycity - basically providing
        a framework that allows for all queries in the Construct to operate
        under a single logical transaction.
        """

        d_out("Construct.__init__: config=%s, handle=%s" % (config, handle))

        if not self.config:
            self.config = config or g_config

        if not self.handle:
            self.handle = handle or Handle(config=self.config)


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
            #d_out("Construct.__getattribute__: Found a conventional attribute")
        except TypeError:
            #d_out("Construct.__getattribute__: Found an uninstanced attribute")
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
            
    # @property
    # def handle(self):
    #     
    #     h = self.__dict__["__handle__"]
    #     if (h.open):
    #         # Tests for the handle not being closed, and attempts to
    #         # reopen the handle on the Handle side.
    #         return h 
    #         
    #     
    # @handle.setter
    # def handle(self, value):
    # 
    #     """Sets the handle object. 
    #     Doesn't bother testing whether or not the handle is any good (this 
    #     needs to be tested at the get) level.
    #     """
    #     self.__dict__['__handle__'] = value


class SimpleModel(Construct):

    """
    The basic simple model class.
    Implements the barest minimum of Model functionality to operate.

    The SimpleModel expects a table=[] to declare its columns.
    SimpleModel expects __load__ to be a function that loads an instance, based
    on primary key, from the database.

    """

    def __init__(self, *args, **kwargs):
        """
        Sets up the objects' internal column.
        Tests for the presence of a primary key, and attempts to load a
        description using it.
        """

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

        if args or kwargs:
            d_out("SimpleModel.__init__: Got args of %s" % str(args))
            d_out("SimpleModel.__init__: Got kwargs of %s" % str(kwargs))

        # should automatically pick up config= and handle=
        super(SimpleModel, self).__init__(config, handle, *args, **kwargs)

        # config and handle have been dealt with, now.
        if args or kwargs:
            if hasattr(self, '__lazyload__'):
                self.__lazyargs__ = args
                self.__lazykwargs__ = kwargs
                d_out("__lazyargs__: %s" % str(self.__lazyargs__))
                d_out("__lazykwargs__: %s" % str(self.__lazykwargs__))

            elif hasattr(self, '__load__'):
                self.__load_by_key__(*args, **kwargs)

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
        #opts['reduce'] = True

        kwargs['options'] = opts
        kwargs['options']['reduce'] = True

        rs = None
        try:
            if hasattr(self, '__lazyload__'):
                rs = self.__lazyload__(*args, **kwargs)
            else:
                rs = self.__load__(*args, **kwargs)
        except psycopg2.InternalError, e:
            d_out("pgerror=%s pgcode=%s diag=%s" % (e.pgerror, e.pgcode, e.diag))
            if not (e.pgcode == 'P0002'): # no_data_found
                raise # as InternalError

        if rs is None:
            raise NotFoundError("Record not found in __load__ (%s)" % self.__class__)

        d_out("SimpleModel.__load_by_key__: rs: %s" % rs)
        try:
            # TODO: why list them explicitly in the table attribute?
            for item in self.table:
                d_out("SimpleModel.__load_by_key__: %s during load is %s" % (item, str(rs[item])))
                self.__dict__[item] = rs[item]
            d_out("SimpleModel.__load_by_key__: self.__dict__ is %s" % self.__dict__)
        except TypeError, e:
            # We can assume that we've been given a single record that
            # cannot be subscripted. Therefore, we'll set it to the first
            # value in self.table
            d_out("Simplemodel.__load_by_key__: TypeError: %s" % e)
            self.__setattr__(self.table[0], rs)
            raise

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
            #d_out("SimpleModel.__getattribute__: Found a conventional attribute %s" % name)
        except TypeError:
            #d_out("SimpleModel.__getattribute__: Found an uninstanced attribute")
            mro = [x.__name__ for x in type(attr).mro(attr)]

        if name in ['__load__', '__lazyload__']:
            d_out("skipping: %s" % name)
            return attr

        if "meta_query" in mro:

            d_out("SimpleModel.__getattribute__: Found meta_query %s" % name)
            def instance(*args,**kwargs):

                if args:
                    raise FunctionError("This function can only take keyword arguments.")
                my_args = kwargs.copy()
                for arg in attr.args:
                    d_out("SimpleModel.__getattribute__ InstanceMethod: checking arg %s" % arg)
                    d_out("SimpleModel.__getattribute__: %s" % self.__dict__)
                    if arg in self:
                        d_out("SimpleModel.__getattribute__ InstanceMethod: found %s in col.." %arg)
                        my_args[arg] = getattr(self, arg)

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


    def set(self,col,val):
        self.__dict__[col] = val


    def save(self):

        """Performs the __save__ method, if it has been declared.
        If not, this function raises a CannotSave exception.
        .save() does *not* implicitly commit the model.
        To commit, it must be done manually."""

        if hasattr(self, "__save__"):

            args = self.__save__.args()
            my_args = {}
            for arg in args:
                if arg in self.__dict__['__dirty']:
                    my_args[arg] = self.__dict__['__dirty'][arg]
                elif arg in self.__dict__:
                    my_args[arg] = self.__dict__[arg]
                else:
                    my_args[arg] = None # Send the NULL to the database.
            rs = self.__save__(**my_args).fetchone()

            for arg in self.table:
                if arg in rs:
                    self.__dict__[arg] = rs[arg]
                else:
                    self.__dict__[arg] = self.__dict__['__dirty'][arg]

            del(self.__dict__['__dirty'])
        else:
#            from simpycity import CannotSave
            raise NotImplementedError("Cannot save without __save__ declaration.")


    def __setattr__(self, name, value):
        """Sets the provided name to the provided value, in the dirty
        dictionary.
        This only occurs if the specified name is in the table specification."""

        if hasattr(self, 'table'):
            if name in self.table:
                if '__dirty' not in self.__dict__:
                    self.__dict__['__dirty'] = {}

                self.__dict__['__dirty'][name] = value
                return

        object.__setattr__(self, name, value)


    def __getattr__(self, name):
        """
            Gets the provided name.
            If the element is present in the dirty dictionary, this element is
            returned first.
            Otherwise, the element in the standard dictionary is returned.
        """
        
        if self.__dict__.has_key('__dirty') and name in self.__dict__['__dirty']:
            return self.__dict__['__dirty'][name]

        if name in self.__dict__:
            return self.__dict__[name]

        # OK, since we got here, check if it's a first-time access to
        # a table attr in a lazy-load model
        try:
            tbl = object.__getattribute__(self, 'table')
        except AttributeError:
            tbl = None

        if tbl and name in tbl:
            ll = object.__getattribute__(self, '__lazyload__')
            la = object.__getattribute__(self, '__lazyargs__')
            lk = object.__getattribute__(self, '__lazykwargs__')
            if ll and (la or lk):
                self.__lazyargs__ = None
                self.__lazykwargs__ = None
                object.__getattribute__(self, '__load_by_key__')(*la, **lk)
                return self.__getattr__(name)

        attr = object.__getattribute__(self, name) # Use the topmost parent version
        return attr


    def __contains__(self, item):
        """
            Internal method.
            Tests if the internal table declaration has a given key.
        """

        return item in self.__dict__ or '__dirty' in self.__dict__ and item in self.__dict__['__dirty']
