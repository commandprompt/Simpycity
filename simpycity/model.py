from simpycity import NotFoundError
from simpycity.core import FunctionError, meta_query
from simpycity import config as g_config
import psycopg2

def d_out(text):

    if g_config.debug:
        print text

class Construct(object):
    config = None

    def __init__(self, config=None, handle=None, *args,**kwargs):
        """
        A Construct is a basic datatype for Simpycity - basically providing
        a framework that allows for all queries in the Construct to operate
        under a single logical transaction.
        """

        d_out("Construct.__init__: config=%s, handle=%s" % (config, handle))

        if not self.config:
            self.config = config or g_config

        self.init_handle = handle


    @property
    def handle(self):
        if not self.init_handle:
            self.init_handle = self.config.handle_factory(config=self.config)

        return self.init_handle

    def commit(self):
        if self.handle is not None:
            self.handle.commit()
        else:
            raise AttributeError("Cannot call commit without localized handle.")

    def close(self):
        if self.init_handle:
            self.init_handle.close()

    def rollback(self):
        if self.handle is not None:
            self.handle.rollback()
        else:
            raise AttributeError("Cannot call rollback without localized handle.")


class SimpleModel(Construct):

    """
    The basic simple model class.
    Implements the barest minimum of Model functionality to operate.

    The SimpleModel expects a table=[] to declare its columns. However if you leave table an empty list and
    define pg_type as a tuple of two members (schema, type) then
    register_composite() will calculate table with a query to Postgres at runtime.
    SimpleModel expects one of:
    * __load__ to be a FunctionSingle or QuerySingle that loads an instance, based on primary key, from the database.
    * __lazyload__ to be a FunctionSingle that loads an instance from the database, depending on the existance of a value for
      loaded_indicator, which should be a member of table that will only be populated after the instance is fully loaded.

    """

    pg_type = None
    table = []

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
            d_out("SimpleModel.__init__: Got args of %s" % repr(args))
            d_out("SimpleModel.__init__: Got kwargs of %s" % repr(kwargs))

        #
        # If the type defines a base type, the base instance will be
        # passed in the 'base_' kwarg: merge the base attrs into
        # kwargs and hand over to superclass.
        #
        SimpleModel.merge_base_attrs(kwargs)

        #
        # Initialize table attrs by copying from keyword arguments.
        #
        # This makes sure the objects not loaded from DB has at least
        # the None values in attrs and access to them doesn't raise.
        #
        if hasattr(self, 'table'):
            for name in self.table:
                self.__dict__[name] = kwargs.get(name, None)

        # should automatically pick up config= and handle=
        super(SimpleModel, self).__init__(config, handle, *args, **kwargs)

        if hasattr(self, 'loaded_indicator') and self.__dict__.get(object.__getattribute__(self, 'loaded_indicator')) is not None:
            self._loaded = True
        else:
            self._loaded = False

        # config and handle have been dealt with, now.
        if not self._loaded and hasattr(self, '__load__'):
            if args or kwargs:
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

        kwargs['options'] = opts

        row = None
        try:
            row = self.__load__(*args, **kwargs)
        except psycopg2.InternalError, e:
            d_out("pgerror=%s pgcode=%s diag=%s" % (e.pgerror, e.pgcode, e.diag))
            if not (e.pgcode == 'P0002'): # no_data_found
                raise # as InternalError

        if row is None:
            raise NotFoundError()

        d_out("SimpleModel.__load_by_key__: rs: %s" % row)
        if isinstance(row, psycopg2.extras.DictRow):
            loaded_attrs = dict(row)
        elif isinstance(row, SimpleModel):
            #python 2.7+:
            #loaded_attrs = {_ for _ in rs.__dict__.iteritems() if _[0] in self.table}
            loaded_attrs = {}
            for col in self.table:
                loaded_attrs[col] = row.__dict__[col]
        else:
            raise Exception("row is type {0}".format(type(row)))
        SimpleModel.merge_base_attrs(loaded_attrs)
        for item in self.table:
            d_out("SimpleModel.__load_by_key__: %s during load is %s" % (item, repr(loaded_attrs[item])))
            self.__dict__[item] = loaded_attrs[item]
        self._loaded = True
        d_out("SimpleModel.__load_by_key__: self.__dict__ is %s" % self.__dict__)

    def __getattribute__(self,name):

        """
        Private method.

        This function tests all attributes of the SimpleModel if they are
        instances of meta_query.
        If they are a meta_query, columns from the Model are mapped
        to the InstanceMethods' arguments, as appropriate.
        """

        attr = object.__getattribute__(self,name)
        if name == '__load__':
            d_out("skipping: %s" % name)
            return attr

        if attr is None and name in object.__getattribute__(self, 'table') and not object.__getattribute__(self,'_loaded'):
            should_lazyload = hasattr(self,'__lazyload__')
        else:
            should_lazyload = False

        if should_lazyload:
            d_out("lazyloading {0} on {1}".format(self.__class__, name))
            attrs = object.__getattribute__(self, '__dict__')
            attrs['_loaded'] = True
            rs = self.__lazyload__(options={'handle':self.handle})
            if not rs:
                raise NotFoundError("__lazyload__ returned: {0}".format(rs))
            if isinstance(rs, psycopg2.extras.DictRow):
                loaded_attrs = dict(rs)
            elif isinstance(rs, SimpleModel):
                #python 2.7+:
                #loaded_attrs = {_ for _ in rs.__dict__.iteritems() if _[0] in self.table}
                loaded_attrs = {}
                for col in self.table:
                    loaded_attrs[col] = rs.__dict__[col]
            else:
                raise Exception("__lazyload__ returned unexpected type: {0}".format(type(rs)))
            SimpleModel.merge_base_attrs(loaded_attrs)
            attrs.update(loaded_attrs)
            return attrs[name]

        if isinstance(attr, meta_query):

            d_out("SimpleModel.__getattribute__: Found meta_query %s" % name)
            def instance(*args,**kwargs):

                if args:
                    raise FunctionError("This function can only take keyword arguments.")
                my_args = kwargs.copy()
                d_out("SimpleModel.__getattribute__ InstanceMethod: kwargs: %s" % repr(kwargs))
                d_out("SimpleModel.__getattribute__ InstanceMethod: self.__dict__: %s" % self.__dict__)
                for arg in attr.args:
                    d_out("SimpleModel.__getattribute__ InstanceMethod: checking arg %s" % arg)
                    if arg not in kwargs:
                        d_out("not in my_args")
                        if hasattr(self, arg):
                            d_out("SimpleModel.__getattribute__ InstanceMethod: found %s in col.." % arg)
                            my_args[arg] = getattr(self, arg)
                        else:
                            my_args[arg] = None

                if 'options' not in kwargs:
                    d_out("SimpleModel.__getattribute__ InstanceMethod: Didn't find options.")
                    my_args['options'] = {}

                # pass self to the query object
                my_args['options']['model'] = self

                d_out("SimpleModel.__getattribute__: InstanceMethod: Setting handle.")
                my_args['options']['handle'] = self.handle
                rs = attr(*args, **my_args)
                d_out("SimpleModel.__getattribute__: InstanceMethod: model :{model} attrib name: {name} (attrib value: {value}) constructor returned {rs}".format(model=self, name=name, value=attr, rs=rs))
                return rs

            if attr.is_property:
                return instance()
            else:
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
                my_args[arg] = self.__dict__.get(arg, None)
            rs = self.__save__(**my_args).fetchone()

            for arg in self.table:
                if arg in rs:
                    self.__dict__[arg] = rs[arg]
        else:
#            from simpycity import CannotSave
            raise NotImplementedError("Cannot save without __save__ declaration.")

    @classmethod
    def register_composite(cls, name, handle=None, factory=None):
        """
        Maps a Postgresql type to this class.  If the class's table attribute
        is empty, and the class has an attribute pg_type of tuple (schema, type),
        it is calculated and set by querying Postgres. Register inherited/inheriting
        classes in heirarchical order.
        Every time a SQL function returns a registered type (including array
        elements and individual columns, recursively), this class
        will be instantiated automatically.

        The object attributes will be passed to the provided callable in
        a form of keyword arguments.

        :param name: the name of a PostgreSQL composite type, e.g. created using
            the |CREATE TYPE|_ command
        :param handle: Simpycity handle
        :param factory: if specified it should be a `psycopg2.extras.CompositeCaster` subclass: use
            it to :ref:`customize how to cast composite types <custom-composite>`
        :return: the registered `CompositeCaster` or *factory* instance
            responsible for the conversion
        """
        class CustomCompositeCaster(psycopg2.extras.CompositeCaster):

            def make(self, values):
                d_out("CustomCompositeCaster.make: cls={0} values={1}".format(repr(cls), repr(values)))
                return cls(**dict(zip(self.attnames, values)))

        PG_TYPE_SQL = """SELECT array_agg(attname)
FROM
    (
        SELECT attname
        FROM
            pg_type t
            JOIN pg_namespace ns ON typnamespace = ns.oid
            JOIN pg_attribute a ON attrelid = typrelid
        WHERE nspname = %s AND typname = %s
            AND attnum > 0 AND NOT attisdropped
        ORDER BY attnum
    ) sub;"""
        if handle is None:
            handle = g_config.handle_factory()
        d_out("SimpleModel.register_composite: before: table for {0} is {1}".format(repr(cls.pg_type), cls.table))
        if cls.pg_type is not None:
            super_table = cls.__mro__[1].table if hasattr(cls.__mro__[1], 'table') else []
            if cls.table == [] or cls.table is super_table:
                cursor = handle.cursor()
                cursor.execute(PG_TYPE_SQL, cls.pg_type)
                row = cursor.fetchone()
                d_out("SimpleModel.register_composite: row={0}".format(row))
                row[0] = [_ for _ in row[0] if _ != 'base_']
                cls.table = cls.table + row[0]
                d_out("SimpleModel.register_composite: after: table for {0} is {1}".format(repr(cls.pg_type), cls.table))
        if factory is None:
            factory = CustomCompositeCaster
        return psycopg2.extras.register_composite(
            name,
            handle.conn,
            globally=True,  # in case of reconnects
            factory=factory
        )

    @staticmethod
    def merge_base_attrs(attrs):
        """
        If one of the attrs is named "base_", assume that attribute is an instance of SimpleModel mapped on a Postgresql
        composite type, and that the base_ instance is of a superclass of this class. Expand the attributes of the
        base_ type and assign to class attributes.

        psycopg2's type casting uses namedtuple() and that forbids a
        name to start with underscore, so we end it with _ instead
        """
        base = attrs.pop('base_', None)
        if base:
            d_out("SimpleModel.merge_base_attrs: base.table={0}".format(base.table))
            for name in base.table:
                attrs[name] = base.__dict__[name]
