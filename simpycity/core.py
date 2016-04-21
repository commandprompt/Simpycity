"""
    COPYRIGHT 2009-2016 Command Prompt, Inc.
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import psycopg2
from simpycity import config, ProgrammingError
import simpycity.handle

def d_out(text):

    if config.debug:
        print text


class meta_query(object):

    """
    Base object for sql query-like objects For internal use only.
    """

    def __init__(self, name, args=[], handle=None, callback=None):

        """

         :name    Sets the base name of the query. How this is used will be
                    declared in the implementing subclass. For instance, in
                    the Function subclass, name is the name of the stored
                    procedure itself. In the case of Raw, it is the entire
                    query.
         :args    A base empty list, declaring the arguments, if any, that
                    this query requires.

         :handle   Instead of creating a new handle to run this query,
                    utilize the provided handle. This is handled implicitly
                    by the SimpleModel.

         :callback  Each row returned by the cursor will be passed to this function, which must return the row.
                      The function is applied to the psycopg row as returned by psycopg, before any other Simpycity handling.
                      Can be overriden on call-to-call basis via options= of the  __call__ methond.
        """

        self.query_base = name
        self.args = args
        self.__attr__ = {}

        self.__attr__['handle'] = handle
        self.__attr__['callback'] = callback
        self.cursor_factory = simpycity.handle.Cursor

    def __call__(self, *in_args, **in_kwargs):

        """
        Call function for a meta_query/sql_function-style object.

        Calling the function in this method causes the core SQL to be run and
        a ResultSet returned.

        :param options: A dict, which responds to the following
        keys:
        * columns: Alters what columns are selected by the query.
        * handle: Overrides the instance handle with a customized version.
        * callback: Override the instance callback with a customized version.
        :return psycopg2 cursor
        """

        d_out("meta_query.__call__: query is %s" % self.query_base)

        d_out("meta_query.__call__: Got args %s" % in_kwargs)

        opts = in_kwargs.pop('options', None)
        if opts:
            # we are going to pop out the options we handle ourselves,
            # then pass any extra options down: make a copy to avoid
            # modifying the passed options dict
            opts = opts.copy()
        else:
            opts = {}

        columns = opts.pop('columns', [])
        handle = opts.pop('handle', self.__attr__['handle'])
        callback = opts.pop('callback', self.__attr__['callback'])

        if len(columns) >= 1:
            # we are limiting the return type.
            # Eventually, we'll check it against the return object.
            # Until then, we just assume the user knows what they're
            # doing.
            cols = ",".join([x for x in columns])
            d_out("meta_query.__call__: Called with column limiters: %s" %cols)


        else:
            cols = "*"

        d_out("meta_query.__call__: Requires args: %s" % len(self.args))
        d_out("meta_query.__call__: Got args: %i" % (len(in_kwargs) + len(in_args)))

        d_out("in_args: %s" % str(in_args))
        d_out("in_kwargs: %s" % str(in_kwargs))

        # If we were called with arguments
        if len(in_args) >= 1:

            # Tests if the number of positional arguments + the number of
            # keyword arguments is less than the number of arguments this
            # instance was declared to require.

            if len(in_args) < len(self.args) \
                and len(in_kwargs) < len(self.args) \
                and len(in_kwargs) + len(in_args) < len(self.args):
                raise Exception("Insufficient arguments: Expected %s, got %s"
                                % (len(self.args), len(in_args)+len(in_kwargs)) )

            # Tests if the number of positional arguments + the number of
            # keyword arguments is GREATER than the number of arguments this
            # instance was declared to require.

            if len(in_args) > len(self.args) \
                or len(in_kwargs) > len(self.args) \
                or len(in_kwargs) + len(in_args) > len(self.args):
                raise Exception("Too many arguments: Expected %s, got %s" %
                                (len(self.args), len(in_args)+len(in_kwargs)))

        # Create a fixed-length array equal to the number of arguments
        # this instance requires.

        call_list = ['' for x in xrange(len(self.args))]

        if in_kwargs:
            # Map the incoming keyword args positionally, based on the
            # position of argument names in the core argument list.

            for arg in in_kwargs.iterkeys():
                try:
                    call_list[ self.args.index(arg) ] = in_kwargs[arg]
                except ValueError:
                    raise Exception("Unknown keyword argument passed: %s" % arg)

        for index,arg in enumerate(in_args):
            call_list[index] = arg
        d_out("meta_query.__call__: Handle is %s" % handle)
        d_out("meta_query.__call__: callback is %s" % callback)
        cur = self.__execute__(cols, call_list, handle, callback, extra_opt=opts)
        d_out("meta_query.__call__: returning cur of %s" % cur)
        return cur


    @property
    def is_property(self):
        """
        Override this for special handling in SimpleModel.__getattribute__
        If True, __call__ will be executed if a reference is made to a SimpleModel attribute
        that is this instance.
        """
        return False

    def form_query(self, columns, options={}):
        """Subclass function to create the query based on the columns
        provided at instance time.
        :return sql string
        """
        pass

    def handle(self, handle):

        """
        Permanently resets the handle.
        """
        self.__attr__['handle'] = handle

    def __execute__(self, columns, call_list, handle=None, callback=None, extra_opt={}):
        '''
        Runs the stored query in a psycopg2 cursor based on the arguments provided to
        __call__.
        If the instance handle is None and also the handle parameter is none, a handle
        is created from config.handle_factory().
        :param extra_opt: a dict passed to form_query
        :return psycopg2 cursor
        '''

        query = self.form_query(columns, options=extra_opt)

        d_out("meta_query __execute__: Handle is %s" % handle)

        if handle is None:
            if self.__attr__['handle'] is None:
                d_out("meta_query.__execute__: Did not find handle, creating new.. ")
                handle = config.handle_factory()
                self.__attr__['handle'] = handle
                d_out("meta_query.__execute__: Handle is %s" % self.__attr__['handle'])
            else:
                d_out("meta_query.__execute__: Found object handle.. ")
                handle = self.__attr__['handle']

        cursor = handle.cursor(cursor_factory=self.cursor_factory, callback=callback)
        d_out("meta_query.__execute__: Cursor is %s" % cursor)
        d_out("meta_query.__execute__: Query: %s" % ( query ) )
        d_out("meta_query.__execute__: Call List: %s" % ( call_list ) )

        try:
            cursor.execute(query, call_list)

        except psycopg2.OperationalError as e:
            # retry query on stale connection error
            d_out("OperationalError: %s" % e)

            cursor = handle.cursor()
            cursor.execute(query, call_list)

        return cursor

    def commit(self):

        """Commits the query, using the internal self.handle."""

        if self.__attr__['handle'] is not None:
            self.__attr__['handle'].commit()
        else:
            raise Exception("Could not commit, no handle found.")

    def rollback(self):

        """Rolls back the query, using the internal self.handle."""

        if self.__attr__['handle'] is not None:
            self.__attr__['handle'].rollback()
        else:
            raise Exception("Could not rollback, no handle found.")


class Function(meta_query):
    """
    Abstract a Postgresql function.
    """
    def __init__(self, *args, **kwargs):
        """
        :param name: sql function name
        :param args: list of sql argument names
        :param direct: perform direct query "SELECT func(args...)" when True,
                 vs. "SELECT * FROM func(args...)" when False (default.)
        """
        self.direct = kwargs.pop('direct', False)
        super(Function, self).__init__(*args, **kwargs)

    def form_query(self, columns, options={}):
        """
        :param columns: literal sql string for list of columns
        :param options: dict supporting a single key "direct" as in the constructor
        :return sql string
        """
        from_cl = 'FROM'

        direct = options.get('direct', self.direct)
        if direct:
            if columns != '*':
                raise ProgrammingError("Column lists cannot be specified for a direct function call.")
            columns = ''
            from_cl = ''

        if len(self.args) >= 1:
            replace = ['%s' for x in xrange(len(self.args))]
            func = "%s(" % self.query_base + ",".join(replace) + ")"
        else:
            func = "%s()" % self.query_base

        return "SELECT %s %s %s" % (columns, from_cl, func)


class FunctionSingle(Function):
    """
    A Postgresql function that returns a single value.
    """
    def __call__(self, *in_args, **in_kwargs):
        cursor = super(FunctionSingle, self).__call__(*in_args, **in_kwargs)
        if cursor.rowcount <> 1:
            raise Exception("Expect only a single row")
        row = cursor.fetchone()
        return row


class FunctionTyped(Function):
    """A Postgresql function that returns row(s) having only a single (typically composite) column"""

    def __init__(self, *args, **kwargs):
        kwargs.pop('direct',None)
        super(FunctionTyped, self).__init__(*args, **kwargs)
        self.direct = True
        self.cursor_factory = simpycity.handle.TypedCursor


class FunctionTypedSingle(FunctionSingle, FunctionTyped):
    """A Postgresql function that returns a single row having a single (typically composite) column"""
    pass

class Property(FunctionTypedSingle):
    """
    Enjoys special handling in SimpleModel.__getattribute__
    When a SimpleModel attribute references an instance of this class, its __call__
    attribute is executed immediately and the result returned. That means in
    __init__, args must be empty, or the args must be mappable by name to the model's table attribute.
    """

    @property
    def is_property(self):
        return True


class Raw(meta_query):
    """
    Execute arbitrary sql.
    """
    def __init__(self, name, args=[], handle=None, callback=None):
        """
        :param name: The raw sql
        :param args: noop
        :param handle: see superclass
        :param callback: see superclass
        """
        super(Raw, self).__init__(name, args, handle, callback)

    def form_query(self, columns, options={}):
        #TODO: use args for parameterized query
        return self.query_base

class Query(meta_query):
    """
    select query access to a Postgresql table or view.
    """
    def __init__(self, name, args=[], handle=None, callback=None):
        """
        :param name: table or view name
        :param args: list of column names used in sql WHERE clause
        :param handle: see superclass
        :param callback: see superclass
        """
        super(Query, self).__init__(name, args, handle, callback)
        self.direct = False

    def form_query(self, columns, options={}):
        where_list = None
        if len(self.args) >= 1:
            where_list = [x+"=%s" for x in self.args]

        if self.direct:
            if columns != '*':
                raise ProgrammingError("Column lists cannot be specified for a typed query call.")
            columns = 'row(t.*)::{base}'.format(base=self.query_base)

        query = "SELECT {columns} FROM {base} t".format(columns=columns, base=self.query_base)
        if where_list:
            query += " WHERE " + " AND ".join(where_list)

        return query

class QuerySingle(Query):
    """
    A select query on a table or view that returns a single row
    """
    def __call__(self, *in_args, **in_kwargs):
        cursor = super(QuerySingle, self).__call__(*in_args, **in_kwargs)
        if cursor.rowcount <> 1:
            raise Exception("Expect only a single row")
        row = cursor.fetchone()
        return row


class QueryTyped(Query):
    """A select query on a table or view that returns composite row values"""

    def __init__(self, *args, **kwargs):
        kwargs.pop('direct',None)
        super(QueryTyped, self).__init__(*args, **kwargs)
        self.direct = True
        self.cursor_factory = simpycity.handle.TypedCursor


class QueryTypedSingle(QuerySingle, QueryTyped):
    """A select query on a table or view that returns a single composite row value"""
    pass

class FunctionError(BaseException):
    """
    Bare exception, used for naming purposes only.
    """
    pass

class ProceduralException(BaseException):

    pass

