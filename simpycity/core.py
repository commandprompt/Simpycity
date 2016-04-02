"""
    COPYRIGHT 2009 Command Prompt, Inc.
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
from psycopg2.errorcodes import *
from psycopg2.extensions import cursor as _cursor
#from psycopg2 import extras

import config

from simpycity import ProgrammingError

#from simpycity import exceptions

def d_out(text):

    if config.debug:
        print text


class meta_query(object):

    """Base object for Simpycity.

       Functions:
         * __init__(self, name, args=[],return_type=None, handle=None):
           Initializes the Simpycity object, as expected. Most classes need
           to override and super() this function, to add additional logic
           parsing to the query generator.

         * __call__(self, *in_args,**in_args):
           Heavy lifting. Takes the provided arguments (positional and keyword)
           and correctly maps them to your query. Returns a ResultSet object
           of your choice, optionally.

         * form_query(self, columns):
           Stub function, needs to be implemented by subclasses. Does what
           little logic is necessary for the generation of a query.

         * set_handle(self, handle):
           Sets the handle used by the query object.

         * commit(self):
           Forcibly commits the handle

         * rollback(self):
           Forcibly rolls back the handle.
    """

    def __init__(self, name, args=[], handle=None):

        """

         * name=    Sets the base name of the query. How this is used will be
                    declared in the implementing subclass. For instance, in
                    the Function subclass, name is the name of the stored
                    procedure itself. In the case of Raw, it is the entire
                    query.
           args=[]  A base empty list, declaring the arguments, if any, that
                    this query requires.

           return_type=  For use with the SimpleModel. Takes a SimpleModel as
                         its argument, and maps all returned keys to the
                         SimpleModel object.
                         ** TODO **
                         Make return_type work with *any* object.

           handle=  Instead of creating a new handle to run this query,
                    utilize the provided handle. This is handled implicitly
                    by the SimpleModel.

           returns_a=   A specific setting for the reduce= option implemented
                        during the execution stage. Specifically, certain
                        queries can be declared to exist as always expecting
                        the return of a list. In these cases, it is most sane
                        to allow a simple flag to say "I expect a list.".

           callback=  A callable to call on every fetched record.  Can
                      be overriden on call-to-call basis via options= of the
                      __call__ methond.

        """

        self.query_base = name
        self.args = args
        self.__attr__ = {}

        self.__attr__['handle'] = handle
        self.cursor_factory = None #optionally override this

    def __call__(self, *in_args, **in_kwargs):

        """
        Call function for a meta_query/sql_function-style object.

        Calling the function in this method causes the core SQL to be run and
        a ResultSet returned.

        It accepts an options/opt={} argument, which responds to the following
        keys:
        * columns: Alters what columns are selected by the query.
        * handle: Overrides the stored handle with a customized version.
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
        handle = opts.pop('handle', None)

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
        cur = self.__execute__(cols, call_list, handle, extra_opt=opts)
        d_out("meta_query.__call__: returning rs of %s" % cur)
        return cur


    def form_query(self, columns, options={}):
        """Subclass function to create the query based on the columns
        provided at instance time."""
        pass

    def handle(self, handle):

        """
        Permanently resets the handle.
        """
        self.__attr__['handle'] = handle

    def __execute__(self, columns, call_list, handle=None, extra_opt={}):
        '''
        Runs the stored query based on the arguments provided to
        __call__.
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

        cursor = handle.cursor(cursor_factory=self.cursor_factory)
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

    def __init__(self, *args, **kwargs):
        """
        direct=  perform direct query "SELECT func(args...)" when True,
                 vs. "SELECT * FROM func(args...)" when False (default.)
        """
        self.direct = kwargs.pop('direct', False)
        super(Function, self).__init__(*args, **kwargs)

    def form_query(self, columns, options={}):

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


class TypedCursor(_cursor):
    """
    A cursor for result sets having only a single (typically composite) column.
    Rather than a row being a tuple, it is simply the value of the one column.
    """
    def execute(self, query, vars=None):
        super(TypedCursor, self).execute(query, vars)
        if len(self.description) != 1:
            raise Exception("Cursor must return exactly one column")

    def fetchone(self):
        row = super(TypedCursor, self).fetchone()
        return row[0]

    def fetchall(self):
        rows = super(TypedCursor, self).fetchall()
        rows = [_[0] for _ in rows]
        return rows

    def fetchmany(self, size=None):
        rows = super(TypedCursor, self).fetchmany(size)
        rows = [_[0] for _ in rows]
        return rows


class FunctionSingle(Function):

    def __call__(self, *in_args, **in_kwargs):
#         if 'options' not in in_kwargs:
#             in_kwargs['options'] = {}
#         in_kwargs['options']['reduce'] = True
        cursor = super(FunctionSingle, self).__call__(*in_args, **in_kwargs)
        if cursor.rowcount <> 1:
            raise Exception("Expect only a single row")
        row = cursor.fetchone()
        return row


class FunctionTyped(Function):
    """Expect the result set to have only a single (typically composite) column"""

    def __init__(self, *args, **kwargs):
        self.direct = True
        kwargs.pop('direct',None)
        super(Function, self).__init__(*args, **kwargs)
        self.cursor_factory = TypedCursor


class FunctionTypedSingle(FunctionSingle, FunctionTyped):
    """Expect the result set to be a single row with a single (typically composite) column"""
    pass

# enjoys special handling in SimpleModel.__getattribute__
class Property(FunctionTypedSingle):
    pass


class Raw(meta_query):

    def form_query(self, columns, options={}):

        return self.query_base

class Query(meta_query):

    def form_query(self, columns, options={}):
        where_list = None
        if len(self.args) >= 1:
            where_list = [x+"=%s" for x in self.args]

        query = "SELECT " + columns + " FROM " + self.query_base
        if where_list:
            query += " WHERE " + " AND ".join(where_list)

        return query

class FunctionError(BaseException):
    """
    Bare exception, used for naming purposes only.
    """
    pass

class ProceduralException(BaseException):

    pass

