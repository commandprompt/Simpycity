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
#from psycopg2 import extras

import re

import config
from handle import Handle

from simpycity import InternalError

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

    def __init__(self, name, args=[], return_type=None, handle=None, returns_a="single", callback=None):

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

        """

        self.query_base = name
        self.args = args
        self.return_type = return_type
        self.__attr__ = {}
        self.returns = returns_a
        self.callback = callback

        self.__attr__['handle'] = handle

        # So now, we need to load our Exceptable exceptions, if any, and map
        # them into the exceptable object.
#        self.__get_exceptions__()

#    def __get_exceptions__(self):

#        if self.__attr__['handle']:

#            cur = self.__attr__['handle'].cursor(factory=extras.DictCursor)

#            rs = cur.execute("SELECT exception, description FROM exceptable.exceptions")

#            for row in rs.fetchall():
#                exceptions.base.map(row['exception'], row['description'])
#        else:
#            # Raise an exception? This is a somewhat odd
#            # state to end up in.
#            pass

    def __call__(self, *in_args, **in_kwargs):

        """
        Call function for a meta_query/sql_function-style object.

        Calling the function in this method causes the core SQL to be run and
        a ResultSet returned.

        It accepts an options/opt={} argument, which responds to the following
        keys:
        * columns: Alters what columns are selected by the query.
        * handle: Overrides the stored handle with a customized version.
        * fold_output/reduce: For single-row result sets, this will return only that
            row as a tuple, instead of a tuple of tuples comprising the entire
            set.
            For a single-column, single-row result set, it will return only
            that value, instead of a tuple of tuple.

        """

        try:
            if not self.query:
                self.query = ''
        except AttributeError:
            self.query = ''
        d_out("meta_query.__call__: query is %s" % self.query)
        self.call_list = []
#        self.args = in_args

        keyargs = in_kwargs

        d_out("meta_query.__call__: Got args %s" % keyargs)

        if 'options' in in_kwargs or 'opt' in in_kwargs:
            d_out("meta_query.__call__: Found a set of options..")
            if 'options' in in_kwargs:

                opts = in_kwargs['options']
                d_out("meta_query.__call__: Found options=")
                del(keyargs['options'])

            elif 'opt' in in_kwargs:

                opts = in_kwargs['opt']
                d_out("meta_query.__call__: Found opt=")
                del(keyargs['opt'])
        else:
            opts = {}

        columns = opts.get('columns', [])
        handle = opts.get('handle', None)
        condense = opts.get('reduce', False)
        ret_type = opts.get('return_type', self.return_type)


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
        d_out("meta_query.__call__: Got args: %i" % (len(keyargs) + len(in_args)))

        d_out("in_args: %s" % str(in_args))
        d_out("keyargs: %s" % str(keyargs))

        # If we were called with arguments
        if in_args >= 1:

            # Tests if the number of positional arguments + the number of
            # keyword arguments is less than the number of arguments this
            # instance was declared to require.

            if len(in_args) < len(self.args) \
                and len(keyargs) < len(self.args) \
                and len(keyargs) + len(in_args) < len(self.args):
                raise Exception("Insufficient arguments: Expected %s, got %s"
                                % (len(self.args), len(in_args)+len(keyargs)) )

            # Tests if the number of positional arguments + the number of
            # keyword arguments is GREATER than the number of arguments this
            # instance was declared to require.

            if len(in_args) > len(self.args) \
                or len(keyargs) > len(self.args) \
                or len(keyargs) + len(in_args) > len(self.args):
                raise Exception("Too many arguments: Expected %s, got %s" %
                                (len(self.args), len(in_args)+len(keyargs)))

            # Create a fixed-length array equal to the number of arguments
            # this instance requires.

            call_list = ['' for x in xrange(len(self.args))]

            if in_kwargs:
                # Map the incoming keyword args positionally, based on the
                # position of argument names in the core argument list.

                for arg in keyargs.iterkeys():
                    try:
                        call_list[ self.args.index(arg) ] = keyargs[arg]
                    except ValueError:
                        raise Exception("Unknown keyword argument passed: %s" % arg)

            for index,arg in enumerate(in_args):
                call_list[index] = arg
        d_out("meta_query.__call__: Handle is %s" % handle)
        return self.__execute__(cols, call_list, handle, condense, ret_type)


    def form_query(self, columns):
        """Subclass function to create the query based on the columns
        provided at instance time."""
        pass

    # def __repr__(self):
    #     query = self.form_query("*")
    #     return query

    def handle(self, handle):

        """
        Permanently resets the handle.
        """
        self.__attr__['handle'] = handle

#    @exceptions.system
#    @exceptions.base
    def __execute__(self, columns, call_list, handle=None, condense=False, ret_type=None):

        '''
        Runs the stored query based on the arguments provided to
        __call__.
        '''

        query = self.form_query(columns)

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


        cursor = handle.cursor()
        d_out("meta_query.__execute__: Cursor is %s" % cursor)
        d_out("meta_query.__execute__: Query: %s" % ( query ) )
        d_out("meta_query.__execute__: Call List: %s" % ( call_list ) )


        try:
            rs = cursor.execute(query, call_list)

        except psycopg2.OperationalError as e:
            # retry query on stale connection error
            d_out("OperationalError: %s" % e)

            cursor = handle.cursor()
            rs = cursor.execute(query, call_list)

        rs = TypedResultSet(cursor,ret_type,callback=self.callback)
        rs.statement = query
        rs.call_list = call_list
        rs.handle = handle

        d_out("meta_query.__execute__: Checking for condense..")
        if condense:
            d_out("meta_query.__execute__: Found condense..")
            if len(rs) == 1:

                item = rs.next()
                # Let's test a little more intelligently here.
                # If we're using a return type, then we can assume that
                # a one-length return set is going to be one object wrapping
                # the return set.

                if self.returns == "list":

                    if ret_type:
                        return [item]
                    elif len(item) == 1:
                        return [item[0]]
                    else:
                        return [item]

                elif self.returns == "single":

                    if ret_type:
                        return item
                    elif len(item) == 1:
                        # It's a list of columns, with one entry.
                        return item[0]
                    else:
                        # It's definitely a list, with multiple entries. Just
                        # return.
                        return item
            else:
                # It's larger than a single row.
                items = rs.fetchall()
                if len(items) >= 1:
                    if ret_type:
                        return items
                    elif len(items[0]) == 1:
                        return [x[0] for x in items]
                    else:
                        return items
                else:
                    # There's nothing
                    return None
        else:
            d_out("meta_query.__execute__: condense not true, returning rs of %s" % rs)
            return rs

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

    def form_query(self, columns):

        if len(self.args) >= 1:
            replace = ['%s' for x in xrange(len(self.args))]
            func = "FROM %s(" % self.query_base + ",".join(replace) + ")"
        else:
            func = "FROM %s()" % self.query_base

        return "SELECT " + columns + " " + func


class Raw(meta_query):

    def form_query(self, columns):

        return self.query_base

class Query(meta_query):

    def form_query(self, columns):
        where_list = None
        if len(self.args) >= 1:
            where_list = [x+"=%s" for x in self.args]

        query = "SELECT " + columns + " FROM " + self.query_base
        if where_list:
            query += " WHERE " + " AND ".join(where_list)

        return query

class SimpleResultSet(object):

    def __init__(self, cursor,*args,**kwargs):
        self.cursor = cursor

    def __iter__(self):
        row = self.cursor.fetchone()
        while row:
            yield row
            row = self.cursor.fetchone()
            if row is None:
                raise StopIteration()

    def __len__(self):
        if self.cursor is not None:
            return self.cursor.rowcount
        else:
            return 0

    def next(self):
        return self.fetchone()

    def fetchone(self):
        return self.wrapper(self.cursor.fetchone())

    def fetchall(self):
        return [self.wrapper(x) for x in self.cursor.fetchall()]

    def commit(self):
        return self.handle.commit()

    def rollback(self):
        return self.handle.rollback()

    def wrapper(self,item):
        return item

    def __getitem__(self, key):

        """Gets the specified index key. This has the slight problem of
        requiring the entire result set up to the requested key to be pulled
        into the result set object.

        Use with care.
        """

        if key > self.cursor.rowcount:
            raise IndexError("Index %i out of range" % key )

        if self.__store__ is not None:
            self.__store__ = self.cursor.fetchall()
        return self.__store__[key]

    def __setitem__(self, *args, **kwargs):

        raise AttributeError("Cannot set resultset values.")


class TypedResultSet(SimpleResultSet):

    def __init__(self,cursor,i_type,callback=None,*args,**kwargs):
        self.cursor=cursor
        self.type=i_type
        self.callback=callback

    def __iter__(self):
        row = self.cursor.fetchone()
        while row:
            o = self.wrapper(row)
            yield o
            row = self.cursor.fetchone()
            if row is None:
                raise StopIteration()

    def wrapper(self,item):
        if self.type is None:
            return item
        i = self.type(handle=self.handle)
        for col in item.keys():
            setattr(i, col, item[col])
        if self.callback:
            self.callback(i)
        return i

class FunctionError(BaseException):
    """
    Bare exception, used for naming purposes only.
    """
    pass

class ProceduralException(BaseException):

    pass

