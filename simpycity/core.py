"""
    COPYRIGHT 2008 Command Prompt, Inc.
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
from psycopg2 import extras
import re

import config
from handle import Handle

from simpycity import InternalError

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
    
    def __init__(self, name, args=[], return_type=None, handle=None):
        
       """Base initialization TODO: Doc Me Better!!!! """
        
        self.query_base = name
        self.args = args
        self.return_type = return_type
        self.attr = {}
        if handle:
            self.attr['handle'] = handle
        else:
            self.attr['handle'] = None
    
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
        
        if 'options' in in_kwargs or 'opt' in in_kwargs:
            d_out("meta_query.__call__: Found a set of options..")
            if 'options' in in_kwargs:
                
                opts = in_kwargs['options']
                d_out("meta_query.__call__: Found options=")
                del(keyargs['options'])
                
            elif 'opt' in in_kwargs:
                
                opts = in_kwargs['opt']
                d_out("meta_query.__call__: Found opt=")
                del(in_kwargs['opt'])
                
            try:
                columns = opts['columns']
            except KeyError:
                columns=[]
            
            try:
                handle = opts['handle']
                d_out("meta_query.__call__: Found handle.")
            except KeyError:
                d_out("meta_query.__call__: Couldn't set handle.")
                handle = None
                
            try:
                if 'fold_output' in opts:
                    condense = opts['fold_output']
                    d_out("meta_query.__call__: Found fold_output.")
                elif 'reduce' in opts:
                    condense = opts['reduce']
                    d_out("meta_query.__call__: found reduce")
                else:
                    condense=None
            except KeyError:
                condense=None
            
            
        else:
            columns = [] # an empty set.
            handle = None
            condense=None
        
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
                        ( len(self.args), len(in_args)+len(keyargs) )
                    )
            
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
        return self.__execute__(cols, call_list, handle, condense)
        
    
    def form_query(self, columns):
        """Subclass function to create the query based on the columns 
        provided at instance time."""
        pass
        
    def __repr__(self):
        query = self.form_query("*")
        return query
            
    def handle(self, handle):
        
        """
        Permanently resets the handle.
        """
        
        self.attr['handle'] = handle
            
    def __execute__(self, columns, call_list, handle=None, condense=False):
        
        '''
        Runs the stored query based on the arguments provided to
        __call__.
        '''

        query = self.form_query(columns)
        
        d_out("meta_query __execute__: Handle is %s" % handle)
        
        if handle is None:
            if self.attr['handle'] is None:
                d_out("meta_query.__execute__: Did not find handle, creating new.. ")
                handle = Handle(config)
                self.attr['handle'] = handle
                d_out("meta_query.__execute__: Handle is %s" % self.attr['handle'])
            else:
                d_out("meta_query.__execute__: Found object handle.. ")
                handle = self.attr['handle']
                
        
        cursor = handle.cursor(cursor_factory=extras.DictCursor)
        d_out("meta_query.__execute__: Cursor is %s" % cursor)
        d_out("meta_query.__execute__: Query: %s" % ( query ) )
        d_out("meta_query.__execute__: Call List: %s" % ( call_list ) )

        try:
            rs = cursor.execute(query, call_list)
        except InternalError, e:
            d_out("meta_query.__execute__: Caught an internal error: rolling back.")
            handle.rollback() # explicitly fix this?
            raise FunctionError(e)
            
        rs = TypedResultSet(cursor,self.return_type)
        rs.statement = query
        rs.call_list = call_list
        rs.conn = handle
        
        d_out("meta_query.__execute__: Checking for condense..")
        if condense:
            d_out("meta_query.__execute__: Found condense..")
            if len(rs) == 1:
                
                item = rs.next()
                # Let's test a little more intelligently here.
                # If we're using a return type, then we can assume that
                # a one-length return set is going to be one object wrapping
                # the return set.
                
                if self.return_type:
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
                    if self.return_type:
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
        
        if self.handle is not None:
            self.handle.commit()
        else:
            raise Exception("Could not commit, no handle found.")
            
    def rollback(self):
        
        """Rolls back the query, using the internal self.handle."""
        
        if self.handle is not None:
            self.handle.rollback()
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
        return self.wrapper(self.cursor.fetchone())
    def fetchone(self):
        return self.wrapper(self.cursor.fetchone())
    def fetchall(self):
        return [self.wrapper(x) for x in self.cursor.fetchall()]
    def commit(self):
        return self.conn.commit()
    def rollback(self):
        return self.conn.rollback()
    def wrapper(self,item):
        return item
        
    

class TypedResultSet(SimpleResultSet):
    
    def __init__(self,cursor,i_type,*args,**kwargs):
        self.cursor=cursor
        self.type=i_type
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
        i = self.type(handle=self.conn)
        for col in item.keys():
            i.set_col(col,item[col])
        return i
        
class FunctionError(BaseException):
    """
    Bare exception, used for naming purposes only.
    """
    pass

class ProceduralException(BaseException):
    
    pass
