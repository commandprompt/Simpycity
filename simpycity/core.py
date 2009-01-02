"""
    COPYRIGHT 2008 Command Prompt, Inc.
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lessor General Public License as published by
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

def d_out(text):
    
    if config.debug:
        print text

class meta_query(object):
    def __init__(self, *in_args, **kwargs):
        
        """
        Initializor for a meta_query/sql_function-style object.
        handle is a psycopg-or-equivalent database handle that has been handed
        to Simpycity.
        fold_output is 
        """
        
        self.rs = None
        try:
            if not self.query:
                self.query = ''
        except AttributeError:
            self.query = ''
        d_out("Simpycity Meta Query: %s" % self.query)
        self.call_list = []
        self.args = in_args
        
        self.keyargs = kwargs
        
        if 'options' in kwargs:
            d_out("Found a set of options..")
            opts = kwargs['options']
            del(self.keyargs['options'])
            try:
                self.columns = opts['columns']
            except KeyError:
                self.columns=[]
            
            try:
                self.i_handle = opts['handle']
                d_out("Simpycity core.__init__: Found handle.")
            except KeyError:
                d_out("Couldn't set i_handle.")
                self.i_handle = None
                pass
                
            try:
                self.condense = opts['fold_output']
            except KeyError:
                self.condense=None
                pass
            
        else:
            self.columns = [] # an empty set.
            self.handle = None
            self.condense=None
        
        if len(self.columns) >= 1:
            # we are limiting the return type.
            # Eventually, we'll check it against the return object.
            # Until then, we just assume the user knows what they're 
            # doing.
            self.cols = ",".join([x for x in self.columns])
            print self.cols
            
            
        else:
            self.cols = "*"
        d_out("Simpycity Meta Query Arg Count: %s" % self.arg_count)

        if self.args >= 1:
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
        self.form_query()
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

        if self.i_handle is None:
            d_out("Simpycity __execute__: Did not find handle, creating new.. ")
            self.i_handle = Handle(config)
            d_out("Handle is %s" % self.i_handle)
            
        cursor = self.i_handle.cursor(cursor_factory=extras.DictCursor)
        d_out("Cursor is %s" % cursor)
        d_out("Simpycity Query: %s" % (self.query))
        d_out("Simpycity Call List: %s" % (self.call_list))

        try:
            rs = cursor.execute(self.query, self.call_list)
        except psycopg2.InternalError, e:
            raise FunctionError(e)
            
        self.rs = TypedResultSet(cursor,self.r_type)
        self.rs.conn = self.i_handle

    def form_query(self):
        # This needs to be overridden
        pass 
        
    def commit(self):
        if self.rs is not None:
            self.rs.commit()
    def rollback(self):
        if self.rs is not None:
            self.rs.rollback()

def Raw(sql, args=[], return_type=None, handle=None):
    # Just let us do raw SQL, kthx.
    
    class sql_function(meta_query):
        query = sql
        creation_args = args
        r_type = return_type
        arg_count = len(args)
        i_handle = handle
        d_out("Raw Query Args Length: %s" % arg_count)
        if arg_count == 1:
            d_out("Found an argument: %s " % args[0])
        def form_query(self):
            # self.cols is ignored - unnecessary.
            
            self.query = sql
    return sql_function
    
def Query(name, where=[], return_type=None,handle=None):
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
        i_handle = handle
        def form_query(self):
            
            self.query = "SELECT " + self.cols + " FROM " + self.name 
            if self.w_list:
                self.query += " WHERE " + " AND ".join(self.w_list)
    return sql_function

def Function(name, args=[], return_type=None, handle=None):
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
        i_handle = handle
        def form_query(self):

            self.query = "SELECT "+ self.cols + " " + self.func_query

    return sql_function
    
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
