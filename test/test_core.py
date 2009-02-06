import unittest
from simpycity.core import Function, Query
from simpycity.model import SimpleModel
from simpycity import config
import psycopg2
from optparse import OptionParser
import sys

import ConfigParser

ini = None
mainConn = None

def setUpModule():
    
    cfg = ConfigParser.ConfigParser()
    ini = cfg.read("test.ini")
    if not ini:
        assert False
        
    config.database =   cfg.get("simpycity","database")
    config.port =       cfg.get("simpycity","port")
    config.user =       cfg.get("simpycity","user")
    config.host =       cfg.get("simpycity","host")
    config.password =   cfg.get("simpycity","password")
    
    
    mainConn = psycopg2.connect("dbname=%s user=%s password=%s port=%s host=%s" %
    
        (
            config.database,
            config.user,
            config.password,
            config.port,
            config.host
        )
    )
    h = open("test/sql/db_test.sql","r")
    create_sql = h.read()
    mainConn.execute(create_sql)
    mainConn.commit()
    
def tearDownModule():
    
    h = open("test/sql/db_test_unload.sql","r")
    destroy_sql = h.read()
    mainConn.execute(destroy_sql)
    mainConn.commit()
    mainConn.close()
    
    
class dbTest(unittest.TestCase):
    pass
class ModelTest(dbTest):
    
    def testCreateModel(self):
        
        q = SimpleReturn()
        self.failUnless(
            'SimpleModel' in [x.__name__ for x in type(q).mro()],
            "Model object not successfully created."
        )
        
    def testInstanceMethods(self):
        q = SimpleInstanceModel()
        self.failUnless(
            'SimpleModel' in [x.__name__ for x in type(q).mro()],
            "Model object not successfully created."
        )
        
    
    def testInstanceLoader(self):
        q = SimpleLoaderModel(key=1)
        self.assertEqual(
            q.id,
            1,
            "Model id is set to 1."
        )
        self.assertEqual(
            q.value,
            "Test row",
            "Model value is set to 'Test row'."
        )
        # So the model is set up correctly..
        
    def testInstanceFunctions(self):
        q = SimpleUpdateModel(key=1)
        try:
            rs = q.update(new_value="Instance function test")
            # Now, the rs should have returned a single row
            self.assertEquals(len(rs),1,"Update returned a single row.")
            row = rs.next()
            
            self.assertEqual(row[0], True, "Did not successfully update, got %s" % row[0])
            f = SimpleUpdateModel(key=1)
            self.assertEqual(
                f.col['value'],
                "Instance function test", 
                "Key was not successfully set, got '%s', expected '%s'" % (
                    f.col['value'], 
                    "Instance function test" )  
                )
            
            
        except Exception, e:
            self.fail("Exception %s during test." % e)
        
        
class FunctionTest(dbTest):
    
    def testCreateFunction(self):
        
        f = Function("test")
        self.failUnless(
            'meta_query' in [x.__name__ for x in type(f).mro()],
            "Isn't a Simpycity function object."
        )
        
    def testExecuteFunction(self):
        
        f = Function("test")
        rs = f()
        self.assertEqual(len(rs),3,'Error in execute of function test, expected 3 rows, got %s' % len(rs))
        
    def testPartialReturnSet(self):
        f = Function("test")
        rs = r(options=dict(columns=['id']))
        self.assertEqual(len(rs),3,"Partial Result Set does not have 3 entries.")
        
        for row in rs:
            self.failUnlessRaises(
                AttributeError,
                row.value,
                "Row %s 'value' column not present in return set." % row.id
            )
    def testPartialWithArguments(self):
        f = Function("test",['id'])
        rs = r(1,dict(columns=['id']))
        self.assertEqual(len(rs),1,"Partial with Arguments returns 1 row.")
        
        for row in rs:
            self.failUnlessRaises(
                AttributeError,
                row.value,
                "'value' not present in returned row, in partial with arguments test."
            )
    
    def testOutsideRange(self):
        f = Function("test",['id'])
        rs = f(4)
        self.assertEqual(len(rs),0,"Request outside range returns 0 rows.")
    
class QueryTest(dbTest):
    
    def testBareQuery(self):
        
        q = Query("test")
        self.failUnless(
            'meta_query' in [x.__name__ for x in type(q).mro()],
            "Return from query creation is of type sql_function."
        )
        rs = q()
        self.assertEqual(len(rs),3,"Bare query result set has %s, expected 3." % len(rs))
        
    def testWhereQuery(self):
        
        q = Query("test",['id'])
        rs = q(1)
        self.assertEqual(len(rs),1,"ResultSet has a single entry")
        
        row = rs.next()
        self.assertEqual(row.id,'1','Return row has ID of 1, as expected.')
        self.assertEqual(row.value,'Test row', 'Return row has value of "test row", as expected.')
    
    def testTypedReturn(self):
        q = Query("test",['id'],return_type=SimpleReturn)
        rs = q(1)
        for row in rs:
            self.assertEqual
        
    def testPartialReturnSet(self):
        q = Query("test")
        rs = q(options=(dict(colums=['id'])))
        self.assertEqual(len(rs),3,"Partial Result Set has 3 entries, as expected.")
        
        for row in rs:
            self.failUnlessRaises(
                AttributeError,
                row.value,
                "Ros %s 'value' column not present in return set" % row.id
            )
    def testPartialWithArguments(self):
        f = Function("test",['id'])
        rs = r(1,options=dict(columns=['id']))
        self.assertEqual(len(rs),1,"Partial with Arguments returns 1 row.")

        for row in rs:
            self.failUnlessRaises(
                AttributeError,
                row.value,
                "'value' not present in returned row, in partial with arguments test."
            )
    
class RawTest(dbTest):
    
    def testRunQuery(self):
        r = Raw("select * from test where id = %s",[1])
        
        self.assertEqual(len(rs), 1, "ResultSet has single entry.")
        
        row = rs.next()
        self.assertEqual(row.id,'1','Return row has ID of 1, as expected.')
        self.assertEqual(row.value,'Test row', 'Return row has value of "test row", as expected.')

class SimpleReturn(SimpleModel):
    
    table = ['id','value']
    
class SimpleInstanceModel(SimpleModel):
    table = ['id','value']
    
    get = Function("test_get",['id'])
    
class SimpleLoaderModel(SimpleInstanceModel):
    __load__ = Function("test_get",['id'])
    
class SimpleUpdateModel(SimpleLoaderModel):
    
    update = Function("update_row",['id','new_value'])
    
       
