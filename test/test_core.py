import unittest
from simpycity import config
from simpycity.core import Function as BaseFunction, Query as BaseQuery, Raw as BaseRaw
from simpycity.model import SimpleModel as BaseSimpleModel, Construct as BaseConstruct
from simpycity.handle import Handle
import psycopg2
from optparse import OptionParser
import sys

import ConfigParser

handle = None

def setUpModule():
    
    cfg = ConfigParser.ConfigParser()
    ini = cfg.read("test.ini")
    assert(ini)

    config.database =   cfg.get("simpycity","database")
    config.port =       cfg.get("simpycity","port")
    config.user =       cfg.get("simpycity","user")
    config.host =       cfg.get("simpycity","host")
    config.password =   cfg.get("simpycity","password")
    config.debug = True

    # clean up state
    h = open("test/sql/test_unload.sql","r")
    destroy_sql = h.read()
    h.close()

    global handle
    handle = Handle(config=config)
    try:    
        handle.cursor().execute(destroy_sql)
    except psycopg2.ProgrammingError:
        pass
    handle.commit()


class dbTest(unittest.TestCase):
    
    def setUp(self):
        h = open("test/sql/test.sql","r")
        create_sql = h.read()
        h.close()

        global handle
        handle.cursor().execute(create_sql)
        handle.commit()
        handle.close()
    
    def tearDown(self):
        global handle

        handle.rollback()
        handle.close()

        h = open("test/sql/test_unload.sql","r")
        destroy_sql = h.read()
        h.close()

        handle.cursor().execute(destroy_sql)
        handle.commit()
        handle.close()


def with_global_handle(kwargs):
    global handle
    kwargs['handle'] = handle
    return kwargs

def Function(*args, **kwargs):
    return BaseFunction(*args, **with_global_handle(kwargs))

def Query(*args, **kwargs):
    return BaseQuery(*args, **with_global_handle(kwargs))

def Raw(*args, **kwargs):
    return BaseRaw(*args, **with_global_handle(kwargs))


class ConstructTest(dbTest):
    
    def testConstructFunction(self):
        class o(Construct):
            r = Raw("SELECT * FROM test_table")
        
        instance = o()
        rs = instance.r()
        self.failUnless(
            'TypedResultSet' in [x.__name__ for x in type(rs).mro()],
            "Construct function doers not return expected result set."
        )

    def testCreateConstruct(self):
        
        class o(Construct):
            r = Raw("SELECT * FROM test_table")
        
        instance = o()
        self.failUnless(
            'Construct' in [x.__name__ for x in type(instance).mro()],
            "Construct not created successfully."
        )
        
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
        q = SimpleLoaderModel(id=1)
        q.commit()
        self.assertEqual(
            q.id,
            1,
            "Model id is set to 1."
        )
        self.assertEqual(
            q.value,
            "one",
            "Model value is not 'Test row', got %s" % q.value
        )
        # So the model is set up correctly..
        
    def testInstanceFunctions(self):
        q = SimpleUpdateModel(id=1)
        try:
            rs = q.update(new_value="Instance function test")
            # Now, the rs should have returned a single row
            self.assertEquals(len(rs),1,"Update returned a single row.")
            row = rs.next()
            rs.commit()
            self.assertEqual(row[0], True, "Did not successfully update, got %s" % row[0])
            f = SimpleUpdateModel(id=1)
            self.assertEqual(
                f.value,
                "Instance function test", 
                "Key was not successfully set, got '%s', expected '%s'" % (
                    f.value, 
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
        rs = f(options=dict(columns=['id']))
        rs.commit()
        self.assertEqual(len(rs),3,"Partial Result Set does not have 3 entries.")
        
        for row in rs:
            try:
                a = row['value']
                self.fail("Expected no value column, found value column of %s" % row['value'])
            except KeyError,e:
                pass 
            except Exception, e:
                self.fail("Failed with exception: %s" %e)
        
                
    def testPartialWithArguments(self):
        f = Function("test",['id'])
        rs = f(1,options=dict(columns=['id']))
        rs.commit()
        self.assertEqual(len(rs),1,"Partial with Arguments returns 1 row.")
        
        for row in rs:
            try:
                a = row['value']
                self.fail("Expected no value column, found value column of %s" % row['value'])
            except KeyError,e:
                pass
            except Exception, e:
                self.fail("Failed with exception: %s" %e)

    
    def testOutsideRange(self):
        f = Function("test",['id'])
        rs = f(4)
        self.assertEqual(len(rs),0,"Request outside range returns 0 rows.")

    
class QueryTest(dbTest):
    
    def testBareQuery(self):
        
        q = Query("test_table")
        self.failUnless(
            'meta_query' in [x.__name__ for x in type(q).mro()],
            "Return from query creation is of type sql_function."
        )
        rs = q()
        self.assertEqual(len(rs),3,"Bare query result set has %s, expected 3." % len(rs))
        
    def testWhereQuery(self):
        
        q = Query("test_table",['id'])
        try:
            rs = q(1)
        except Exception, e:
            self.fail("Failed with exception %s" % e)
            
        self.assertEqual(len(rs),1,"ResultSet has a single entry")
        
        row = rs.next()
        rs.commit()
        self.assertEqual(row['id'],1,'Return row not 1, got %s' % row['id'])
        self.assertEqual(row['value'],'one', 'Return row not "one", got %s' % row['value'])
    
    def testTypedReturn(self):
        q = Query("test_table",['id'],return_type=SimpleReturn)
        try:
            rs = q(1)
            
        except Exception, e:
            self.fail("Failed with exception %s" % e)
        rs.commit()
        for row in rs:
            self.failUnless(
                'SimpleReturn' in [x.__name__ for x in type(row).mro()],
                "Return from Typed Return is not SimpleReturn."
            )
        
    def testPartialReturnSet(self):
        q = Query("test_table")
        try:
            rs = q(options=(dict(columns=['id'])))
        except Exception, e:
            self.fail("Failed with exception %s" % e)
            
        self.assertEqual(len(rs),3,"Partial Result Set has 3 entries, as expected.")
        
        for row in rs:
            try:
                a = row['value']
                self.fail("Expected no value column, found value column of %s" % row['value'])
            except KeyError,e:
                pass
                
    def testPartialWithArguments(self):
        f = Function("test",['id'])
        rs = f(1,options=dict(columns=['id']))
        self.assertEqual(len(rs),1,"Partial with Arguments returns 1 row.")

        for row in rs:
            try:
                a = row['value']
                self.fail("Expected no value column, found value column of %s" % row['value'])
            except KeyError,e:
                pass
            except Exception, e:
                self.fail("Failed with exception: %s" %e)
    
class RawTest(dbTest):
    
    def testRunQuery(self):
        r = Raw("select * from test_table where id = %s",['id'])
        try:
            rs = r(1)
            
        except Exception, e:
            self.fail("Failed with exception %s" % e)
        
        self.assertEqual(len(rs), 1, "ResultSet has single entry.")
        
        row = rs.next()
        rs.commit()
        self.assertEqual(row['id'],1,'Return row not 1, got %s' % row['id'])
        self.assertEqual(row['value'],'one', 'Return row not "one", got %s' % row['value'])


class Construct(BaseConstruct):
    def __init__(self, *args, **kwargs):
        global handle
        kwargs['handle'] = handle
        super(Construct, self).__init__(*args, **kwargs)

class SimpleModel(BaseSimpleModel):
    def __init__(self, *args, **kwargs):
        global handle
        kwargs['handle'] = handle
        super(SimpleModel, self).__init__(*args, **kwargs)

class SimpleReturn(SimpleModel):
    
    table = ['id','value']
    
class SimpleInstanceModel(SimpleModel):
    table = ['id','value']
    
    get = Function("test_get",['id'])
    
class SimpleLoaderModel(SimpleInstanceModel):
    __load__ = Function("test_get",['id'])
    
class SimpleUpdateModel(SimpleLoaderModel):
    
    update = Function("update_row",['id','new_value'])
    
       
if __name__ == '__main__':
    setUpModule()
    unittest.main()
