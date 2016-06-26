import unittest
from simpycity import config
from simpycity.core import *
from simpycity.model import SimpleModel, Construct
from psycopg2.extensions import cursor as _cursor
import psycopg2

import ConfigParser

handle = None

def test_handle_factory(*args, **kwargs):

    global handle
    if not handle:
        print("test_handle_factory: make new handle")
        from simpycity.handle import Handle
        handle = Handle(*args, **kwargs)

    return handle


def setUpModule():

    cfg = ConfigParser.ConfigParser()
    ini = cfg.read("test.ini")
    try:
        assert(ini)
    except AssertionError:
        print("Run the tests from the test directory")
        raise
    config.database =   cfg.get("simpycity","database")
    config.port =       cfg.get("simpycity","port")
    config.user =       cfg.get("simpycity","user")
    config.host =       cfg.get("simpycity","host")
    config.password =   cfg.get("simpycity","password")
    config.debug =      cfg.getboolean("simpycity","debug")

    config.handle_factory = test_handle_factory

    # clean up state
    h = open("sql/test_unload.sql","r")
    destroy_sql = h.read()
    h.close()

    handle = config.handle_factory() #, isolation_level=0)
    with handle.transaction():
        try:
            handle.execute(destroy_sql)
        except psycopg2.ProgrammingError:
            pass

class dbTest(unittest.TestCase):

    def setUp(self):
        h = open("sql/test.sql","r")
        create_sql = h.read()
        h.close()

        handle = config.handle_factory()
        with handle.transaction():
            handle.execute(create_sql)
        handle.commit()


    def tearDown(self):
        handle = config.handle_factory()
        handle.rollback()
        #handle.close()

        h = open("sql/test_unload.sql","r")
        destroy_sql = h.read()
        h.close()

        with handle.transaction():
            handle.execute(destroy_sql)


class ConstructTest(dbTest):

    def testConstructFunction(self):
        class o(Construct):
            r = Raw("SELECT * FROM test_table")

        instance = o()
        cur = instance.r()
        self.failUnless(
            isinstance(cur, _cursor),
            "Construct function doers not return expected result type."
        )

    def testCreateConstruct(self):

        class o(Construct):
            r = Raw("SELECT * FROM test_table")

        instance = o()
        self.assertTrue(
            isinstance(instance,Construct),
            "Construct not created successfully."
        )

class ModelTest(dbTest):

    def testCreateModel(self):

        q = SimpleReturn()
        self.assertTrue(
            isinstance(q, SimpleModel),
            "Model object not successfully created."
        )

    def testInstanceMethods(self):
        q = SimpleInstanceModel()
        self.assertTrue(
            isinstance(q, SimpleModel),
            "Model object not successfully created."
        )


    def testInstanceLoader(self):
        q = SimpleLoaderModel(id=1)
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

    def testQueryLoader(self):
        q = QueryLoaderModel(1)
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

    def testQueryTyped(self):
        handle = config.handle_factory()
        QueryTypedModel.register_composite('public.test_table', handle)
        model = QueryTypedModel(id=1, handle=handle)
        self.assertEqual(model.value, 'one', 'QueryTypedModel loads correctly')
        cur = model.get(id=2, options={'handle': handle})
        o = cur.fetchone()
        self.assertEqual(o.value, 'two', 'QueryTyped returns correctly')

    def testLazyLoad(self):
        model = SimpleLazyLoaderModel(id=1)
        self.assertEqual(
            model.value,
            "one",
            "Model value is not 'Test row', got %s" % model.value
        )

    def testTypeRegistrationLazy(self):
        handle = config.handle_factory()
        SimpleLazyLoaderModel.register_composite('public.test_table', handle)
        f = FunctionTypedSingle('test_get',['id'], handle=handle)
        model = f(id=1)
        self.assertTrue(isinstance(model, SimpleLazyLoaderModel), 'result row is registered class instance')
        self.assertEqual(
            model.value,
            "one",
            "Model value is not 'Test row', got %s" % model.value
        )

    def testInstanceFunctions(self):
        model = SimpleInstanceModel(id=1)
        cur = model.get()
        row = cur.fetchone()
        self.assertEqual(row[0], 1, 'missing Function argument supplied implicitly')
        model = SimpleUpdateModel(id=1)
        cur = model.update(new_value="Instance function test")
        # Now, the rs should have returned a single row
        self.assertEquals(cur.rowcount,1,"Update returned a single row.")
        row = cur.fetchone()
        self.assertEqual(row[0], True, "Did not successfully update, got %s" % row[0])
        model = SimpleUpdateModel(id=1)
        self.assertEqual(
            model.value,
            "Instance function test",
            "Key was not successfully set, got '%s', expected '%s'" % (
                model.value,
                "Instance function test" )
            )

    def testTypedSet(self):
        handle = config.handle_factory()
        SimpleReturn.register_composite('public.test_table', handle)
        f = FunctionTyped('test',[])
        cur = f()
        for item in cur.fetchall():
            self.assertTrue(isinstance(item, SimpleReturn), 'Each "row" in FunctionTyped cursor result is a single model object')

    def testFunctionCallback(self):
        callback_value = 'value one'
        def test_callback(row):
            print('executing callback on row: {0}'.format(repr(row)))
            model = row[0]
            model.callback_attrib = callback_value
            return row

        handle = config.handle_factory()
        SimpleReturn.register_composite('public.test_table', handle)
        f = FunctionTyped('test',[], callback=test_callback)
        cur = f()
        for model in cur.fetchall():
            self.assertTrue(model.callback_attrib == callback_value, 'Function-initiallized callback was executed on row with id={0}'.format(model.id))
        callback_value = 'value two'
        cur = f(options={'callback': test_callback})
        for model in cur:
            self.assertTrue(model.callback_attrib == callback_value, 'Function call callback was executed on row with id={0}'.format(model.id))


    def testNestedModel(self):
        handle = config.handle_factory()
        SimpleLazyLoaderModel.register_composite('public.test_table', handle)
        NestedModel.register_composite('public.nested', handle)
        model = NestedModel(id=1)
        self.assertTrue(isinstance(model, NestedModel), 'result row is registered class instance')
        self.assertEqual(
            model.value,
            "one",
            "Model value is not 'Test row', got %s" % model.value
        )
        self.assertTrue(isinstance(model.others, list), 'model.others is a list')

    def testProperty(self):
        model = SimpleInstanceModel(id=1)
        self.assertEqual(model.prop, 1, 'Property returns single object')

    def testDynamicModel(self):
        handle = config.handle_factory()
        DynamicModel.register_composite('public.test_table', handle)
        model = DynamicModel()
        self.assertEqual(model.table, SimpleReturn.table, 'table is determined automatically')


class FunctionTest(dbTest):

    def testCreateFunction(self):

        f = Function("test")
        self.failUnless(
            isinstance(f, Function),
            "Isn't a Simpycity function object."
        )


    def testExecuteFunction(self):

        f = Function("test")
        cur = f()
        self.assertEqual(cur.rowcount,3,'Error in execute of function test, expected 3 rows, got %s' % cur.rowcount)


    def testPartialReturnSet(self):
        f = Function("test")
        cur = f(options=dict(columns=['id']))
        self.assertEqual(cur.rowcount,3,"Partial Result Set does not have 3 entries.")

        for row in cur:
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
        self.assertEqual(rs.rowcount,1,"Partial with Arguments returns 1 row.")

        for row in rs:
            try:
                a = row['value']
                self.fail("Expected no value column, found value column of %s" % row['value'])
            except KeyError,e:
                pass
            except Exception, e:
                self.fail("Failed with exception: %s" %e)

class QueryTest(dbTest):

    def testBareQuery(self):

        q = Query("test_table")
        self.failUnless(
            isinstance(q, Query),
            "Return from query creation is of type sql_function."
        )
        cur = q()
        self.assertEqual(cur.rowcount,3,"Bare query result set has %s, expected 3." % cur.rowcount)

    def testWhereQuery(self):

        q = Query("test_table",['id'])
        try:
            cur = q(1)
        except Exception, e:
            self.fail("Failed with exception %s" % e)

        self.assertEqual(cur.rowcount,1,"ResultSet has a single entry")

        row = cur.fetchone()
        self.assertEqual(row['id'],1,'Return row not 1, got %s' % row['id'])
        self.assertEqual(row['value'],'one', 'Return row not "one", got %s' % row['value'])

    def testPartialReturnSet(self):
        q = Query("test_table")
        try:
            cur = q(options=(dict(columns=['id'])))
        except Exception, e:
            self.fail("Failed with exception %s" % e)

        self.assertEqual(cur.rowcount,3,"Partial Result Set has 3 entries, as expected.")

        for row in cur:
            try:
                a = row['value']
                self.fail("Expected no value column, found value column of %s" % row['value'])
            except KeyError,e:
                pass

    def testPartialWithArguments(self):
        f = Function("test",['id'])
        cur = f(1,options=dict(columns=['id']))
        self.assertEqual(cur.rowcount,1,"Partial with Arguments returns 1 row.")

        for row in cur:
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
            cur = r(1)

        except Exception, e:
            self.fail("Failed with exception %s" % e)

        self.assertEqual(cur.rowcount, 1, "ResultSet has single entry.")

        row = cur.fetchone()
        self.assertEqual(row['id'],1,'Return row not 1, got %s' % row['id'])
        self.assertEqual(row['value'],'one', 'Return row not "one", got %s' % row['value'])



class SimpleReturn(SimpleModel):

    table = ['id','value']

class SimpleInstanceModel(SimpleModel):
    table = ['id','value']

    get = Function("test_get",['id'])
    prop = Property('test_constant')

class SimpleLoaderModel(SimpleInstanceModel):
    __load__ = FunctionSingle("test_get",['id'])

class QueryLoaderModel(SimpleInstanceModel):
    __load__ = QuerySingle("test_table",['id'])

class QueryTypedModel(SimpleInstanceModel):
    __load__ = QueryTypedSingle("test_table",['id'])
    loaded_indicator = 'value'
    get = QueryTyped("test_table",['id'])

class SimpleLazyLoaderModel(SimpleReturn):
    __lazyload__ = FunctionSingle("test_get",['id'])
    loaded_indicator = 'value'

class SimpleUpdateModel(SimpleLoaderModel):

    update = Function("update_row",['id','new_value'])

class NestedModel(SimpleLazyLoaderModel):
    table = SimpleLazyLoaderModel.table + ['others']
    __lazyload__ = FunctionSingle("test_nested",['id'])

class DynamicModel(SimpleModel):
    table = []
    pg_type = ('public','test_table')

if __name__ == '__main__':
    setUpModule()
    unittest.main()
