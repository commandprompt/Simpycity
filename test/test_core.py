import unittest
from simpycity import config
from simpycity.core import Raw, Query, Function, FunctionSingle, FunctionTypedSingle
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
    assert(ini)

    config.database =   cfg.get("simpycity","database")
    config.port =       cfg.get("simpycity","port")
    config.user =       cfg.get("simpycity","user")
    config.host =       cfg.get("simpycity","host")
    config.password =   cfg.get("simpycity","password")
    config.debug =      cfg.getboolean("simpycity","debug")

    config.handle_factory = test_handle_factory

    # clean up state
    h = open("test/sql/test_unload.sql","r")
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
        h = open("test/sql/test.sql","r")
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

        h = open("test/sql/test_unload.sql","r")
        destroy_sql = h.read()
        h.close()

        with handle.transaction():
            handle.execute(destroy_sql)


class ConstructTest(dbTest):

    def testConstructFunction(self):
        class o(Construct):
            r = Raw("SELECT * FROM test_table")

        instance = o()
        rs = instance.r()
        self.failUnless(
            isinstance(rs, _cursor),
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
        q = SimpleUpdateModel(id=1)
        rs = q.update(new_value="Instance function test")
        # Now, the rs should have returned a single row
        self.assertEquals(rs.rowcount,1,"Update returned a single row.")
        row = rs.fetchone()
        self.assertEqual(row[0], True, "Did not successfully update, got %s" % row[0])
        f = SimpleUpdateModel(id=1)
        self.assertEqual(
            f.value,
            "Instance function test",
            "Key was not successfully set, got '%s', expected '%s'" % (
                f.value,
                "Instance function test" )
            )

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
        self.assertEqual(rs.rowcount,3,'Error in execute of function test, expected 3 rows, got %s' % rs.rowcount)


    def testPartialReturnSet(self):
        f = Function("test")
        rs = f(options=dict(columns=['id']))
        self.assertEqual(rs.rowcount,3,"Partial Result Set does not have 3 entries.")

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
            'meta_query' in [x.__name__ for x in type(q).mro()],
            "Return from query creation is of type sql_function."
        )
        rs = q()
        self.assertEqual(rs.rowcount,3,"Bare query result set has %s, expected 3." % rs.rowcount)

    def testWhereQuery(self):

        q = Query("test_table",['id'])
        try:
            rs = q(1)
        except Exception, e:
            self.fail("Failed with exception %s" % e)

        self.assertEqual(rs.rowcount,1,"ResultSet has a single entry")

        row = rs.fetchone()
        self.assertEqual(row['id'],1,'Return row not 1, got %s' % row['id'])
        self.assertEqual(row['value'],'one', 'Return row not "one", got %s' % row['value'])

    def testPartialReturnSet(self):
        q = Query("test_table")
        try:
            rs = q(options=(dict(columns=['id'])))
        except Exception, e:
            self.fail("Failed with exception %s" % e)

        self.assertEqual(rs.rowcount,3,"Partial Result Set has 3 entries, as expected.")

        for row in rs:
            try:
                a = row['value']
                self.fail("Expected no value column, found value column of %s" % row['value'])
            except KeyError,e:
                pass

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

class RawTest(dbTest):

    def testRunQuery(self):
        r = Raw("select * from test_table where id = %s",['id'])
        try:
            rs = r(1)

        except Exception, e:
            self.fail("Failed with exception %s" % e)

        self.assertEqual(rs.rowcount, 1, "ResultSet has single entry.")

        row = rs.fetchone()
        self.assertEqual(row['id'],1,'Return row not 1, got %s' % row['id'])
        self.assertEqual(row['value'],'one', 'Return row not "one", got %s' % row['value'])



class SimpleReturn(SimpleModel):

    table = ['id','value']

class SimpleInstanceModel(SimpleModel):
    table = ['id','value']

    get = Function("test_get",['id'])

class SimpleLoaderModel(SimpleInstanceModel):
    __load__ = FunctionSingle("test_get",['id'])

class SimpleLazyLoaderModel(SimpleReturn):
    lazyload = FunctionSingle("test_get",['id'])
    loaded_indicator = 'value'

class SimpleUpdateModel(SimpleLoaderModel):

    update = Function("update_row",['id','new_value'])

class NestedModel(SimpleLazyLoaderModel):
    table = SimpleLazyLoaderModel.table + ['others']
    lazyload = FunctionSingle("test_nested",['id'])


if __name__ == '__main__':
    setUpModule()
    unittest.main()
