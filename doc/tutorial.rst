This tutorial walks you through a simple console program that displays information about animals. First, we have one table in a database of animals::

    create table animal
    (
        id int primary key,
        genus_id int,
        species text,
        common_name text,
        extinct boolean
    );


Basic Models
============

Our Python model library will start like this::

    import simpycity.model
    import simpycity.core

    class Animal(simpycity.model.SimpleModel):
        table = ['id',
                'genus_id',
                'species',
                'common_name',
                'extinct']

        __load__ = simpycity.core.QuerySingle('public.animal', ['id'])

SimpleModel is the class that connects PostgreSQL types to Python classes. Programmers unfamiliar with PostgreSQL may be unfamiliar with the concept of "type" in the context of a database. In PostgreSQL, every table and view defines a row type. Additionally, composite data types can be defined and functions declared to return values or sets of such types. Simpycity supports mapping of these formally defined types -- and also adhoc query result sets -- to Python classes.

At the minimum, a SimpleModel needs to know what PostgreSQL type (or at least what result set column names) it maps to, and it needs to now how to query the database. Above, we explicitly list the columns from the animal table; later we'll see how that can be left blank and done automatically. The ``__load__`` attribute in this case defines the way to query the data as a simple query on a table by an id column. The analogous ``__lazyload__`` attribute does the same thing but only queries the database at the point where an attribute reference requires it.

Our main script file looks like this to start::

    import simpycity.config
    import animals

    simpycity.config.port = 5434
    simpycity.config.database = 'animals'
    simpycity.config.user = 'ormboss'

    animal = animals.Animal(id=3)
    print(animal.common_name)

And its output is *"racoon"*. When the ``Animal`` object is instantiated, the ``__load__`` attribute signals to immediately query the database and populate the class with attributes named for the members of ``table``. So ``animal.common_name`` is already populated before the print function runs.


Querying Models
===============

Data accessor methods can easily be added to our SimpleModel.  Here is a silly ``mammals`` function in sql::

    CREATE FUNCTION mammals() RETURNS setof animal AS
    $body$
        SELECT * FROM animal where id in (1,3,7);
    $body$ language sql;

We map this to our Animals class by adding::

    mammals = simpycity.core.Function('public.mammals', [])

To our main script we add::

    for mammal in animal.mammals():
        print(repr(mammal))

which outputs psycopg2 DictRows::

    [1, 2, 'sapiens', 'human', False]
    [3, 3, 'lotor', 'racoon', False]
    [7, 7, 'hemionus', 'mule deer', False]

Simpycity uses the psycopg2 library to execute queries, and utilizes its ``DictRow`` option, which causes result sets to behave like both lists and dicts, so ``print(mammal['common_name'])`` outputs *racoon*.

The ``Function`` class returns a psycopg2 cursor.  The ``QuerySingle`` and ``FunctionSingle`` classes returns the single row from a sql query/function that returns a single row.

Soon we'll see how to return Python class instances in result sets instead of simple scalar values. First, we have to look at database connections.


Database Connections and Handles
================================

When your code is running a bunch of queries, you normally want them running in a single connection and often in a single transaction. The type-mapping feature described in the next section depends on the app utilizing a single connection.

Simpycity's ``Handle`` class wraps a psycopg2 connection and cursors. You need all your code to use the same ``Handle`` object.  Add this under the ``config`` lines at the top of the script::

    handle = simpycity.handle.Handle()

    def handle_factory(*args, **kwargs):
        return handle

    simpycity.config.handle_factory = handle_factory

Now, any Simpycity object that needs a handle will use the one that has already been created from the ``config`` values. Before we added these lines, the script created a new connection handle every time a handle was needed.


Mapping SQL Types to Python Classes
===================================

Returning lists from a query is a weak form of mapping. We can return Python classes from our queries instead. In our main script before the line instantiating ``animal`` we add::

    animals.Animal.register_composite('public.animal')

This magical line tells psycopg2 to pass column values to the ``Animal`` constructor whenever a PostgreSQL type of ``public.animal`` is returned. The ``Animal`` constructor creates instance attributes from the column values.  Now in our library code we can replace::

    mammals = simpycity.core.Function('public.mammals', [])

with::

    mammals = simpycity.core.FunctionTyped('public.mammals', [])

And in our main script we replace::

    print(mammal['common_name'])

with::

    print(mammal.common_name)

because each ``mammal`` is now an instance of ``Animal``, not a psycopg2 ``DictRow``. If your query only returns a single row, then you can use the ``FunctionTypedSingle`` class, or ``QueryTypedSingle`` class for table/view queries. For semantic tidiness you can use the ``Property`` class just like ``FunctionTypedSingle``, except that a ``Property`` attribute cannot be called. For example::

    CREATE FUNCTION human() RETURNS animal AS
    $body$
        SELECT * FROM animal where id = 1;
    $body$ language sql;

    class Animal(simpycity.model.SimpleModel):
        ...
        master = simpycity.core.Property('public.human', [])

    animal = animals.Animal(id=3)
    print(animal.master.species)
    >>>sapiens

Automatic Table Attribute
-------------------------

Because psycopg2 is deeply familiar with PostgreSQL, we can leverage type mapping so that figuring the ``table`` attribute, ie. list of column names, is completely automatic. In our ``Animal`` class we add a class attribute ``pg_type``, and then omit ``table``::

    pg_type = ('public', 'animal')
    #table = []

The script output will remain identical.


Type and Class Inheritance
==========================

Of course Python supports class inheritance and this feature is extremely powerful and useful. Many people don't realize that PostgreSQL also supports it. Simpycity maps the two together::

    create table predator (tactic text, primary key (id)) inherits (animal);
    insert into predator select * from animal where id in (1,6,10);
    delete from only animal where id in (1,6,10);
    update predator set tactic = 'shoot' where species = 'sapiens';
    update predator set tactic = 'gulp' where species = 'catesbeiana';
    update predator set tactic = 'chomp' where species = 'rex';

    create table predation
    (
        predator_id int references predator (id),
        prey_id int references animal (id)
    );

    copy predatation from predator.csv with (format csv)

    create type predator_type as
    (
        base_ animal,
        tactic text,
        prey animal[]
    );

``base_`` is simply a special name given to an inherited type, in this case, ``animal``.  ``animal[]`` is an array of prey animal types. The following function returns a single composite ``predator_type`` representing a predator given an id::

    create or replace function predator(id int)
    returns predator_type
    language sql as
    $$
    select
        row(p1.id, p1.genus_id, p1.species, p1.common_name, p1.extinct)::animal,
        p1.tactic,
        array_agg(row(prey.*)::animal)::animal[]
    from 
        predator p1
        join predation p2 on p1.id = p2.predator_id
        join animal prey on prey.id = p2.prey_id
    where p1.id = $1
    group by 1,2;
    $$;

Now inherit the base class. If you want ``table`` to be automatically figured, do not override it::

    class Predator(Animal):
        pg_type = ('public', 'predator_type')
        __load__ = simpycity.core.FunctionTypedSingle('public.predator', ['id'])

In the main script be sure to register this class *after* the superclass::

    animals.Predator.register_composite('public.predator_type')
    trex = animals.Predator(id=10)
    print("{0}, {1}!".format(trex.common_name, trex.tactic))
    for victim in trex.prey:
        print('Victim: ' + victim.common_name)
    >>>tyranosaurus rex, chomp!
    >>>Victim: mule deer
    >>>Victim: elephant
