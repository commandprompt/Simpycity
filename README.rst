Overview
========

What Simpycity Is
-----------------

Simpycity is an object-relational mapper. It seamlessly maps PostgreSQL query and
function result sets to Python classes and class attributes.

It allows for the easy and rapid development of query- and
stored procedure-based data representations. Simpycity leverages PostgreSQL's
powerful composite type system, and the advanced type handling of the psycopg2
database access library.

What Simpycity is Not
---------------------

Simpycity is not a query generator, and by default it does not support query
generation. Simpycity is designed for developers who deeply understand SQL and
desire to write the best possible SQL representations for their database.

Core Philosophy
---------------

The core philosophy behind Simpycity is that the Database and the Application
are separate entities, each with distinct abilities and design
representations - the classic Object versus Relation argument.
By providing a mechanism where a single business Object can easily represent
several Relations, and allow the base Relational layer to follow normal forms
without compromising or complicating application design.

Usage
=====

At its simplest, object-relation mapping looks like::

    --SQL
    create table foo (id int, name text);
    insert into foo (id, name) values (1, 'one'), (2, 'two');

    #Python
    class Foo(simpycity.model.SimpleModel):
        pg_type = ('public', 'foo')
        __load__ = simpycity.core.QuerySingle('foo',['id'])

    my_foo = Foo(1)
    print(my_foo.name)
    >>>one

Read the tutorial for more narrative help. The doc directory also includes formal class documentation.

License
=======

Simpycity is licensed under the LGPL license, and a copy of your rights and
permissions is available in the LICENSE file included in your distribution.

Contact
=======

The official source repository is https://github.com/commandprompt/Simpycity

For support, questions, and additional help with Simpycity, please feel free
to contact us on github.
