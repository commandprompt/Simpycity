Simpycity -- Aurynn Shaw -- July 8, 2010

* What Simpycity Is *

Simpycity is a simple mapping system for Python that allows for arbitrary SQL
queries to be mapped to Python callables in a consistent and predictable
manner, as well as providing a model system that takes advantage of those
callable methods, allowing for the easy and rapid development of query- and 
stored procedure-based data representations.

* What Simpycity is Not *

Simpycity is not a query generator, and by default it does not support query 
generation. Simpycity is designed for developers who deeply understand SQL and
desire to write the best possible SQL representations for their database.

* Core Philosophy *

The core philosophy behind Simpycity is that the Database and the Application 
are separate entities, each with distinct abilities and design 
representations - the classic Object versus Relation argument.
By providing a mechanism where a single business Object can easily represent
several Relations, and allow the base Relational layer to follow normal forms
without compromising or complication application design.

* Usage *

The majority of Simpycity usage is detailed on the Wiki, and the Wiki will be
the most up-to-date source of information.
We will keep the docs/ folder updated as new usage documentation is created.

** Context **

To begin a Simpycity session, first we need to instance the base Context.
The Context encapsulates all queries and models around a single connection 
handle, 

To instance a Context:
{{{
>>> from simpycity.context import Context
>>> 
>>> ctx = Context(dsn="database=%s user=%s password=%s" % \
... (database, username, password))
}}}

The connection string is, for psycopg2, identical to the standard psycopg2 
connection string. As other backends are supported, this will change to 
represent backend-specific connection logic.

** Basic Queries **

Now that a Context has been initialized, we can create some basic queries.

A basic query is in the form of:
Function(function_name, <args=[]>)
and
Query(query_string, <args=[]>)

The function name and query string are required, however, the argument list is
optional.

{{{
>>>> my_sproc = ctx.Function("my_getter", ['id'])
}}}
or, for raw SQL:

{{{
>>> my_raw = ctx.Raw("SELECT id FROM my_table")
}}}

my_raw and my_sproc are now usable via:

{{{
>>> rs = my_raw()
>>> for i in rs:
...   print i['id']
}}}

and

{{{
>>> rs = my_sproc(1)
>>> for i in rs:
...   print i['id']
}}}

Each iterable is presently implemented as a dict of all the columns in the 
returned set.

** Basic Models **

Models in Simpycity are the meat of the functionality, allowing for complex 
data representations.

To instance a basic Model in Simpycity, we:

{{{
class myModel(ctx.Model()):
     table = ['id','value']
     __load__ = ctx.Function("get_by_id", ['id'])
     __save__ = ctx.Function("update_table",['id','value'])
     
search = ctx.Function(
    "my_table_search", 
    ['value'], 
    return_type=myModel, 
    returns_a="list")
}}}

This covers some fundamental capabilities of the Simpycity model and query 
classes.

*** table = [] ***

The table= declaration in a Simpycity model names the columns that a given 
Model has. These are directly mapped from the source query - Any dictionary 
set that matches the table declaration can be used to instance a Model.

Additionally, every item defined in table= is supported via .column. This will
update the internal model representation, and can be persisted via .save().

Further, any Queries or Functions bound to the object will have their named 
parameters mapped from the table definition. For example:
{{{
    
class myModel(ctx.Model()):
    ...
    delete = ctx.Function("delete_by_id", ['id'])
}}}

By calling myModelInstance.delete(), without arguments, Simpycity will
automatically map the internal columns to the function arguments.

*** __load__ ***

__load__ is the Simpycity function that supports the *primary* mechanism for 
instancing from the database.
What this means is, when a developer calls
{{{
>>> m = myModel(1)
}}}

the __load__ function will be used.

*** __save__ ***

The __save__ function allows the Active Record-style pattern of a model
supporting a .save() method. 
__save__ supports no arguments.

*** search= ***

This declaration is interesting, and provides the cornerstones of Simpycity 
API development.

The two most interesting pieces here are
return_type
and
returns_a

return_type takes a class definition as its argument, and the Simpycity query
will attempt to use that class to represent the returned row.

returns_a defaults to "single", but also accepts "list" as an argument. When 
defined as "list", any row-set provided to Simpycity will be represented as a
list, even if there was only a single entry.

By default, Simpycity would return the single object alone.

This allows Simpycity to perform offer a wider API to your application, that
is more sensibly delineated than might normally be available. As an example:

{{{
from yourApp.model import aModel
m = aModel.model(1)
s = aModel.search("pattern")
id = aModel.by_id(1)
}}}

This is discussed in greater depth on the Wiki.

* License *

Simpycity is licensed under the LGPL license, and a copy of your rights and
permissions is available in the LICENSE file included in your distribution.

* Contact *

For support, questions, and additional help with Simpycity, please feel free 
to contact us.
Our mailing list is at https://lists.commandprompt.com/mailman/listinfo/simpycity,
and our Wiki can be reached at
https://public.commandprompt.com/projects/simpycity/wiki