port=None
host=None
database=None
user=None
password=None
debug=False


def dsn():
    """
    Return a libpq connection string using the variables defined in this file.
    """
    configs = {'host': host,
               'port': port,
               'dbname': database,
               'user': user,
               'password': password}
    return ' '.join(['{0}={1}'.format(_[0], _[1]) for _ in configs.iteritems() if _[1] is not None and _[1] != ''])

def handle_factory(*args, **kwargs):
    """
    Simpycity calls this to get a handle when it needs one.
    It may be useful to redefine this function with one that returns an existing instance of Handle,
    so that your application only opens one -- or a controlled number of -- database connections.
    """
    from handle import Handle
    return Handle(*args, **kwargs)
