port=None
host=None
database=None
user=None
password=None
debug=False


def dsn():
    configs = {'host': host,
               'port': port,
               'dbname': database,
               'user': user,
               'password': password}
    return ' '.join(['{0}={1}'.format(_[0], _[1]) for _ in configs.iteritems() if _[1] is not None])

def handle_factory(*args, **kwargs):
    from handle import Handle
    return Handle(*args, **kwargs)
