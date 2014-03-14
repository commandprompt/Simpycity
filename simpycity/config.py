port=5432
host="localhost"
database=None
user=None
password=None
debug=False


def dsn():
    return "host=%s port=%s dbname=%s user=%s password=%s" % \
           (host, port, database, user, password)

def handle_factory(*args, **kwargs):
    from handle import Handle
    return Handle(*args, **kwargs)
