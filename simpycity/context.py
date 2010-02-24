from simpycity.model import SimpleModel
from simpycity import config as dbc
from simpycity.handle import Handle

from simpycity.core import Raw, Function

import logging
log = logging.getLogger(__name__)

class Context(object):

    def __init__(self, dsn=None):

        if dsn is not None:
            self.dsn = dsn
            self.__handle__ = Handle(dsn=dsn)
        else:
            self.__handle__ = handle

            if config is not None:
                self.config = config
            else:
                self.config = dbc

    def Raw(self, *args, **kwargs):
        kwargs['handle'] = self.handle
        return Raw(*args,**kwargs)
    def Function(self, *args, **kwargs):
        kwargs['handle'] = self.handle
        return Function(*args,**kwargs)

    def Model(self, *args, **kwargs):
        this_handle = self.handle
        class internalModel(SimpleModel):

            def __init__(self, *args, **kwargs):
                if 'handle' in kwargs:
                    del (kwargs['handle'])
                kwargs['handle'] = this_handle
                super(internalModel, self).__init__(*args, **kwargs)
        return internalModel

    def commit(self):
        self.handle.commit()

    def rollback(self):
        self.handle.rollback()

    def close(self):
        self.handle.close()

    @property
    def handle(self):
        if self.__handle__ is None:
            self.__handle__ = Handle(dsn=self.dsn)
            if hasattr(self.config):
                log.debug("Using db user of %s" % self.config.user)
            else:
                log.debug("Using DSN of %s" % self.dsn)

        if self.__handle__.conn.closed:
            self.__handle__.__reconnect__()

        return self.__handle__

