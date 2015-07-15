__author__ = 'nolka'

class BaseTask(object):
    @property
    def id(self):
        return self._id

    @property
    def result(self):
        return self._result

    def __init__(self, data, index=None):
        self._id = index
        self._result = None
        if isinstance(data, dict):
            for k,v in data.iteritems():
                setattr(self, k,v)
        elif isinstance(data, (str, unicode)):
            self.data = data

    def done(self, result):
        self._result = result

class SimpleTask(BaseTask):
    pass


class PoisonTask(BaseTask):
    pass