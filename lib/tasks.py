__author__ = 'nolka'


class BaseTask(object):
    @property
    def id(self):
        return self._id

    @property
    def result(self):
        return self._result

    def __init__(self, index=None, **kwargs):
        self._id = index
        self._result = None
        if isinstance(kwargs, dict):
            for k, v in kwargs.iteritems():
                setattr(self, k, v)
        elif isinstance(kwargs, (str, unicode)):
            self.data = kwargs

    def done(self, result):
        self._result = result

    def set_id(self, value):
        self._id = value


class SimpleTask(BaseTask):
    pass


class PoisonTask(BaseTask):
    pass