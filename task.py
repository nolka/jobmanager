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
        self._message = None
        if isinstance(kwargs, dict):
            self.__dict__.update(kwargs)
        elif isinstance(kwargs, (str, unicode)):
            self.data = kwargs

    def done(self, result, message=None):
        self._result = result
        self._message = message

    def set_id(self, value):
        self._id = value


class SimpleTask(BaseTask):
    pass


class PoisonTask(BaseTask):
    pass