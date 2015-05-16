__author__ = 'nolka'

class SimpleTask(object):
    __slots__ = ['index', 'data', 'result']
    def __init__(self, data, index=None):
        self.index = index
        self.data = data

    def __getitem__(self, item):
        if item in self.data.keys():
            return self.data[item]
        raise KeyError("%s not found in data collection" % item)

    def __unicode__(self):
        return u'%s, %s, %s' % (self.index, self.result, self.data)

    def __repr__(self):
        return unicode(self).encode('utf-8')

    def __str__(self):
        return unicode(self).encode('utf-8')