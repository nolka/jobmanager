__author__ = 'nolka'

import requests
import random

from lib.worker import BaseWorker
from lib import SimpleTask


class UrlParser(BaseWorker):
    def run(self, url):
        """
        :type url: SimpleTask
        """
        result = requests.get('%s' % url)

        if result.status_code == 200:
            return len(result.text)