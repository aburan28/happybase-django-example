
import json
import logging
import random
import threading

from django.conf import settings
from django.http import HttpResponse

import happybase

logger = logging.getLogger(__name__)

N_KEYS = 10000

#
# Initialization
#

pool = happybase.ConnectionPool(
    size=3,
    host=settings.HBASE_HOST)


def populate_table():
    with pool.connection() as connection:
        connection.delete_table(settings.HBASE_TABLE, disable=True)
        connection.create_table(
            settings.HBASE_TABLE,
            families={'cf': {}}
        )
        table = connection.table(settings.HBASE_TABLE)
        with table.batch() as b:
            for i in xrange(N_KEYS):
                row_data = {'cf:col1': 'value-%d' % i}
                b.put('row-key-%d' % i, row_data)


with pool.connection() as connection:
    if not settings.HBASE_TABLE in connection.tables():
        populate_table()


#
# Views
#

def index(request):

    start = 'row-key-%d' % random.randint(0, N_KEYS)
    with pool.connection() as connection:
        table = connection.table(settings.HBASE_TABLE)
        scan = table.scan(row_start=start, limit=4)
        output = list(scan)

    if 'use-after-return' in request.GET:
        # It is an error to use the connection after it was returned to
        # the pool
        connection.tables()

    logger.debug('Request from thread %s', threading.current_thread().name)

    return HttpResponse(
        json.dumps(output),
        content_type='application/json')
