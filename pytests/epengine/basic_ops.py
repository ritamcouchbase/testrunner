import json

from basetestcase import BaseTestCase
from mc_bin_client import MemcachedError
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import VBucketAwareMemcached

"""

Capture basic get, set operations, also the meta operations. This is based on some 4.1.1 test which had separate
bugs with incr and delete with meta and I didn't see an obvious home for them.

This is small now but we will reactively add things

These may be parameterized by:
   - full and value eviction
   - DGM and non-DGM



"""



class basic_ops(BaseTestCase):

    def setUp(self):
        super(basic_ops, self).setUp()

    def tearDown(self):
        super(basic_ops, self).tearDown()

    def do_basic_ops(self):

        KEY_NAME = 'key1'
        CAS = 1234
        self.log.info('Starting basic ops')


        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, 'default')
        mcd = client.memcached(KEY_NAME)

        # MB-17231 - incr with full eviction
        rc = mcd.incr(KEY_NAME, 1)
        print 'rc for incr', rc



        # MB-17289 del with meta


        rc = mcd.set(KEY_NAME, 0,0, json.dumps({'value':'value2'}))
        print 'set is', rc


        # wait for it to persist
        persisted = 0
        while persisted == 0:
                opaque, rep_time, persist_time, persisted, cas = client.observe(KEY_NAME)


        try:
            rc = mcd.evict_key(KEY_NAME)

        except MemcachedError as exp:
            self.fail("Exception with evict meta - {0}".format(exp) )


        try:
            #key, exp, flags, seqno, old_cas, new_cas,
            rc = mcd.del_with_meta(KEY_NAME, 0, 0, 0, CAS, CAS+1)
            print 'del with meta is', rc


        except MemcachedError as exp:
            self.fail("Exception with del_with meta - {0}".format(exp) )

