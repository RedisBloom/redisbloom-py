import unittest

from time import sleep
from unittest import TestCase
from redisbloom.client import Client as RedisBloom
from redis import ResponseError

xrange = range
rb = None
port = 6379

i = lambda l: [int(v) for v in l]

# Can be used with assertRaises
def run_func(func, *args, **kwargs): 
    func(*args, **kwargs)

class TestRedisBloom(TestCase):
    def setUp(self):
        global rb
        rb = RedisBloom(port=port)
        rb.flushdb()

    def testCreate(self):
        '''Test CREATE/RESERVE calls'''
        self.assertTrue(rb.bfCreate('bloom', 0.01, 1000))
        self.assertTrue(rb.bfCreate('bloom_e', 0.01, 1000, expansion=1))
        self.assertTrue(rb.bfCreate('bloom_ns', 0.01, 1000, noScale=True))
        self.assertTrue(rb.cfCreate('cuckoo', 1000))
        self.assertTrue(rb.cfCreate('cuckoo_e', 1000, expansion=1))
        self.assertTrue(rb.cfCreate('cuckoo_bs', 1000, bucket_size=4))
        self.assertTrue(rb.cfCreate('cuckoo_mi', 1000, max_iterations=10))
        self.assertTrue(rb.cmsInitByDim('cmsDim', 100, 5))
        self.assertTrue(rb.cmsInitByProb('cmsProb', 0.01, 0.01))
        self.assertTrue(rb.topkReserve('topk', 5, 100, 5, 0.9))

    ################### Test Bloom Filter ###################
    def testBFAdd(self):
        self.assertTrue(rb.bfCreate('bloom', 0.01, 1000))
        self.assertEqual(1, rb.bfAdd('bloom', 'foo'))
        self.assertEqual(0, rb.bfAdd('bloom', 'foo'))
        self.assertEqual([0], i(rb.bfMAdd('bloom', 'foo')))
        self.assertEqual([0, 1], rb.bfMAdd('bloom', 'foo', 'bar'))
        self.assertEqual([0, 0, 1], rb.bfMAdd('bloom', 'foo', 'bar', 'baz'))
        self.assertEqual(1, rb.bfExists('bloom', 'foo'))
        self.assertEqual(0, rb.bfExists('bloom', 'noexist'))
        self.assertEqual([1, 0], i(rb.bfMExists('bloom', 'foo', 'noexist')))

    def testBFInsert(self):
        self.assertTrue(rb.bfCreate('bloom', 0.01, 1000))
        self.assertEqual([1], i(rb.bfInsert('bloom', ['foo'])))
        self.assertEqual([0, 1], i(rb.bfInsert('bloom', ['foo', 'bar'])))
        self.assertEqual([1], i(rb.bfInsert('captest', ['foo'], capacity=1000)))
        self.assertEqual([1], i(rb.bfInsert('errtest', ['foo'], error=0.01)))
        self.assertEqual(1, rb.bfExists('bloom', 'foo'))
        self.assertEqual(0, rb.bfExists('bloom', 'noexist'))
        self.assertEqual([1, 0], i(rb.bfMExists('bloom', 'foo', 'noexist')))

    def testBFDumpLoad(self):
        # Store a filter
        rb.bfCreate('myBloom', '0.0001', '1000')
        
        # test is probabilistic and might fail. It is OK to change variables if 
        # certain to not break anything
        def do_verify():
            res = 0
            for x in xrange(1000):
                rb.bfAdd('myBloom', x)
                rv = rb.bfExists('myBloom', x)
                self.assertTrue(rv)
                rv = rb.bfExists('myBloom', 'nonexist_{}'.format(x))
                res += (rv == x)
            self.assertLess(res, 5)

        do_verify()
        cmds = []
        cur = rb.bfScandump('myBloom', 0)
        first = cur[0]
        cmds.append(cur)

        while True:
            cur = rb.bfScandump('myBloom', first)
            first = cur[0]
            if first == 0:
                break
            else:
                cmds.append(cur)
        prev_info = rb.execute_command('bf.debug', 'myBloom')

        # Remove the filter
        rb.execute_command('del', 'myBloom')

        # Now, load all the commands:
        for cmd in cmds:
            rb.bfLoadChunk('myBloom', *cmd)

        cur_info = rb.execute_command('bf.debug', 'myBloom')
        self.assertEqual(prev_info, cur_info)
        do_verify()

        rb.execute_command('del', 'myBloom')
        rb.bfCreate('myBloom', '0.0001', '10000000')

    ################### Test Cuckoo Filter ###################
    def testCFAddInsert(self):
        self.assertTrue(rb.cfCreate('cuckoo', 1000))
        self.assertTrue(rb.cfAdd('cuckoo', 'filter'))
        self.assertFalse(rb.cfAddNX('cuckoo', 'filter'))
        self.assertEqual(1, rb.cfAddNX('cuckoo', 'newItem'))
        self.assertEqual([1], rb.cfInsert('captest', ['foo']))
        self.assertEqual([1], rb.cfInsert('captest', ['foo'], capacity=1000))
        self.assertEqual([1], rb.cfInsertNX('captest', ['bar']))
        self.assertEqual([1], rb.cfInsertNX('captest', ['food'], nocreate='1'))
        self.assertEqual([0, 0, 1], rb.cfInsertNX('captest', ['foo', 'bar', 'baz']))
        self.assertEqual([0], rb.cfInsertNX('captest', ['bar'], capacity=1000))
        self.assertEqual([1], rb.cfInsert('empty1', ['foo'], capacity=1000))
        self.assertEqual([1], rb.cfInsertNX('empty2', ['bar'], capacity=1000))
        self.assertRaises(ResponseError, run_func(rb.cfInsert, 'noexist', ['foo']))

    def testCFExistsDel(self):
        self.assertTrue(rb.cfCreate('cuckoo', 1000))
        self.assertTrue(rb.cfAdd('cuckoo', 'filter'))
        self.assertTrue(rb.cfExists('cuckoo', 'filter'))
        self.assertFalse(rb.cfExists('cuckoo', 'notexist'))
        self.assertEqual(1, rb.cfCount('cuckoo', 'filter'))        
        self.assertEqual(0, rb.cfCount('cuckoo', 'notexist'))        
        self.assertTrue(rb.cfDel('cuckoo', 'filter'))
        self.assertEqual(0, rb.cfCount('cuckoo', 'filter'))        

    ################### Test Count-Min Sketch ###################
    def testCMS(self):
        self.assertTrue(rb.cmsInitByDim('dim', 1000, 5))
        self.assertTrue(rb.cmsInitByProb('prob', 0.01, 0.01))
        self.assertTrue(rb.cmsIncrBy('dim', ['foo'], [5]))
        self.assertEqual([0], rb.cmsQuery('dim', 'notexist'))        
        self.assertEqual([5], rb.cmsQuery('dim', 'foo'))        
        self.assertEqual([10, 15], rb.cmsIncrBy('dim', ['foo', 'bar'], [5, 15]))
        self.assertEqual([10, 15], rb.cmsQuery('dim', 'foo', 'bar'))   
        info = rb.cmsInfo('dim')
        self.assertEqual(1000, info.width)     
        self.assertEqual(5, info.depth)     
        self.assertEqual(25, info.count)  

    def testCMSMerge(self):
        self.assertTrue(rb.cmsInitByDim('A', 1000, 5))
        self.assertTrue(rb.cmsInitByDim('B', 1000, 5))
        self.assertTrue(rb.cmsInitByDim('C', 1000, 5))
        self.assertTrue(rb.cmsIncrBy('A', ['foo', 'bar', 'baz'], [5, 3, 9]))
        self.assertTrue(rb.cmsIncrBy('B', ['foo', 'bar', 'baz'], [2, 3, 1]))
        self.assertEqual([5, 3, 9], rb.cmsQuery('A', 'foo', 'bar', 'baz'))        
        self.assertEqual([2, 3, 1], rb.cmsQuery('B', 'foo', 'bar', 'baz'))        
        self.assertTrue(rb.cmsMerge('C', 2, ['A', 'B']))
        self.assertEqual([7, 6, 10], rb.cmsQuery('C', 'foo', 'bar', 'baz'))        
        self.assertTrue(rb.cmsMerge('C', 2, ['A', 'B'], ['1', '2']))
        self.assertEqual([9, 9, 11], rb.cmsQuery('C', 'foo', 'bar', 'baz'))    
        self.assertTrue(rb.cmsMerge('C', 2, ['A', 'B'], ['2', '3']))
        self.assertEqual([16, 15, 21], rb.cmsQuery('C', 'foo', 'bar', 'baz'))        
        
    ################### Test Top-K ###################
    def testTopK(self):
        # test list with empty buckets
        self.assertTrue(rb.topkReserve('topk', 3, 50, 4, 0.9))
        self.assertEqual([None, None, None, None, None, None, None, None,
                          None, None, None, None, 'C', None, None, None, None], 
                          rb.topkAdd('topk', 'A', 'B', 'C', 'D', 'E', 'A', 'A', 'B', 'C',
                                                    'G', 'D', 'B', 'D', 'A', 'E', 'E', 1))
        self.assertEqual([1, 1, 0, 1, 0, 0, 0],
                             rb.topkQuery('topk', 'A', 'B', 'C', 'D', 'E', 'F', 'G'))
        self.assertEqual([4, 3, 2, 3, 3, 0, 1],
                             rb.topkCount('topk', 'A', 'B', 'C', 'D', 'E', 'F', 'G'))                             

        # test full list
        self.assertTrue(rb.topkReserve('topklist', 3, 50, 3, 0.9))
        self.assertTrue(rb.topkAdd('topklist', 'A', 'B', 'C', 'D', 'E','A', 'A', 'B', 'C', 
                                                        'G', 'D', 'B', 'D', 'A', 'E', 'E'))        
        self.assertEqual(['D', 'A', 'B'], rb.topkList('topklist'))
        info = rb.topkInfo('topklist')
        self.assertEqual(3, info.k)     
        self.assertEqual(50, info.width)     
        self.assertEqual(3, info.depth)  
        self.assertAlmostEqual(0.9, float(info.decay))  

if __name__ == '__main__':
    unittest.main()