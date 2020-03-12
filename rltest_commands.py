# run using:
#           RLTest -t rltest_commands.py --module <directory>/rebloom.so -s

from RLTest import Env
from redisbloom.client import Client as RedisBloom
from redis import ResponseError

'''
from time import sleep
from unittest import TestCase
'''
# def CreateConn():
#     port = 6379
#     rb = RedisBloom(port=port)
#     rb.flushdb()
#     return rb

i = lambda l: [int(v) for v in l]

class TestRedisBloom():
    def __init__(self):
        self.env = Env()
        self.rb = RedisBloom(port=6379)

    def testCreate(self):
        '''Test CREATE/RESERVE calls'''
        self.env.cmd("flushall")
        rb = self.rb
        self.env.assertTrue(rb.bfCreate('bloom', 0.01, 1000))
        self.env.assertTrue(rb.cfCreate('cuckoo', 1000))
        self.env.assertTrue(rb.cmsInitByDim('cmsDim', 100, 5))
        self.env.assertTrue(rb.cmsInitByProb('cmsProb', 0.01, 0.01))
        self.env.assertTrue(rb.topkReserve('topk', 5, 100, 5, 0.9))

    ################### Test Bloom Filter ###################
    def testBFAdd(self):
        self.env.cmd("flushall")
        rb = self.rb
        self.env.assertTrue(rb.bfCreate('bloom', 0.01, 1000))
        self.env.assertEqual(1, rb.bfAdd('bloom', 'foo'))
        self.env.assertEqual(0, rb.bfAdd('bloom', 'foo'))
        self.env.assertEqual([0], i(rb.bfMAdd('bloom', 'foo')))
        self.env.assertEqual([0, 1], rb.bfMAdd('bloom', 'foo', 'bar'))
        self.env.assertEqual([0, 0, 1], rb.bfMAdd('bloom', 'foo', 'bar', 'baz'))
        self.env.assertEqual(1, rb.bfExists('bloom', 'foo'))
        self.env.assertEqual(0, rb.bfExists('bloom', 'noexist'))
        self.env.assertEqual([1, 0], i(rb.bfMExists('bloom', 'foo', 'noexist')))

    def testBFInsert(self):
        self.env.cmd("flushall")
        rb = self.rb
        self.env.assertTrue(rb.bfCreate('bloom', 0.01, 1000))
        self.env.assertEqual([1], i(rb.bfInsert('bloom', ['foo'])))
        self.env.assertEqual([0, 1], i(rb.bfInsert('bloom', ['foo', 'bar'])))
        self.env.assertEqual([1], i(rb.bfInsert('captest', ['foo'], capacity=1000)))
        self.env.assertEqual([1], i(rb.bfInsert('errtest', ['foo'], error=0.01)))
        self.env.assertEqual(1, rb.bfExists('bloom', 'foo'))
        self.env.assertEqual(0, rb.bfExists('bloom', 'noexist'))
        self.env.assertEqual([1, 0], i(rb.bfMExists('bloom', 'foo', 'noexist')))

    def testBFDumpLoad(self):
        self.env.cmd("flushall")
        rb = self.rb
        # Store a filter
        rb.bfCreate('myBloom', '0.0001', '1000')
        
        # test is probabilistic and might fail. It is OK to change variables if 
        # certain to not break anything
        def do_verify():
            res = 0
            for x in range(1000):
                rb.bfAdd('myBloom', x)
                rv = rb.bfExists('myBloom', x)
                self.env.assertTrue(rv)
                rv = rb.bfExists('myBloom', 'nonexist_{}'.format(x))
                res += (rv == x)
            self.env.assertLess(res, 5)

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
        self.env.assertEqual(prev_info, cur_info)
        do_verify()

        rb.execute_command('del', 'myBloom')
        rb.bfCreate('myBloom', '0.0001', '10000000')

    ################### Test Cuckoo Filter ###################
    def testCFAddInsert(self):
        self.env.cmd("flushall")
        rb = self.rb
        self.env.assertTrue(rb.cfCreate('cuckoo', 1000))
        self.env.assertTrue(rb.cfAdd('cuckoo', 'filter'))
        self.env.assertFalse(rb.cfAddNX('cuckoo', 'filter'))
        self.env.assertEqual(1, rb.cfAddNX('cuckoo', 'newItem'))
        self.env.assertEqual([1], rb.cfInsert('captest', ['foo']))
        self.env.assertEqual([1], rb.cfInsert('captest', ['foo'], capacity=1000))
        self.env.assertEqual([1], rb.cfInsertNX('captest', ['bar']))
        self.env.assertEqual([0, 0, 1], rb.cfInsertNX('captest', ['foo', 'bar', 'baz']))
        self.env.assertEqual([0], rb.cfInsertNX('captest', ['bar'], capacity=1000))
        self.env.assertEqual([1], rb.cfInsert('empty1', ['foo'], capacity=1000))
        self.env.assertEqual([1], rb.cfInsertNX('empty2', ['bar'], capacity=1000))

    def testCFExistsDel(self):
        self.env.cmd("flushall")
        rb = self.rb
        self.env.assertTrue(rb.cfCreate('cuckoo', 1000))
        self.env.assertTrue(rb.cfAdd('cuckoo', 'filter'))
        self.env.assertTrue(rb.cfExists('cuckoo', 'filter'))
        self.env.assertFalse(rb.cfExists('cuckoo', 'notexist'))
        self.env.assertEqual(1, rb.cfCount('cuckoo', 'filter'))        
        self.env.assertEqual(0, rb.cfCount('cuckoo', 'notexist'))        
        self.env.assertTrue(rb.cfDel('cuckoo', 'filter'))
        self.env.assertEqual(0, rb.cfCount('cuckoo', 'filter'))        

    ################### Test Count-Min Sketch ###################
    def testCMS(self):
        self.env.cmd("flushall")
        rb = self.rb
        self.env.assertTrue(rb.cmsInitByDim('dim', 1000, 5))
        self.env.assertTrue(rb.cmsInitByProb('prob', 0.01, 0.01))
        self.env.assertTrue(rb.cmsIncrBy('dim', ['foo'], [5]))
        self.env.assertEqual([0], rb.cmsQuery('dim', 'notexist'))        
        self.env.assertEqual([5], rb.cmsQuery('dim', 'foo'))        
        self.env.assertTrue(rb.cmsIncrBy('dim', ['foo', 'bar'], [5, 15]))
        self.env.assertEqual([10, 15], rb.cmsQuery('dim', 'foo', 'bar'))   
        info = rb.cmsInfo('dim')
        self.env.assertEqual(1000, info.width)     
        self.env.assertEqual(5, info.depth)     
        self.env.assertEqual(25, info.count)  

    def testCMSMerge(self):
        self.env.cmd("flushall")
        rb = self.rb
        self.env.assertTrue(rb.cmsInitByDim('A', 1000, 5))
        self.env.assertTrue(rb.cmsInitByDim('B', 1000, 5))
        self.env.assertTrue(rb.cmsInitByDim('C', 1000, 5))
        self.env.assertTrue(rb.cmsIncrBy('A', ['foo', 'bar', 'baz'], [5, 3, 9]))
        self.env.assertTrue(rb.cmsIncrBy('B', ['foo', 'bar', 'baz'], [2, 3, 1]))
        self.env.assertEqual([5, 3, 9], rb.cmsQuery('A', 'foo', 'bar', 'baz'))        
        self.env.assertEqual([2, 3, 1], rb.cmsQuery('B', 'foo', 'bar', 'baz'))        
        self.env.assertTrue(rb.cmsMerge('C', 2, ['A', 'B']))
        self.env.assertEqual([7, 6, 10], rb.cmsQuery('C', 'foo', 'bar', 'baz'))        
        self.env.assertTrue(rb.cmsMerge('C', 2, ['A', 'B'], ['1', '2']))
        self.env.assertEqual([9, 9, 11], rb.cmsQuery('C', 'foo', 'bar', 'baz'))    
        self.env.assertTrue(rb.cmsMerge('C', 2, ['A', 'B'], ['2', '3']))
        self.env.assertEqual([16, 15, 21], rb.cmsQuery('C', 'foo', 'bar', 'baz'))        
        
    ################### Test Top-K ###################
    def testTopK(self):
        self.env.cmd("flushall")
        rb = self.rb
        # test list with empty buckets
        self.env.assertTrue(rb.topkReserve('topk', 10, 50, 3, 0.9))
        self.env.assertTrue(rb.topkAdd('topk', 'A', 'B', 'C', 'D', 'E', 'A', 'A', 'B', 'C',
                                                    'G', 'D', 'B', 'D', 'A', 'E', 'E'))
        self.env.assertEqual([1, 1, 1, 1, 1, 0, 1],
                             rb.topkQuery('topk', 'A', 'B', 'C', 'D', 'E', 'F', 'G'))
        self.env.assertEqual([4, 3, 2, 3, 3, 0, 1],
                             rb.topkCount('topk', 'A', 'B', 'C', 'D', 'E', 'F', 'G'))                             

        # test full list
        self.env.assertTrue(rb.topkReserve('topklist', 3, 50, 3, 0.9))
        self.env.assertTrue(rb.topkAdd('topklist', 'A', 'B', 'C', 'D', 'E','A', 'A', 'B', 'C', 
                                                        'G', 'D', 'B', 'D', 'A', 'E', 'E'))        
        self.env.assertEqual(['D', 'A', 'B'], rb.topkList('topklist'))