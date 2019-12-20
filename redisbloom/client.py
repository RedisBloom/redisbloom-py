import six
import redis
from redis import Redis, RedisError 
from redis.client import Pipeline
from redis.client import bool_ok
from redis._compat import (long, nativestr)
from redis.exceptions import DataError

class CMSInfo(object):
    width = None
    depth = None
    count = None

    def __init__(self, args):
        response = dict(zip(map(nativestr, args[::2]), args[1::2]))
        self.width = response['width']
        self.depth = response['depth']
        self.count = response['count']

class TopKInfo(object):
    k = None
    width = None
    depth = None
    decay = None

    def __init__(self, args):
        response = dict(zip(map(nativestr, args[::2]), args[1::2]))
        self.k = response['k']
        self.width = response['width']
        self.depth = response['depth']
        self.decay = response['decay']

def spaceHolder(response):
    return response

def parseToList(response):
    res = []
    for item in response:
        if item is not None:
            res.append(nativestr(item))
        else:
            res.append(None)
    return res

class Client(Redis): #changed from StrictRedis
    """
    This class subclasses redis-py's `Redis` and implements 
    RedisBloom's commands.
    The client allows to interact with RedisBloom and use all of
    it's functionality.
    Prefix is according to the DS used.
    - BF for Bloom Filter
    - CF for Cuckoo Filter
    - CMS for Count-Min Sketch
    - TopK for TopK Data Structure
    """

    MODULE_INFO = {
        'name': 'RedisBloom',
        'ver':  '0.1.0'
    }

    BF_RESERVE = 'BF.RESERVE'
    BF_ADD = 'BF.ADD'
    BF_MADD = 'BF.MADD'
    BF_INSERT = 'BF.INSERT'
    BF_EXISTS = 'BF.EXISTS'
    BF_MEXISTS = 'BF.MEXISTS'
    BF_SCANDUMP = 'BF.SCANDUMP'
    BF_LOADCHUNK = 'BF.LOADCHUNK'

    CF_RESERVE = 'CF.RESERVE'
    CF_ADD = 'CF.ADD'
    CF_ADDNX = 'CF.ADDNX'
    CF_INSERT = 'CF.INSERT'
    CF_INSERTNX = 'CF.INSERTNX'
    CF_EXISTS = 'CF.EXISTS'
    CF_DEL = 'CF.DEL'
    CF_COUNT = 'CF.COUNT'
    CF_SCANDUMP = 'CF.SCANDUMP'
    CF_LOADDUMP = 'CF.LOADDUMP'
    
    CMS_INITBYDIM = 'CMS.INITBYDIM'
    CMS_INITBYPROB = 'CMS.INITBYPROB'
    CMS_INCRBY = 'CMS.INCRBY'
    CMS_QUERY = 'CMS.QUERY'
    CMS_MERGE = 'CMS.MERGE'
    CMS_INFO = 'CMS.INFO'

    TOPK_RESERVE = 'TOPK.RESERVE'
    TOPK_ADD = 'TOPK.ADD'
    TOPK_QUERY = 'TOPK.QUERY'
    TOPK_COUNT = 'TOPK.COUNT'
    TOPK_LIST = 'TOPK.LIST'
    TOPK_INFO = 'TOPK.INFO'

    def __init__(self, *args, **kwargs):
        """
        Creates a new RedisBloom client.
        """
        Redis.__init__(self, *args, **kwargs)
            
        # Set the module commands' callbacks
        MODULE_CALLBACKS = {
            self.BF_RESERVE : bool_ok,
            #self.BF_ADD : spaceHolder,
            #self.BF_MADD : spaceHolder,
            #self.BF_INSERT : spaceHolder,
            #self.BF_EXISTS : spaceHolder,
            #self.BF_MEXISTS : spaceHolder,
            #self.BF_SCANDUMP : spaceHolder,
            #self.BF_LOADCHUNK : spaceHolder,

            self.CF_RESERVE : bool_ok,
            #self.CF_ADD : spaceHolder,
            #self.CF_ADDNX : spaceHolder,
            #self.CF_INSERT : spaceHolder,
            #self.CF_INSERTNX : spaceHolder,
            #self.CF_EXISTS : spaceHolder,
            #self.CF_DEL : spaceHolder,
            #self.CF_COUNT : spaceHolder,
            #self.CF_SCANDUMP : spaceHolder,
            #self.CF_LOADDUMP : spaceHolder,

            
            self.CMS_INITBYDIM : bool_ok,
            self.CMS_INITBYPROB : bool_ok,
            #self.CMS_INCRBY : spaceHolder,
            #self.CMS_QUERY : spaceHolder,
            self.CMS_MERGE : bool_ok,
            self.CMS_INFO : CMSInfo,

            self.TOPK_RESERVE : bool_ok,
            self.TOPK_ADD : parseToList,
            #self.TOPK_QUERY : spaceHolder,
            #self.TOPK_COUNT : spaceHolder,
            self.TOPK_LIST : parseToList,
            self.TOPK_INFO : TopKInfo,
        }
        for k, v in six.iteritems(MODULE_CALLBACKS):
            self.set_response_callback(k, v)

    @staticmethod
    def appendItems(params, items):
        params.extend(['ITEMS'])
        params += items

    @staticmethod
    def appendError(params, error):
        if error is not None:
            params.extend(['ERROR', error])

    @staticmethod
    def appendCapacity(params, capacity):
        if capacity is not None:
            params.extend(['CAPACITY', capacity])

    @staticmethod
    def appendExpansion(params, expansion):
        if expansion is not None:
            params.extend(['EXPANSION', expansion])

    @staticmethod
    def appendNoScale(params, noScale):
        if noScale is not None:
            params.extend(['NONSCALING'])

    @staticmethod
    def appendWeights(params, weights):
        if len(weights) > 0:
            params.append('WEIGHTS')
            params += weights

    @staticmethod
    def appendNoCreate(params, noCreate):
        if noCreate is not None:
            params.extend(['NOCREATE'])

    @staticmethod
    def appendItemsAndIncrements(params, items, increments):
        for i in range(len(items)):
            params.append(items[i])
            params.append(increments[i])

    @staticmethod
    def appendMaxIterations(params, max_iterations):
        if max_iterations is not None:
            params.extend(['MAXITERATIONS', max_iterations])

    @staticmethod
    def appendBucketSize(params, bucket_size):
        if bucket_size is not None:
            params.extend(['BUCKETSIZE', bucket_size])

################## Bloom Filter Functions ######################
    def bfCreate(self, key, errorRate, capacity, expansion=None, noScale=None):
        """
        Creates a new Bloom Filter ``key`` with desired probability of false 
        positives ``errorRate`` expected entries to be inserted as ``capacity``.
        Default expansion value is 2.
        By default, filter is auto-scaling.
        """
        params = [key, errorRate, capacity]
        self.appendExpansion(params, expansion)
        self.appendNoScale(params, noScale)

        return self.execute_command(self.BF_RESERVE, *params)
        
    def bfAdd(self, key, item):
        """
        Adds to a Bloom Filter ``key`` an ``item``.
        """
        params = [key, item]
        
        return self.execute_command(self.BF_ADD, *params)

    def bfMAdd(self, key, *items):
        """
        Adds to a Bloom Filter ``key`` multiple ``items``.
        """
        params = [key]
        params += items

        return self.execute_command(self.BF_MADD, *params)

    def bfInsert(self, key, items, capacity=None, error=None, noCreate=None, expansion=None, noScale=None):
        """
        Adds to a Bloom Filter ``key`` multiple ``items``. If ``nocreate``
        remain ``None`` and ``key does not exist, a new Bloom Filter ``key`` 
        will be created with desired probability of false positives ``errorRate``
        and expected entries to be inserted as ``size``.
        """
        params = [key]
        self.appendCapacity(params, capacity)
        self.appendError(params, error)
        self.appendNoCreate(params, noCreate)
        self.appendItems(params, items)
        self.appendExpansion(params, expansion)
        self.appendNoScale(params, noScale)

        return self.execute_command(self.BF_INSERT, *params)

    def bfExists(self, key, item):
        """
        Checks whether an ``item`` exists in Bloom Filter ``key``.
        """
        params = [key, item]
        
        return self.execute_command(self.BF_EXISTS, *params)

    def bfMExists(self, key, *items):
        """
        Checks whether ``items`` exist in Bloom Filter ``key``.
        """
        params = [key]
        params += items

        return self.execute_command(self.BF_MEXISTS, *params)

    def bfScandump(self, key, iter):
        """
        Begins an incremental save of the bloom filter ``key``. This is useful
        for large bloom filters which cannot fit into the normal SAVE
        and RESTORE model.
        The first time this command is called, the value of ``iter`` should be 0.
        This command will return successive (iter, data) pairs until
        (0, NULL) to indicate completion.
        """
        params = [key, iter]
        
        return self.execute_command(self.BF_SCANDUMP, *params)

    def bfLoadChunk(self, key, iter, data):
        """
        Restores a filter previously saved using SCANDUMP. See the SCANDUMP
        command for example usage.
        This command will overwrite any bloom filter stored under key.
        Ensure that the bloom filter will not be modified between invocations.
        """
        params = [key, iter, data]
        
        return self.execute_command(self.BF_LOADCHUNK, *params)


################## Cuckoo Filter Functions ######################

    def cfCreate(self, key, capacity, expansion=None, bucket_size=None, max_iterations=None):
        """
        Creates a new Cuckoo Filter ``key`` an initial ``capacity`` items.
        """
        params = [key, capacity]
        self.appendExpansion(params, expansion)
        self.appendBucketSize(params, bucket_size)
        self.appendMaxIterations(params, max_iterations)

        return self.execute_command(self.CF_RESERVE, *params)
        
    def cfAdd(self, key, item):
        """
        Adds an ``item`` to a Cuckoo Filter ``key``.
        """
        params = [key, item]
        
        return self.execute_command(self.CF_ADD, *params)

    def cfAddNX(self, key, item):
        """
        Adds an ``item`` to a Cuckoo Filter ``key`` only if item does not yet exist.
        Command might be slower that ``cfAdd``.
        """
        params = [key, item]
        
        return self.execute_command(self.CF_ADDNX, *params)

    def cfInsert(self, key, items, capacity=None, nocreate=None):
        """
        Adds multiple ``items`` to a Cuckoo Filter ``key``, allowing the filter to be
        created with a custom ``capacity` if it does not yet exist.
        ``items`` must be provided as a list.
        """
        params = [key]
        self.appendCapacity(params, capacity)
        self.appendNoCreate(params, nocreate)
        self.appendItems(params, items)

        return self.execute_command(self.CF_INSERT, *params)

    def cfInsertNX(self, key, items, capacity=None, nocreate=None):
        """
        Adds multiple ``items`` to a Cuckoo Filter ``key`` only if they do not exist yet,
        allowing the filter to be created with a custom ``capacity` if it does not yet exist.
        ``items`` must be provided as a list.
        """
        params = [key]
        self.appendCapacity(params, capacity)
        self.appendNoCreate(params, nocreate)
        self.appendItems(params, items)

        return self.execute_command(self.CF_INSERTNX, *params)

    def cfExists(self, key, item):
        """
        Checks whether an ``item`` exists in Cuckoo Filter ``key``.
        """
        params = [key, item]
        
        return self.execute_command(self.CF_EXISTS, *params)

    def cfDel(self, key, item):
        """
        Deletes ``item`` from ``key``.
        """
        params = [key, item]

        return self.execute_command(self.CF_DEL, *params)

    def cfCount(self, key, item):
        """
        Returns the number of times an ``item`` may be in the ``key``.
        """
        params = [key, item]

        return self.execute_command(self.CF_COUNT, *params)

    def cfScandump(self, key, iter):
        """
        Begins an incremental save of the Cuckoo filter ``key``. This is useful
        for large Cuckoo filters which cannot fit into the normal SAVE
        and RESTORE model.
        The first time this command is called, the value of ``iter`` should be 0.
        This command will return successive (iter, data) pairs until
        (0, NULL) to indicate completion.
        """
        params = [key, iter]
        
        return self.execute_command(self.CF_SCANDUMP, *params)

    def cfLoadChunk(self, key, iter, data):
        """
        Restores a filter previously saved using SCANDUMP. See the SCANDUMP
        command for example usage.
        This command will overwrite any Cuckoo filter stored under key.
        Ensure that the Cuckoo filter will not be modified between invocations.
        """
        params = [key, iter, data]
        
        return self.execute_command(self.CF_LOADDUMP, *params)


################## Count-Min Sketch Functions ######################

    def cmsInitByDim(self, key, width, depth):
        """
        Initializes a Count-Min Sketch ``key`` to dimensions
        (``width``, ``depth``) specified by user.
        """
        params = [key, width, depth]
        
        return self.execute_command(self.CMS_INITBYDIM, *params)

    def cmsInitByProb(self, key, error, probability):
        """
        Initializes a Count-Min Sketch ``key`` to characteristics
        (``error``, ``probability``) specified by user.
        """
        params = [key, error, probability]
        
        return self.execute_command(self.CMS_INITBYPROB, *params)

    def cmsIncrBy(self, key, items, increments):
        """
        Adds/increases ``items`` to a Count-Min Sketch ``key`` by ''increments''.
        Both ``items`` and ``increments`` are lists.
        Example - cmsIncrBy('A', ['foo'], [1])
        """
        params = [key]
        self.appendItemsAndIncrements(params, items, increments)
        
        return self.execute_command(self.CMS_INCRBY, *params)

    def cmsQuery(self, key, *items):
        """
        Returns count for an ``item`` from ``key``.
        Multiple items can be queried with one call.
        """
        params = [key]
        params += items
        
        return self.execute_command(self.CMS_QUERY, *params)

    def cmsMerge(self, destKey, numKeys, srcKeys, weights=[]):
        """
        Merges ``numKeys`` of sketches into ``destKey``. Sketches specified in ``srcKeys``. 
        All sketches must have identical width and depth.
        ``Weights`` can be used to multiply certain sketches. Default weight is 1.
        Both ``srcKeys`` and ``weights`` are lists.
        """
        params = [destKey, numKeys]
        params += srcKeys
        self.appendWeights(params, weights)

        return self.execute_command(self.CMS_MERGE, *params)

    def cmsInfo(self, key):
        """
        Returns width, depth and total count of the sketch.
        """

        return self.execute_command(self.CMS_INFO, key)


################## Top-K Functions ######################

    def topkReserve(self, key, k, width, depth, decay):
        """
        Creates a new Cuckoo Filter ``key`` with desired probability of false 
        positives ``errorRate`` expected entries to be inserted as ``size``.
        """
        params = [key, k, width, depth, decay]
        
        return self.execute_command(self.TOPK_RESERVE, *params)


    def topkAdd(self, key, *items):
        """
        Adds one ``item`` or more to a Cuckoo Filter ``key``.
        """
        params = [key]
        params += items
        
        return self.execute_command(self.TOPK_ADD, *params)

    def topkQuery(self, key, *items):
        """
        Checks whether one ``item`` or more is a Top-K item at ``key``.
        """
        params = [key]
        params += items
        
        return self.execute_command(self.TOPK_QUERY, *params)

    def topkCount(self, key, *items):
        """
        Returns count for one ``item`` or more from ``key``. 
        """
        params = [key]
        params += items

        return self.execute_command(self.TOPK_COUNT, *params)

    def topkList(self, key):
        """
        Return full list of items in Top-K list of ``key```.
        """
        
        return self.execute_command(self.TOPK_LIST, key)

    def topkInfo(self, key):
        """
        Returns k, width, depth and decay values of ``key``.
        """
        
        return self.execute_command(self.TOPK_INFO, key)

    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        Overridden in order to provide the right client through the pipeline.
        """
        p = Pipeline(
            connection_pool=self.connection_pool,
            response_callbacks=self.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint)
        return p

class Pipeline(Pipeline, Client):
    "Pipeline for RedisBloom Client"