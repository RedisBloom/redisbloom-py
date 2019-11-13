import requests
import redis
import time
from redis import Redis
from redisbloom.client import Client as RedisBloom

def create_topk(ctx, k, width, depth):
	ctx.topkReserve('bm_topk', k, k * width, depth, 0.5)

def detect(list_a, list_b):
	#print set(list_a).difference(list_b)
	return len(set(list_a).intersection(list_b)) / float(len(list_a))

redis = redis.Redis(host='localhost', port=6379, db=0)
redis_pipe = redis.pipeline()
redis_bloom = RedisBloom(host='localhost', port=6379, db=0)
redis_bloom.flushall()

start_time = time.time()

print "Downloading data"
url = 'https://www.gutenberg.org/files/2554/2554-0.txt'

page = requests.get(url)
#page = open('./bigdata.txt')flu    

print("--- %s seconds ---" % (time.time() - start_time))
start_time = time.time()

print "\nUsing sorted set with pipeline"
#for line in page:
for line in page.iter_lines():
    for word in line.split():
        redis_pipe.zincrby('bm_text', 1, word)

responses = redis_pipe.execute()
for response in responses:
	pass

real_results = redis.zrevrange('bm_text', 0, 49)
#print real_results
print('Memory used %s'% redis.memory_usage('bm_text'))
print('This is an accurate list for comparison')
#redis.flushall()
print("--- %s seconds ---" % (time.time() - start_time))
start_time = time.time()
print(redis.zcount('bm_text', '-inf', '+inf'))
'''
# test Top-K inserting multiple words at the time
print "\nMultiple words"
create_topk(redis_bloom)
for line in page.iter_lines():
	if line is not '' and line is not ' ':
		a = line.split()
		redis_bloom.topkAdd('bm_text', *a)

leaderboard = redis_bloom.topkList('bm_text')
print('Memory used %s'% redis_bloom.memory_usage('bm_text'))
print('Accuracy is %s percent' % (detect(real_results, leaderboard) * 100))
print("--- %s seconds ---" % (time.time() - start_time))
'''

'''
# test Top-K single word at the time
print "\nSingle word"
redis_bloom.flushall()
create_topk(redis_bloom)
start_time = time.time()

for line in page.iter_lines():
	for word in line.split():
		redis_bloom.topkAdd('bm_text', word)

leaderboard = redis_bloom.topkList('bm_text')
print('Memory used %s'% redis_bloom.memory_usage('bm_text'))
#print redis_bloom.memory_usage('bm_text')
print('Accuracy is %s percent' % (detect(real_results, leaderboard) * 100))
print("--- %s seconds ---" % (time.time() - start_time))
'''

# test Top-K pipeline
print "\nPipelining"
print("K Width(*k) Depth Memory Accuracy Time")
k_list = [2000]
pipe = redis_bloom.pipeline()
for k in k_list:
	real_results = redis.zrevrange('bm_text', 0, k - 1)
	for width in [10]:
		for depth in [10]:
			#page.seek(0)
			redis_bloom.execute_command('DEL', 'bm_topk')
			create_topk(redis_bloom, k, width, depth)
			start_time = time.time()

			for line in page.iter_lines():
			#for line in page:
				if line is not '' and line is not ' ':
					a = line.split()
					pipe.execute_command('topk.add', 'bm_topk', *a)

			responses = pipe.execute()
			for response in responses:
				pass

			leaderboard = redis_bloom.topkList('bm_topk')
			'''
			print('Memory used %s'% redis.memory_usage('bm_text'))
			print('Accuracy is %s percent' % (detect(real_results, leaderboard) * 100))
			print("--- %s seconds ---" % (time.time() - start_time))
			'''
			print(str(k) + " " + str(width) + " " + str(depth) + " " +
					str(redis.memory_usage('bm_topk')) + " " +
					str(detect(real_results, leaderboard) * 100) + " " +
					str(time.time() - start_time))