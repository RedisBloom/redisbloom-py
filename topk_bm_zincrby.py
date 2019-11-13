import requests
import redis
import time
start_time = time.time()

r = redis.Redis(host='localhost', port=6379, db=0)
r.flushall()

url = 'https://raw.githubusercontent.com/mxw/grmr/master/src/finaltests/bible.txt'
page = requests.get(url)
print("--- %s seconds ---" % (time.time() - start_time))
start_time = time.time()
print "w/o pipeline"

for line in page.iter_lines():
    for word in line.split():
        r.zincrby('bible', 1, word)

leaderboard = r.zrevrange('bible', 0, 50)
print leaderboard
print r.memory_usage('bible')
print("--- %s seconds ---" % (time.time() - start_time))
start_time = time.time()

pipe = r.pipeline()
pipe.flushall()
print "with pipeline"

for line in page.iter_lines():
    for word in line.split():
        pipe.zincrby('bible', 1, word)

responses = pipe.execute()
for response in responses:
	pass

leaderboard = r.zrevrange('bible', 0, 50)
print leaderboard
print r.memory_usage('bible')
print("--- %s seconds ---" % (time.time() - start_time))
