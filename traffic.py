
#
# Random traffic to test Mrcache
#

import asyncio, time, random
from collections import deque

#import tracemalloc
#tracemalloc.start()



import asyncmrcache
import zstandard as zstd
#import objgraph
async def lost_conn():
  pass

async def setup(rc):
  rc.hot = deque(maxlen=1000)
  for x in range(1000):
    rc.hot.append( ("test"+str(x)).encode("utf-8") )
  for k in rc.hot:
    rc.set( k, b"somekey" )
  

def getRandomHotKey(rc):
  return random.choice( rc.hot )

async def run(loop):

  rc = await asyncmrcache.create_client("localhost", loop, pool_size=1,lost_cb=lost_conn)
  await setup(rc)

  #snapshot1 = tracemalloc.take_snapshot()
 
  n = 1000
  while 1:

    futs = []

    # Do some hot gets
    for y in range(10):
      k = getRandomHotKey(rc)
      futs.append( rc.get(k) )
    
    # Do some hot sets
    for y in range(2):
      k = getRandomHotKey(rc)
      rc.set( k, b"this is a hot key" ) 
    # Add new keys
    for y in range(3):
      k = ("test"+str(n)).encode("utf-8") 
      rc.set( k, b"some new key" ) 
      n += 1
      if random.randrange(200) < 2:
        rc.hot.append(k)
    
    rets = await asyncio.gather(*futs)
    if n % 1000 == 0: 
      print(" ",n,len(rets))
      #if n > 2000000:
        #break
  
  #snapshot2 = tracemalloc.take_snapshot()
  #top_stats = snapshot2.compare_to(snapshot1, 'lineno')
  #print("[ Top 10 differences ]")
  #for stat in top_stats[:10]:
    #print(stat)

  await rc.close()
  return


if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()


