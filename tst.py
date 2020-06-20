
import asyncio, time
import asyncmrcache
import zstandard as zstd

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

def lcb(client):
  pass
  #test( client.debug_data )
    

async def run(loop):

  rc = await asyncmrcache.create_client("localhost", loop, pool_size=1,lost_cb=lcb)

  num_items = 2000
  item_sz = 10000

  #print( await rc.get(b"test541") )
  #print( await rc.get(b"test615") )
  

  if 1:
    for x in range(num_items):
      k = b"test"  + str(x).encode()
      v = b"test"  + str(x).encode()
      for y in range(item_sz-len(v)):
        v += b'a'
      await rc.set(k, v)
      #print(k)
      #if (x%10000)==0: print(k)
    await asyncio.sleep(2)
    rc.stat()

  if 1:
    missed = 0
    for x in range(num_items):
      k   = b"test" + str(x).encode()
      exp = b"test" + str(x).encode()
      for y in range(item_sz-len(exp)):
        exp += b'a'
      v = await rc.get(k)
      if v == None: missed += 1
      if v != exp:
        if v != None: print(exp[:10], " != ", v[:10]) 
    print( "Missed ", missed )
    print( "hit ", num_items-missed )
    await asyncio.sleep(2)
    rc.stat()
  
  exit()
  print( await rc.get(b"test22") )
  print( await rc.get(b"test25") )
  print( await rc.get(b"test26") )
  print( await rc.get(b"test27") )

  rc.set(b"test", b"good") 
  rc.set(b"test22", b"good") 
  rc.set(b"test25", b"good") 
  rc.set(b"test1", b"good") 
  rc.set(b"test212", b"good") 
  rc.set(b"test500", b"good") 
  exit()
  print("A")
  for x in range(1):
    futs = []
    print("1")
    futs.append( rc.get(b"test") )
    futs.append( rc.get(b"test212") )
    futs.append( rc.get(b"test1") )
    futs.append( rc.get(b"test") )
    
    futs.append( rc.get(b"test") )
    futs.append( rc.get(b"test500") )
    futs.append( rc.get(b"test") )
    futs.append( rc.get(b"test") )
    ret = await asyncio.gather(*futs)
    for v in ret:
      if v != b"good":
        print("NO",v)
        exit()
    #rc.set(b"ffdsad",b"dfadsfwee")
    #rc.set(b"ffdsad",b"dfadsfwee")
    #rc.set(b"ffdsad",b"dfadsfwee")
  
  print("DELME") 
  print(rc.q)
  await rc.close()
  print(rc.q)
  return


if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
  print("DONE")


