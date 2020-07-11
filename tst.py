
import asyncio, time
import asyncmrcache

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

def lcb(client):
  pass
  #test( client.debug_data )
    

async def run(loop):

  #print("1")
  #await asyncio.sleep(2)
  #print("1")
  #await asyncio.sleep(2)
  #print("1")

  #rc = await asyncmrcache.create_client( [("localhost",7000),("localhost",7001)], loop, lost_cb=lcb)
  #rc = await asyncmrcache.create_client( [("localhost",7000)], loop, pool_size=2,lost_cb=lcb)
  rc = await asyncmrcache.create_client( [("localhost",7000)], loop, lost_cb=lcb)

  if 0:
    await rc.set(b"test1",b"tets1")
    print(await rc.get(b"test1"))
    await rc.set(b"test2",b"tets2")
    print(await rc.get(b"test2"))
    print(await rc.get(b"test1"))
    print(await rc.get(b"test1"))
    print(await rc.get(b"test2"))
    print(await rc.get(b"test2"))

    for x in range(2):
      futs = []
      futs.append( rc.get(b"test1") )
      futs.append( rc.get(b"test2") )
      futs.append( rc.get(b"test1") )
      futs.append( rc.get(b"test2") )
      futs.append( rc.get(b"test1") )
      futs.append( rc.get(b"test2") )
      futs.append( rc.get(b"test2") )
      futs.append( rc.get(b"test2") )
      futs.append( rc.get(b"test2") )
      futs.append( rc.get(b"test1") )
      ret = await asyncio.gather(*futs)
      for v in ret:
        print(v)
      #rc.stat()
      await asyncio.sleep(2)

    exit()

  await rc.set(b"test1",b"tets1")
  await rc.set(b"test2",b"tets2")
  await rc.set(b"test3",b"tets3")
  await rc.set(b"test4",b"tets4")
  await rc.set(b"test5",b"tets5")
  await rc.set(b"test6",b"tets6")
  await rc.set(b"test7",b"tets7")
  await rc.set(b"test8",b"tets8")
  await rc.set(b"test9",b"tets9")
  await rc.set(b"test10",b"tets10")
  await rc.set(b"test11",b"tets11")

  while 1:
    print("top")
    futs = []
    #print(await rc.get(b"test1"))
    futs.append( rc.get(b"test1") )
    futs.append( rc.get(b"test2") )
    futs.append( rc.get(b"test3") )
    futs.append( rc.get(b"test4") )
    futs.append( rc.get(b"test5") )
    futs.append( rc.get(b"test6") )
    futs.append( rc.get(b"test7") )
    futs.append( rc.get(b"test8") )
    futs.append( rc.get(b"test9") )
    futs.append( rc.get(b"test10") )

    try:  
      print("before gather")
      ret = await asyncio.gather(*futs)
    except Exception as e:
      print(" Connection failed waiting 5: ",e)
      await asyncio.sleep(5)
      continue
    futs = []
    for v in ret:
      print(v)
    print("A")
    await asyncio.sleep(1)
    print("B")
  

  print("before close")
  await rc.close()
  print("after close")


  exit()

  await rc.set(b"test1",b"test1")
  await rc.set(b"test2",b"test2")
  await rc.set(b"test3",b"test3")
  await rc.set(b"test4",b"test4")
  
  print(await rc.get(b"test1"))
  print(await rc.get(b"test2"))
  print(await rc.get(b"test3"))
  print(await rc.get(b"test4"))
  
  exit() 

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


