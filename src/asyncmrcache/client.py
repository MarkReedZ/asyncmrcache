
import asyncio
import socket, time
import os
import asyncmrcache

async def create_client( server, loop=None, pool_size=2, connection_timeout=1,lost_cb=None ):
  loop = loop if loop is not None else asyncio.get_event_loop()
  c = Client(server, loop, pool_size, connection_timeout, lost_cb=lost_cb)
  await c.setup_connections()
  return c
  

class MrServer():
  def __init__(self, host, pool_size=2):
    self.host = host
    if "//" in host: self.host = host.split("//")[1]
    self.port = 7000
    self.pool_size = 2
    self.num_connections = 0
    self.reconnecting = False
    self.reconnect_attempts = 0

  #def get_connection(self):
    #c = self.conns[self.next]
    #self.next = (self.next+1) % len(self.conns)
    #return c

  async def close(self):
    pass
    #fut = asyncio.open_connection( self.host, self.port, loop=self.loop )
    #try:
      #r, w = await asyncio.wait_for(fut, timeout=self.connection_timeout)
    #except asyncio.TimeoutError:


class Client(asyncmrcache.CMrClient):
  def __init__(self, server, loop, pool_size=2, connection_timeout=1, lost_cb=None):

    #if not isinstance(servers, list):
      #raise ValueError("Memcached client takes a list of (host, port) servers")

    super().__init__()
    self.loop = loop
    self.server = MrServer(server, pool_size)
    self.connection_timeout = connection_timeout
    self.pool_size = pool_size
    self.debug_data = b""
    self.lost_cb = lost_cb
    self._pause_waiter = None
    self.q = None

  def pause(self):
    self._pause_waiter = self.loop.create_future()
  def resume(self):
    waiter = self._pause_waiter
    if waiter is not None:
      self._pause_waiter = None
      if not waiter.done():
        waiter.set_result(None)

  async def setup_connections(self):
    try:
      s = self.server
      for c in range(self.pool_size):
        fut = self.loop.create_connection(lambda: asyncmrcache.MrProtocol(self), s.host, s.port)
        try:
          await asyncio.wait_for(fut, timeout=self.connection_timeout)
        except asyncio.TimeoutError:
          print("TODO timeout on connection")
          exit(1)
        
    except ConnectionRefusedError:
      print("Could not connect to the mrcache server(s)")
      exit(1)
    except Exception as e:
      print(e)
      print("wtf")
      exit(1)

  async def close(self):
    #for s in self.servers:
    await self.server.close()


  async def reconnect(self):
    s = self.server
    while True:
      try:
        await self.loop.create_connection(lambda: asyncmrcache.MrProtocol(self), s.host, s.port)
        s.num_connections += 1
        s.reconnect_attempts = 0
        s.reconnecting = False  #TODO make sure open pool size number of conns
        return
      except ConnectionRefusedError:
        print("Reconnect failed to", s.host, "port", s.port)
        await asyncio.sleep(10)
      except Exception as e:
        print(e)
        await asyncio.sleep(30)

  def lost_connection(self):
    if self.lost_cb: self.lost_cb(self)
    s = self.server
    s.num_connections -= 1
    print( "    Lost connection to",s.host, "port",s.port )
    if not s.reconnecting:
      s.reconnecting = True
      asyncio.ensure_future( self.reconnect() )

  def get_new_respq(self):
    #return self.loop.create_future()
    self.q = asyncio.Queue(loop=self.loop)
    return self.q

  def push_data(self, data):
    #self.debug_data.append(data)
    self.debug_data += data
  def clear_data(self):
    self.debug_data = b""
  

  async def get(self, key):
    q = self._get(key)
    ret = await q.get()
    q.task_done()
    return ret

  # TODO need a drain?
  async def set(self, key, val):
    self._set(key, val)
    if self._pause_waiter != None:
      await self._pause_waiter

    #c = self.server.get_connection()
    #c.w.write(b'get '  + key + b'\r\n')
    #r = await c.respq.get()
    #try:
      #return r[key][0]
    #except:
      #return None

  def stat(self):
    self._stat()


