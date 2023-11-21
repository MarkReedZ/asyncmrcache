# AsyncMrcache
An async python 3.5+ mrcache client

# Installation

-  ``pip install asyncmrcache``

# Usage

```python
import asyncio, time
import asyncmrcache

def lcb(client):
  print( "Connection lost" )

async def run(loop):
  mrc = await asyncmrcache.create_client( "localhost", loop, lost_cb=lcb)

  await mrc.set(b"test1",b"tets1")
  print(await mrc.get(b"test1"))

  await mrc.close()


if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
```


# Benchmarks

```

```



