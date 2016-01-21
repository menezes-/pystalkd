pystalkd
========
>Beanstalk is a simple, fast work queue.
>Its interface is generic, but was originally designed for reducing the latency of page views in high-volume web applications by running time-consuming tasks asynchronously

http://kr.github.io/beanstalkd/

pystalkd is a beanstalkd bindings targeting python3.
This library is based on https://github.com/earl/beanstalkc and should be API compatible.

Installing
-------
```
pip install pystalkd
```
or from source:
```
python setup.py install
```

Using
------
```python
>>> from pystalkd.Beanstalkd import Connection
>>> c = Connection("localhost", 11300) #if no argument is given default configuration is used
>>> c.put("hey!")
>>> job = c.reserve(0)
>>> job.body
"hey!"
```
One of the goals is to be API compatible with beanstalkc, so this tutorial should be valid: https://github.com/earl/beanstalkc/blob/master/TUTORIAL.mkd

The main differences, API wise are: 

1) where number of seconds is expected pystalkd also accepts a timedelta object

2) you can temporarily watch and use a tube using the `with` keyword

```python
print(c.using()) # "default"
with c.temporary_use("test"):
  print(c.using()) # "test"
print(c.using()) # "default"

print(c.watching()) # ["default"]
with c.temporary_use("test"):
  print(c.watching()) # ["default", "test"]
print(c.watching()) # ["default"]
```
3) you also have access to the "bytes" API. 
To maintain compatibility with beanstalkc the API worked only with strings but now
you can use the functions ending in "_bytes" (internally this is controlled using the `raw` paramater) to work directly 
with bytes

```python
from os import urandom
test_bytes = urandom(50)
job_id = c.put_bytes(test_bytes)
job = c.reserve_bytes(0)
print(job.body) # b'i\x91\xdf\xf8\x1b?zj....'

job_id2 = c.put("string")
job2 = c.reserve_bytes(0)
print(job2.body) # b'string'
```

Note: you can use `reserve_bytes` with `put` and get the raw string (not encoded), but the other way around might cause problems


Tests
-------
To test with default host and port (localhost, 11300): 
```
python3 test.py
```
To test on a specific host (if port is not specified 11300 is used)
```
python3 test.py host [port]
```

License
-------

Copyright (C) 2008-2014 Andreas Bolka.

Copyright (C) 2015-2016 Gabriel Menezes.
Licensed under the [Apache License,Version 2.0][license].

[license]: http://www.apache.org/licenses/LICENSE-2.0

