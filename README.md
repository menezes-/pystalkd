pystalkd
========

Beanstalkd bindings for python3.
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
```
One of the goals is to be API compatible with beanstalkc, so this tutorial should be valid: https://github.com/earl/beanstalkc/blob/master/TUTORIAL.mkd

The main difference, API wise, is that where number of seconds is expected pystalkd also accepts a timedelta object



Tests
-------
```
python3 test.py
```

License
-------

Copyright (C) 2008-2014 Andreas Bolka.

Copyright (C) 2014 Gabriel Menezes.
Licensed under the [Apache License,Version 2.0][license].

[license]: http://www.apache.org/licenses/LICENSE-2.0

