Welcome to the astore wiki!
===========================

Benchmark (Simple)
------------------

Environment:
~~~~~~~~~~~~

::

    HOST: Dell Inc. PowerEdge R420/0VD50G
    CPU: 2 x Intel(R) Xeon(R) CPU E5-2420 v2 @ 2.20GHz (2 x 6 #core, 2 x 12 #HT)
    OS: CentOS Linux release 7.0.1406 (Core)

Client
~~~~~~

3 apache ab processes on one client machine request one server instance on
another machine via 1G Ethernet.

Prepare
~~~~~~~

.. code:: shell

    curl --data @PersonInfo.avsc 'http://127.0.0.1:8080/putschema/personinfo'

Simple GET
~~~~~~~~~~

Command line:

.. code:: shell

    ab -c100 -n100000 -k 'http://127.0.0.1:8080/personinfo/get/1?benchmark_only=10240'

Result:

::

    Document Path:          /personinfo/get/1?benchmark_only=10240
    Document Length:        50 bytes

    Concurrency Level:      100
    Time taken for tests:   17.706 seconds
    Complete requests:      3000000
    Failed requests:        0
    Write errors:           0
    Keep-Alive requests:    3000000
    Total transferred:      744004464 bytes
    HTML transferred:       150000900 bytes
    Requests per second:    169437.39 [#/sec] (mean)
    Time per request:       0.590 [ms] (mean)
    Time per request:       0.006 [ms] (mean, across all concurrent requests)
    Transfer rate:          41035.86 [Kbytes/sec] received

Simple PUT
~~~~~~~~~~

Command line:

.. code:: shell

    ab -c100 -n100000 -k -p PersonInfo.record -T 'application/octet-stream' \
     'http://127.0.0.1:8080/personinfo/put/1?benchmark_only=10240'

Where, PersonInfo.record:

::

    {"name":"James Bond","age":60}

Result:

::

    Document Path:          /personinfo/update/1
    Document Length:        2 bytes

    Document Path:          /personinfo1/put/1?benchmark_only=10240
    Document Length:        2 bytes

    Concurrency Level:      100
    Time taken for tests:   2.914 seconds
    Complete requests:      300000
    Failed requests:        0
    Write errors:           0
    Keep-Alive requests:    300000
    Total transferred:      59700597 bytes
    Total POSTed:           71171811
    HTML transferred:       600006 bytes
    Requests per second:    102961.35 [#/sec] (mean)
    Time per request:       0.971 [ms] (mean)
    Time per request:       0.010 [ms] (mean, across all concurrent requests)
    Transfer rate:          20009.28 [Kbytes/sec] received
                            23853.99 kb/s sent
                            43863.27 kb/s total

Comparing with simple ping/pong
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Command line:

.. code:: shell

    ab -c100 -n100000 -k 'http://127.0.0.1:8080/ping'

Result:

::

    Document Path:          /ping
    Document Length:        4 bytes

    Concurrency Level:      100
    Time taken for tests:   12.190 seconds
    Complete requests:      3000000
    Failed requests:        0
    Write errors:           0
    Keep-Alive requests:    3000000
    Total transferred:      603001206 bytes
    HTML transferred:       12000024 bytes
    Requests per second:    246100.68 [#/sec] (mean)
    Time per request:       0.406 [ms] (mean)
    Time per request:       0.004 [ms] (mean, across all concurrent requests)
    Transfer rate:          48306.96 [Kbytes/sec] received

