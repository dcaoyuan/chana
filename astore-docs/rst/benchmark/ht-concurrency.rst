======================
AStore Concurrent Thoughput under Intel© Hyper-Threading
======================


Environment
-----------

Client:
~~~~~~
HOST: Dell Inc. PowerEdge R420/0VD50G
CPU: 2 x Intel(R) Xeon(R) CPU E5-2420 v2 @ 2.20GHz (2 x 6 #core, 2 x 12 #HT)
OS: CentOS release 6.6 (Final)
Kernel: 2.6.32-504.3.3.el6.x86_64

4 apache ab concurrent processes 

Server:
~~~~~~
HOST: Dell Inc. PowerEdge R420/0VD50G
CPU: 2 x Intel(R) Xeon(R) CPU E5-2420 v2 @ 2.20GHz (2 x 6 #core, 2 x 12 #HT)
OS: CentOS Linux release 7.0.1406 (Core)
Kernel: 3.10.0-123.13.2.el7.x86_64 

Configuration
-------------
.. code:: 

  akka {
    actor.default-dispatcher {
      fork-join-executor {
        parallelism-factor = 1
        parallelism-min = 1
        parallelism-max = 22 # we'll increase it from 1 to 22 
      }
      throughput = 100
    }
  }

  akka.io.tcp {
    max-channels = 921600
    nr-of-selectors = 2
  }

  spray.can.server {
    pipelining-limit = disabled
    reaping-cycle = infinite
    request-chunk-aggregation-limit = 0
    stats-support = off
    response-size-hint = 192
  }


Prepare
-------
.. code:: shell

  curl --data @PersonInfo.avsc 'http://server_host:8080/putschema/personinfo'

Apache ab command
-----------------
.. code:: shell

  ab -c100 -n100000 -k 'http://server_host:8080/personinfo/get/1?benchmark_only=10240'

Approach
--------
- Keep 2 threads for ``akka.io.tcp.nr-of-selections``
- Increase ``akk.actor.default-dispatcher.fork-join-executor.parallelism-max`` from 1 to 22 for each round
- Access astore server via 4 ab processes from client machine concurrently through 1Gb ethernet interface
- Each ab process runs 10 times 
- Discard results of first 2-round and last 2-round, keep the 3\ :sup:`rd`\ to 8\ :sup:`th`\ rounds and average the results 
- Sum the above average results of 4 ab processes 

Benchmark chart
---------------

.. image:: ../images/ht-concurrent.png


Observation
-----------

- The 2 threads of akka-io selectors kept < 60%
- The default-dispatcher threads (from 1 to 22) kept about 90%
- There were other jvm threads kept about 10%
- The thoughput scaled almost linearly when parallelism-max <= 13
- The thoughput did not scale any more when parallelism-max >= 18, and the total CPU usage kept 100% therefrom

Conclusion
----------
- Akka scales very well under multiple-core machine
- By enabling Intel© Hyper-Threading, you can acheive about 25% more thoughtput after total CPU usage reached 50%

