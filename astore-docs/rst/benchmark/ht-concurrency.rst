======================
Concurrent Behavior with Hyper-Threading on
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
        parallelism-max = 22  
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
- Stick 2 threads for ``akka.io.tcp.nr-of-selections``
- Increase ``akk.actor.default-dispatcher.fork-join-executor.parallelism-max`` from 1 to 22 
- Access astore server via 4 ab processes from client machine concurrently through 1Gb ethernet interface
- Each ab process runs 10 rounds
- Discard results of first 2-rounds and last 2-rounds, keep the 3rd to 8th rounds and average the results 
- Sum the above average results of 4 ab processes 

Benchmark chart
---------------

.. image:: ../images/ht-concurrent.png
