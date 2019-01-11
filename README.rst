Leaky Pykafka
=============

Test repo to show leaky pykafka with rdkafka. Requires Docker and Docker Compose.

Running
-------

To run the stack, issue the following commands

.. code:: bash

   docker-compose up -d

To observe memory utilization, in a different bash write:

.. code:: bash

   docker stats
   
That should show you a screen similar to this, observe the producer's memory utilization keeps increasing, very slowly but still.
   
.. code:: bash

    CONTAINER ID        NAME                    CPU %               MEM USAGE / LIMIT     MEM %               NET I/O             BLOCK I/O           PIDS
    2e1024a7e802        kafka-test_kafka_1      0.29%               221.7MiB / 1.934GiB   11.19%              448MB / 525kB       98.3kB / 676kB      47
    8b7d63525eeb        kafka-test_zk_1         0.09%               37.58MiB / 1.934GiB   1.90%               268kB / 197kB       98.3kB / 53.2kB     17
    a33ddfff63e8        kafka-test_producer_1   99.54%              23.59MiB / 1.934GiB   1.12%               187kB / 165MB       274kB / 0B          6
