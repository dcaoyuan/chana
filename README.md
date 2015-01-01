astore
====

Avro Data Store based on Akka

#### Run astore
```
$ sbt run
```

#### Access astore

```
$ cd src/test/resources/avsc
$ curl --data @LongList.record 'http://localhost:8080/putschema/longlist'
$ curl 'http://localhost:8080/longlist/get/1'
$ curl --data-binary @update.value 'http://localhost:8080/longlist/update/1'
$ curl 'http://localhost:8080/longlist/get/1'
$ weighttp -c100 -n100000 -k 'http://localhost:8080/longlist/get/1'
```
