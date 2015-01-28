astore
======

Avro Data Store based on Akka (TODO persistence)

Core Design
^^^^^^^^^^^

-  Each record is an actor (non-blocking)
-  Akka sharding cluster (easy to scale-out)
-  Locate field/value deeply via
   `avpath <https://github.com/wandoulabs/avpath>`__
-  Scripting triggered by field updating events (JDK 8 JavaScript engine
   -
   `Nashorn <http://docs.oracle.com/javase/8/docs/technotes/guides/scripting/nashorn/>`__)

Run astore
^^^^^^^^^^

.. code:: shell

    $ sbt run

Or

.. code:: shell

    $ sbt clean compile dist
    $ ls target/universal/
    tmp  astore-0.1.1-SNAPSHOT.zip 

Then, copy astore-0.1.1.-SNAPSHOT.zip to somewhere and unzip it

.. code:: shell

    $ cd astore-0.1.1-SNAPSHOT/bin
    $ ./astore

Access astore
^^^^^^^^^^^^^

Example 1: Simple Record
''''''''''''''''''''''''

Schema: PersonInfo.avsc

.. code:: json

    {
      "type" : "record",
      "name" : "PersonInfo",
      "namespace" : "astore",
      "fields" : [ {
        "name" : "name",
        "type" : "string"
      }, {
        "name" : "age",
        "type" : "int"
      }, {
        "name" : "gender",
        "type" : {
          "type" : "enum",
          "name" : "GenderType",
          "symbols" : [ "Female", "Male", "Unknown" ]
        },
        "default" : "Unknown"
      }, {
        "name" : "emails",
        "type" : {
          "type" : "array",
          "items" : "string"
        }
      } ]
    }

Try it:

.. code:: shell

    $ cd src/test/resources/avsc

    $ curl --data @PersonInfo.avsc 'http://127.0.0.1:8080/putschema/personinfo'
    OK

    $ curl 'http://127.0.0.1:8080/personinfo/get/1'
    {"name":"","age":0,"gender":"Unknown","emails":[]}

    $ curl --data-binary @PersonInfo.update 'http://127.0.0.1:8080/personinfo/update/1'
    OK

    $ curl 'http://127.0.0.1:8080/personinfo/get/1'
    {"name":"James Bond","age":60,"gender":"Unknown","emails":[]}

    $ curl 'http://127.0.0.1:8080/personinfo/get/1/name'
    "James Bond"

    $ ab -c100 -n100000 -k 'http://127.0.0.1:8080/personinfo/get/1?benchmark_only=1024'

Script example: (requires JDK8+)
''''''''''''''''''''''''''''''''

A piece of JavaScript code that will be executed when field
PersionInfo.name was updated: on\_name.js:

.. code:: javascript

    function onNameUpdated() {
        var age = record.get("age");
        what_is(age);
        what_is(http_get);
        http_get.apply("http://localhost:8080/ping");
        http_post.apply("http://localhost:8080/personinfo/put/2/age", "888");
        for (i = 0; i < fields.length; i++) {
            var field = fields[i];
            what_is(field._1);
            what_is(field._2);
        }
    }

    function what_is(value) {
        print(id + ": " + value);
    }

    onNameUpdated();

Try it:

.. code:: shell

    $ curl --data-binary @on_name.js \
     'http://127.0.0.1:8080/personinfo/putscript/name/SCRIPT_NO_1'
    OK

    $ curl --data '"John"' 'http://127.0.0.1:8080/personinfo/put/1/name'
    OK

    $ curl 'http://127.0.0.1:8080/personinfo/get/2/age'
    888

Example 2: With Embedded Type
'''''''''''''''''''''''''''''

Schema: hatInventory.avsc

.. code:: json

    {
      "type" : "record",
      "name" : "hatInventory",
      "namespace" : "astore",
      "fields" : [ {
        "name" : "sku",
        "type" : "string",
        "default" : ""
      }, {
        "name" : "description",
        "type" : {
          "type" : "record",
          "name" : "hatInfo",
          "fields" : [ {
            "name" : "style",
            "type" : "string",
            "default" : ""
          }, {
            "name" : "size",
            "type" : "string",
            "default" : ""
          }, {
            "name" : "color",
            "type" : "string",
            "default" : ""
          }, {
            "name" : "material",
            "type" : "string",
            "default" : ""
          } ]
        },
        "default" : { }
      } ]
    }

Try it:

.. code:: shell

    $ cd src/test/resources/avsc

    $ curl --data @hatInventory.avsc 'http://127.0.0.1:8080/putschema/hatinv'
    OK

    $ curl 'http://127.0.0.1:8080/hatinv/get/1'
    {"sku":"","description":{"style":"","size":"","color":"","material":""}}

    $ curl --data '{"style":"classic","size":"Large","color":"Red"}' \
     'http://127.0.0.1:8080/hatinv/put/1/description'
    OK

    $ curl 'http://127.0.0.1:8080/hatinv/get/1'
    {"sku":"","description":{"style":"classic","size":"Large","color":"Red","material":""}}

    $ curl 'http://127.0.0.1:8080/hatinv/get/1/description'
    {"style":"classic","size":"Large","color":"Red","material":""}

    $ ab -c100 -n100000 -k 'http://127.0.0.1:8080/hatinv/get/1?benchmark_only=1024'

Simple benchmark for REST-JSON API (too simple too naive)
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Environment:
            

::

    HOST: Dell Inc. PowerEdge R420/0VD50G
    CPU: 2 x Intel(R) Xeon(R) CPU E5-2420 v2 @ 2.20GHz (12 #core, 24 #HT)
    OS: CentOS Linux release 7.0.1406 (Core)

Simple GET/PET REST-JSON Result:
                                

::

    Simple GET: 169,437 [req#/sec] (mean)
    Simple PET: 102,961 [req#/sec] (mean)

Details: `Benchmark <https://github.com/wandoulabs/astore/wiki>`__

To run:
       

.. code:: shell

    sbt run
    cd src/test/resources/avsc
    ./bench.sh
    ./bench-put.sh

Preface
=======

astore stores Avro record, with two groups of APIs:

-  Primitive API (Scala/Java)
-  RESTful API

Primitive API (Scala / Java)
----------------------------

use **avpath** expression to locate. see
`avpath <https://github.com/wandoulabs/avpath>`__

1. Schema
~~~~~~~~~

.. code:: scala

    case class PutSchema(entityName: String, schema: String, entityFullName: Option[String])
    case class RemoveSchema(entityName: String)

2. Basic operations
~~~~~~~~~~~~~~~~~~~

.. code:: scala

    case class GetRecord(id: String)
    case class GetRecordAvro(id: String)
    case class GetRecordJson(id: String)
    case class PutRecord(id: String, record: Record)
    case class PutRecordJson(id: String, record: String)
    case class GetField(id: String, field: String)
    case class GetFieldAvro(id: String, field: String)
    case class GetFieldJson(id: String, field: String)
    case class PutField(id: String, field: String, value: Any)
    case class PutFieldJson(id: String, field: String, value: String)

    case class Select(id: String, path: String)
    case class SelectAvro(id: String, path: String)
    case class SelectJson(id: String, path: String)
    case class Update(id: String, path: String, value: Any)
    case class UpdateJson(id: String, path: String, value: String)

3. Operations applicable on Array / Map
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: scala

    case class Insert(id: String, path: String, value: Any)
    case class InsertJson(id: String, path: String, value: String)
    case class InsertAll(id: String, path: String, values: List[_])
    case class InsertAllJson(id: String, path: String, values: String)
    case class Delete(id: String, path: String)
    case class Clear(id: String, path: String)

4. Script
~~~~~~~~~

.. code:: scala

    case class PutScript(entity: String, field: String, id: String, script: String)
    case class RemoveScript(entity: String, field: String, id: String)

RESTful API
-----------

Put schema
~~~~~~~~~~

::

    POST /putschema/$entityName/ 

    Host: status.wandoujia.com  
    Content-Type: application/octet-stream 
    Content-Length: NNN

    BODY:
    <SCHEMA_STRING>

Put schema that contains multiple referenced complex types in union
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    POST /putschema/$entityName/$entryEntityFullName 

    Host: status.wandoujia.com  
    Content-Type: application/octet-stream 
    Content-Length: NNN

    BODY:
    <SCHEMA_STRING>

Del schame
~~~~~~~~~~

::

    GET /delschema/$entityName/ 

    Host: status.wandoujia.com  

Get record
~~~~~~~~~~

::

    GET /$entity/get/$id/ 

    Host: status.wandoujia.com  

Get record field
~~~~~~~~~~~~~~~~

::

    GET /$entity/get/$id/$field

    Host: status.wandoujia.com  

Put record
~~~~~~~~~~

::

    POST /$entity/put/$id/ 

    Host: status.wandoujia.com  
    Content-Type: application/octet-stream 
    Content-Length: NNN

    BODY:
    <JSON_STRING>

Put record field
~~~~~~~~~~~~~~~~

::

    POST /$entity/put/$id/$field 

    Host: status.wandoujia.com  
    Content-Type: application/octet-stream 
    Content-Length: NNN

    BODY:
    <JSON_STRING>

Select
~~~~~~

::

    POST /$entity/select/$id/ 

    Host: status.wandoujia.com  
    Content-Type: application/octet-stream 
    Content-Length: NNN

    BODY:
    $avpath
    <JSON_STRING>

Update
~~~~~~

::

    POST /$entity/update/$id/$avpath

    Host: status.wandoujia.com 
    Content-Type: application/octet-stream 
    Content-Length: NNN

    BODY:
    <JSON_STRING>

Example (update array field -> record’s number field):

::

    POST /account/update/12345/
    BODY: 
    .chargeRecords[0].time
    1234

Example (update map field -> record’s number field):

::

    POST /account/update/12345/
    BODY:
    .devApps("a"|"b").numBlackApps
    1234

Insert (applicable for Array / Map only)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    POST /$entity/insert/$id/$avpath

    Host: status.wandoujia.com 
    Content-Type: application/octet-stream 
    Content-Length: NNN

    BODY:
    <JSON_STRING>

Example (insert to array field):

::

    POST /account/insert/12345/
    BODY: 
    .chargeRecords
    {"time": 4, "amount": -4.0}

Example (insert to map field):

::

    POST /account/insert/12345/
    BODY: 
    .devApps
    {"h" : {"numBlackApps": 10}}

InsertAll (applicable for Array / Map only)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    POST /$entity/insertall/$id/$avpath

    Host: status.wandoujia.com 
    Content-Type: application/octet-stream 
    Content-Length: NNN

    BODY:
    <JSON_STRING>

Example (insert to array field):

::

    POST /account/insertall/12345/
    BODY: 
    .chargeRecords
    [{"time": -1, "amount": -5.0}, {"time": -2, "amount": -6.0}]

Example (insert to map field):

::

    POST /account/insertall/12345/
    BODY: 
    .devApps
    {"g" : {}, "h" : {"numBlackApps": 10}}

Delete (applicable for Array / Map only)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    POST /$entity/delete/$id/

    Host: status.wandoujia.com 
    Content-Type: application/octet-stream 
    Content-Length: NNN

    BODY:
    $avpath

Clear (applicable for Array / Map only)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    POST /$entity/clear/$id/

    Host: status.wandoujia.com 
    Content-Type: application/octet-stream 
    Content-Length: NNN

    BODY:
    $avpath

Put Script (apply on all instances of this entity)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    POST /$entity/putscript/$field/$scriptid/

    Host: status.wandoujia.com 
    Content-Type: application/octet-stream 
    Content-Length: NNN

    BODY:
    <JavaScript>

Del Script (apply on all instances of this entity)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    GET /$entity/delscript/$field/$scriptid/

    Host: status.wandoujia.com 

Note:

-  Replace ``$entity`` with the object/table/entity name
-  Replace ``$id`` with object id
-  Replace ``$avpath`` with actual avpath expression
-  Put the ``$avpath`` and **** format value(s) for **update / insert /
   insertall** in **POST** body, separate ``$avpath`` and **** with
   **""**, and make sure it’s encoded as binary, set **Content-Type:
   application/octet-stream**

Scripting supporting
--------------------

The bindings that could be accessed in script:

.. code:: scala

      def prepareBindings(onUpdated: OnUpdated) = {
        val bindings = new SimpleBindings
        bindings.put("http_get", http_get)
        bindings.put("http_post", http_post)
        bindings.put("id", onUpdated.id)
        bindings.put("record", onUpdated.recordAfter)
        bindings.put("fields", onUpdated.fieldsBefore)
        bindings
      }

Where, \* ``http_get``: a function could be invoked via
``http_get.apply(url: String)`` \* ``http_post``: a function could be
invoked via ``http_post.apply(url: String, body: String)`` \* ``id``:
the id of this entity \* ``record``: the entity record after updated \*
``fields``: array of tuple (Schema.Field, valueBeforeUpdated) during
this updating action \* ``fields(i)._1``:
`org.apache.avro.Schema.Field <https://avro.apache.org/docs/1.7.7/api/java/org/apache/avro/Schema.Field.html>`__
\* ``fields(i)._2``: value

-  The JavaScript code should do what ever operation via function only.
   You can define local variables in function, and transfer these local
   vars between functions to share them instead of defining global vars.

Reference
=========

-  `avpath <https://github.com/wandoulabs/avpath>`__
-  `Nashorn <https://wiki.openjdk.java.net/display/Nashorn/Main>`__

