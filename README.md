astore
====

Avro Data Store based on Akka (TODO persistence)

#### Run astore
```
$ sbt run
```

#### Access astore

##### Example 1: Simple Record

Schema: PersonInfo.avsc
```json
{
  "type": "record",
  "namespace": "avro",
  "name": "PersonInfo",
  "fields": [
    {
      "name": "first",
      "type": "string",
      "default": ""
    },
    {
      "name": "last",
      "type": "string",
      "default": ""
    },
    {
      "name": "age",
      "type": "int",
      "default": 0
    }
  ]
}

```

Testing:
```shell
cd src/test/resources/avsc
curl --data @PersonInfo.avsc 'http://localhost:8080/putschema/personinfo'
curl 'http://localhost:8080/personinfo/get/1'
curl --data-binary @PersonInfo.update 'http://localhost:8080/personinfo/update/1'
curl 'http://localhost:8080/personinfo/get/1'
curl 'http://localhost:8080/personinfo/get/1/first'
weighttp -c100 -n100000 -k 'http://localhost:8080/personinfo/get/1'
```

##### Example 2: Using Embedded Records

Schema: hatInventory.avsc
```json
{
  "type": "record",
  "name": "hatInventory",
  "namespace": "avro",
  "fields": [
    {
      "name": "sku",
      "type": "string",
      "default": ""
    },
    {
      "name": "description",
      "type": {
        "type": "record",
        "name": "hatInfo",
        "fields": [
          {
            "name": "style",
            "type": "string",
            "default": ""
          },
          {
            "name": "size",
            "type": "string",
            "default": ""
          },
          {
            "name": "color",
            "type": "string",
            "default": ""
          },
          {
            "name": "material",
            "type": "string",
            "default": ""
          }
        ]
      },
      "default": {}
    }
  ]
}
```

Testing:
```shell
cd src/test/resources/avsc
curl --data @hatInventory.avsc 'http://localhost:8080/putschema/hatinv'
curl 'http://localhost:8080/hatinv/get/1'
curl --data '{"style": "classic", "size": "Large", "color": "Red"}' 'http://localhost:8080/hatinv/put/1/description'
curl 'http://localhost:8080/hatinv/get/1'
curl 'http://localhost:8080/hatinv/get/1/description'
weighttp -c100 -n100000 -k 'http://localhost:8080/hatinv/get/1'
```
##### Simple benchmark test (too simple too naive)
Simple bench.sh content:
```shell
OPGET=http://localhost:8080/personinfo/get/
OPPUT=http://localhost:8080/personinfo/update/

curl --data @PersonInfo.avsc 'http://localhost:8080/putschema/personinfo'

for((i=1;i<=1000;i++));
do 
    curl --data-binary @PersonInfo.update ${OPPUT}${i};
    curl ${OPGET}${i};
done

for((i=1;i<=10;i++));
do
    ab -c100 -n100000 -k 'http://localhost:8080/personinfo/get/1'
done
```

To run:
```shell
sbt run
cd src/test/resources/avsc
./bench.sh
```

Some results:
 * Dell Inc. PowerEdge R420/0VD50G, 2 x Intel(R) Xeon(R) CPU E5-2420 v2 @ 2.20GHz
```
Server Software:        spray-can/1.3.2-20140909
Server Hostname:        localhost
Server Port:            8080

Document Path:          /personinfo/get/1
Document Length:        30 bytes

Concurrency Level:      100
Time taken for tests:   1.968 seconds
Complete requests:      100000
Failed requests:        0
Write errors:           0
Keep-Alive requests:    100000
Total transferred:      23700000 bytes
HTML transferred:       3000000 bytes
Requests per second:    50804.77 [#/sec] (mean)
Time per request:       1.968 [ms] (mean)
Time per request:       0.020 [ms] (mean, across all concurrent requests)
Transfer rate:          11758.53 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.1      0       3
Processing:     1    2   1.2      2      19
Waiting:        1    2   1.2      2      19
Total:          1    2   1.3      2      20
``` 

# Preface

astore stores Avro record, with two groups of APIs:

* Primitive API (Scala/Java)
* RESTful API

## Primitive API (Scala / Java)

use AvPath expression to locate，see「[AvPath](https://github.com/wandoulabs/wandou-avpath)」

### 1. Basic operations
```scala
case class GetRecord(id: String)
case class PutRecord(id: String, record: Record)
case class PutRecordJson(id: String, record: String)
case class GetField(id: String, field: String)
case class PutField(id: String, field: String, value: Any)
case class PutFieldJson(id: String, field: String, value: String)

case class Select(id: String, path: String)
case class Update(id: String, path: String, value: Any)
case class UpdateJson(id: String, path: String, value: String)
```
### 2. Operations applicable on Array / Map
```scala
case class Insert(id: String, path: String, value: Any)
case class InsertJson(id: String, path: String, value: String)
case class InsertAll(id: String, path: String, values: List[_])
case class InsertAllJson(id: String, path: String, values: String)
case class Delete(id: String, path: String)
case class Clear(id: String, path: String)
```
### 3. Script
```scala
case class PutScript(entity: String, id: String, script: String)
case class DelScript(entity: String, id: String)
```

### 4. Schema
```scala
case class PutSchema(entityName: String, schema: Schema, entityFullName: Option[String] = None)
case class DelSchema(entityName: String)
```

## RESTful API

* Note: URL valid charaters (see [http://tools.ietf.org/html/rfc3986#section-2](http://tools.ietf.org/html/rfc3986#section-2))
ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/?#[]@!$&'()*+,;=

## URL format

### Put schema 
```
POST /putschema/$entityName/ 

Host: status.wandoujia.com  
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
<SCHEMA_STRING>
```

### Put schema that contains multiple referenced complex types in union 
```
POST /putschema/$entityName/$entryEntityFullName 

Host: status.wandoujia.com  
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
<SCHEMA_STRING>
```


### Del schame 
```
GET /delschema/$entityName/ 

Host: status.wandoujia.com  
```

### Get record
```
GET /$entity/get/$id/ 

Host: status.wandoujia.com  
```

### Get record field
```
GET /$entity/get/$id/$field

Host: status.wandoujia.com  
```

### Put record
```
POST /$entity/put/$id/ 

Host: status.wandoujia.com  
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
<JSON_STRING>
```

### Put record field
```
POST /$entity/put/$id/$field 

Host: status.wandoujia.com  
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
<JSON_STRING>
```

### Select
```
POST /$entity/select/$id/ 

Host: status.wandoujia.com  
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
$avpath
<JSON_STRING>
```

### Update
```
POST /$entity/update/$id/$avpath

Host: status.wandoujia.com 
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
<JSON_STRING>
```
Example (update array field -> record’s number field):
```
POST /account/update/12345/
BODY: 
.chargeRecords[0].time
1234
```
Example (update map field -> record’s number field):
```
POST /account/update/12345/
BODY:
.devApps("a"|"b").numBlackApps
1234
```

### Insert (applicable for Array / Map only)
```
POST /$entity/insert/$id/$avpath

Host: status.wandoujia.com 
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
<JSON_STRING>
```
Example (insert to array field):
```
POST /account/insert/12345/
BODY: 
.chargeRecords
{"time": 4, "amount": -4.0}
```
Example (insert to map field):
```
POST /account/insert/12345/
BODY: 
.devApps
{"h" : {"numBlackApps": 10}}
```

### InsertAll (applicable for Array / Map only)
```
POST /$entity/insertall/$id/$avpath

Host: status.wandoujia.com 
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
<JSON_STRING>
```
Example (insert to array field):
```
POST /account/insertall/12345/
BODY: 
.chargeRecords
[{"time": -1, "amount": -5.0}, {"time": -2, "amount": -6.0}]
```

Example (insert to map field):
```
POST /account/insertall/12345/
BODY: 
.devApps
{"g" : {}, "h" : {"numBlackApps": 10}}
```

### Delete (applicable for Array / Map only)
```
POST /$entity/delete/$id/

Host: status.wandoujia.com 
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
$avpath
```


### Clear (applicable for Array / Map only)
```
POST /$entity/clear/$id/

Host: status.wandoujia.com 
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
$avpath
```


### Put Script (apply on all instances of this entity)
```
POST /$entity/putscript/$scriptid/

Host: status.wandoujia.com 
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
<JavaScript>
```


* For script, the bindings will be:

    * **id**: the id string of this record

    * **record**: the avro record itself

* The JavaScript code should do what ever operation via function only. You can define local variables in function, and transfer these local vars between functions to share them instead of defining global vars. 

### Del Script (apply on all instances of this entity)
```
POST /$entity/delscript/$scriptid/

Host: status.wandoujia.com 
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
```


Note:

* Replace `$entity` with the object/table/entity name

* Replace `$id` with object id

* Replace `$avpath` with actual avpath expression

* Put the `$avpath` and **<JSON_STRING>** format value(s) for **update / insert / insertall** in **POST** body, separate `$avpath` and **<JSON_STRING>** with **"\n"**, and make sure it’s encoded as binary, and set **Content-Type: application/octet-stream**


# Reference

* [AvPath](https://github.com/wandoulabs/wandou-avpath)

