astore
====

Avro Data Store based on Akka

#### Run astore
```
$ sbt run
```

#### Access astore

Example schema: LongList.record
```json
{
  "type": "record", 
  "name": "LongList",
  "fields" : [
    {"name": "value", "type": "long", "default": 0},             
    {"name": "next", "type": ["null", "LongList"], "default": null}
  ]
}
```

Testing:
```shell
cd src/test/resources/avsc
curl --data @LongList.record 'http://localhost:8080/putschema/longlist'
curl 'http://localhost:8080/longlist/get/1'
curl --data-binary @update.value 'http://localhost:8080/longlist/update/1'
curl 'http://localhost:8080/longlist/get/1'
weighttp -c100 -n100000 -k 'http://localhost:8080/longlist/get/1'
```


# Preface

astore 以 Avro record 为存贮单元，有两组 APIs，分别为：

* Primitive API (Scala/Java)
* RESTful API

# Primitive API (Scala / Java)

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

# RESTful API

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

### Del schame 
```
GET /delschema/$entityName/ 

Host: status.wandoujia.com  
```

### Get record
```
GET /$record/get/$id/ 

Host: status.wandoujia.com  
```

### Get record field
```
GET /$record/get/$id?field=$field

Host: status.wandoujia.com  
```

### Put record
```
POST /$record/put/$id/ 

Host: status.wandoujia.com  
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
<JSON_STRING>
```

### Put record field
```
POST /$record/put/$id?field=$field 

Host: status.wandoujia.com  
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
<JSON_STRING>
```

### Select
```
POST /$record/select/$id/ 

Host: status.wandoujia.com  
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
$avpath
<JSON_STRING>
```

### Update
```
POST /$record/update/$id/$avpath

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
POST /$record/insert/$id/$avpath

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
POST /$record/insertall/$id/$avpath

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
POST /$record/delete/$id/

Host: status.wandoujia.com 
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
$avpath
```


### Clear (applicable for Array / Map only)
```
POST /$record/clear/$id/

Host: status.wandoujia.com 
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
$avpath
```


### Put Script (apply on all instances of this entity)
```
POST /$record/putscript/$scriptid/

Host: status.wandoujia.com 
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
<JavaScript>
```


* For script, the bindings will be:

    * **id**: the id string of this record

    * **record**: the avro record itself

* 在 JavaScript 脚本中，一律通过 function 来操作。在 function 中可以定义局部变量，但如果要在多个 functions 中共享这些变量，则必须在 functions 之间作为参数互相传递，而不能定义全局变量。

### Del Script (apply on all instances of this entity)
```
POST /$record/delscript/$scriptid/

Host: status.wandoujia.com 
Content-Type: application/octet-stream 
Content-Length: NNN

BODY:
```


Note:

* Replace `$record` with the object/table/entity name

* Replace `$id` with object id

* Replace `$avpath` with actual avpath expression

* Put the `$avpath` and **<JSON_STRING>** format value(s) for **update / insert / insertall** in **POST** body, separate `$avpath` and **<JSON_STRING>** with **"\n"**, and make sure it’s encoded as binary, and set **Content-Type: application/octet-stream**


# Reference

* [AvPath](https://github.com/wandoulabs/wandou-avpath)

