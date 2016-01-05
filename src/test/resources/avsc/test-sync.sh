#!/bin/bash

## Usage:
##	bash t.sh <key> <steps>

# echo ":: init ::"

ITEM=${1-'item'}
NUM=${2-10}

# put schema
curl 'http://localhost:8080/schema/put/a'\
	--data '{"name":"RecordA", "type":"record", "fields":[{ "name": "steps", "type": { "type": "map", "values": "int" } }]}'
#sleep 5s
curl 'http://localhost:8080/schema/put/b'\
	--data '{"name":"RecordB", "type":"record", "fields":[{ "name": "status", "type": "string" }]}'
sleep 5s

# put script
curl 'http://localhost:8080/a/script/put/steps/steps_on_update'\
	--data 'print("SSM", unescape(id), record.get("steps"));'
curl 'http://localhost:8080/b/script/put/status/status_on_update'\
	--data 'var ff=unescape(id), f=ff.split(":"); print("TSM", ff); http_post.apply("http://localhost:8080/a/insert/"+f[0], "/steps\n{\""+f[1]+"\":1}", 5);'
sleep 5s

echo
echo ":: test ::"

# test start
let max=$NUM-1
for i in `seq 0 $max`; do
	echo -e -n '/status\n"Ok"' | curl --data-binary @- "http://localhost:8080/b/update/$ITEM:$i"&
done
sleep 5s

echo
echo ":: check ::"
curl "http://localhost:8080/a/get/$ITEM"
echo

# The final result should be proper ordered as:
# {"steps":{"0":1,"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1,"8":1,"9":1}}
