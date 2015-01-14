#/bin/sh

curl --data @PersonInfo.avsc 'http://localhost:8080/putschema/personinfo'
# OK
printf "\n"

curl 'http://localhost:8080/personinfo/get/1'
# {"name":"","age":0,"gender":"Unknown","emails":[]}
printf "\n"

curl --data-binary @PersonInfo.update 'http://localhost:8080/personinfo/update/1'
# OK
printf "\n"

curl 'http://localhost:8080/personinfo/get/1'
# {"name":"James Bond","age":60,"gender":"Unknown","emails":[]}
printf "\n"

curl 'http://localhost:8080/personinfo/get/1/name'
# "James Bond"
printf "\n"

curl --data-binary @on_name.js 'http://localhost:8080/personinfo/putscript/name/SCRIPT_NO_1'
# OK
printf "\n"

curl --data '"John"' 'http://localhost:8080/personinfo/put/1/name'
# OK
printf "\n"

sleep 2s
curl 'http://localhost:8080/personinfo/get/2/age'
# 888
printf "\n"

curl 'http://localhost:8080/personinfo/delscript/name/SCRIPT_NO_1'
# OK
printf "\n"
