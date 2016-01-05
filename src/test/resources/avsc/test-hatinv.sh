#!/bin/sh

curl --data @hatInventory.avsc 'http://localhost:8080/schema/put/hatinv?timeout=5000'
# OK
printf "\n"

curl 'http://localhost:8080/hatinv/get/1'
# {"sku":"","description":{"style":"","size":"","color":"","material":""}}
printf "\n"

curl --data '{"style":"classic","size":"Large","color":"Red"}' 'http://localhost:8080/hatinv/put/1/description'
# OK
printf "\n"

curl 'http://localhost:8080/hatinv/get/1'
# {"sku":"","description":{"style":"classic","size":"Large","color":"Red","material":""}}
printf "\n"

curl 'http://localhost:8080/hatinv/get/1/description'
# {"style":"classic","size":"Large","color":"Red","material":""}
printf "\n"

