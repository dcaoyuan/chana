#/bin/sh

curl --data @Kwafer.avsc 'http://localhost:8080/putschema/kwafer/?fullname=com.wandoujia.kwaf.schema.Kwafer'
# OK
printf "\n"

curl 'http://localhost:8080/kwafer/get/1'
#
printf "\n"

curl --data-binary @Kwafer.update 'http://localhost:8080/kwafer/put/1'
# OK
printf "\n"

curl 'http://localhost:8080/kwafer/get/1'
#
printf "\n"

curl --data '.accountRecords[0].action.type' 'http://localhost:8080/kwafer/select/1'
# 
printf "\n"

