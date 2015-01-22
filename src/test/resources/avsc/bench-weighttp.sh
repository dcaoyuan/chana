#/bin/sh

OPGET=http://127.0.0.1:8080/personinfo/get/
OPPUT=http://127.0.0.1:8080/personinfo/update/

curl --data @PersonInfo.avsc 'http://127.0.0.1:8080/putschema/personinfo'

#for((i=1;i<=1000;i++)); do 
#    curl --data-binary @PersonInfo.update ${OPPUT}${i};
#done

for((i=1;i<=10;i++)); do
    weighttp -c100 -n100000 -k 'http://127.0.0.1:8080/personinfo/get/1?benchmark_only=true'
done
