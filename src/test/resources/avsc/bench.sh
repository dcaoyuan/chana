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
    weighttp -c100 -n100000 -k 'http://localhost:8080/personinfo/get/1'
done
