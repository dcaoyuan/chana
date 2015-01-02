OPGET=http://localhost:8080/longlist/get/
OPPUT=http://localhost:8080/longlist/update/

for((i=1;i<=1000;i++));
do 
    curl --data-binary @update.value ${OPPUT}${i};
    curl ${OPGET}${i};
done

for((i=1;i<=10;i++));
do
    weighttp -c100 -n100000 -k 'http://localhost:8080/longlist/get/1'
done
