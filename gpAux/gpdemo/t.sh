pkill -9 postgres
rm -rf /tmp/output.log
rm -rf /tmp/output1.log
rm -rf /tmp/output2.log
rm -rf /tmp/output3.log
rm -rf /tmp/output4.log

source /usr/local/sdb/greenplum_path.sh

rm datadirs/optimizer0 -rf
cp -r datadirs/initdb0 datadirs/optimizer0
rm -rf datadirs/optimizer0/base/1/*
cd datadirs/optimizer0/global/
ls | grep -E "^[0-9]{4,}" | xargs rm -r
cd ../../..
#ls datadirs/optimizer0/global/ -a | grep -E "^[0-9]{4,}" 

cp -r datadirs/initdb0 datadirs/worker1
cp -r datadirs/initdb0 datadirs/worker2
cp -r datadirs/initdb0 datadirs/worker3
cp -r datadirs/initdb0 datadirs/worker4
cp -r datadirs/initdb0 datadirs/worker5

postgres --optimizer=1 -D datadirs/optimizer0 --port=40000 
