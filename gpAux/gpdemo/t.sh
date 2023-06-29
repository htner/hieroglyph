pkill -9 postgres
rm -rf /tmp/output.log
rm -rf /tmp/output1.log
rm -rf /tmp/output2.log
rm -rf /tmp/output3.log
rm -rf /tmp/output4.log

source /usr/local/sdb/greenplum_path.sh

cp -r datadirs/initdb0 datadirs/optimizer0
cp -r datadirs/initdb0 datadirs/worker1
cp -r datadirs/initdb0 datadirs/worker2
cp -r datadirs/initdb0 datadirs/worker3
cp -r datadirs/initdb0 datadirs/worker4
cp -r datadirs/initdb0 datadirs/worker5

postgres --optimizer=1 -D datadirs/optimizer0 --port=40000 
