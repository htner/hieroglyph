export TCMALLOC_SAMPLE_PARAMETER=524288
export TCMALLOC_RELEASE_RATE=10
pkill -9 postgres
#sudo bash -c 'echo 1 > /proc/sys/net/ipv4/ip_forward'
sudo bash -c 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse'
sleep 1
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

rm datadirs/worker1 -rf
cp -r datadirs/initdb0 datadirs/worker1
rm -rf datadirs/worker1/base/1/*
cd datadirs/worker1/global/
ls | grep -E "^[0-9]{4,}" | xargs rm -r
cd ../../..

rm datadirs/worker2 -rf
cp -r datadirs/initdb0 datadirs/worker2
rm -rf datadirs/worker2/base/1/*
cd datadirs/worker2/global/
ls | grep -E "^[0-9]{4,}" | xargs rm -r
cd ../../..

rm datadirs/worker3 -rf
cp -r datadirs/initdb0 datadirs/worker3
rm -rf datadirs/worker3/base/1/*
cd datadirs/worker3/global/
ls | grep -E "^[0-9]{4,}" | xargs rm -r
cd ../../..

rm datadirs/worker4 -rf
cp -r datadirs/initdb0 datadirs/worker4
rm -rf datadirs/worker4/base/1/*
cd datadirs/worker4/global/
ls | grep -E "^[0-9]{4,}" | xargs rm -r
cd ../../..

rm datadirs/worker5 -rf
cp -r datadirs/initdb0 datadirs/worker5
rm -rf datadirs/worker5/base/1/*
cd datadirs/worker5/global/
ls | grep -E "^[0-9]{4,}" | xargs rm -r
cd ../../..

rm datadirs/worker6 -rf
cp -r datadirs/initdb0 datadirs/worker6
rm -rf datadirs/worker6/base/1/*
cd datadirs/worker6/global/
ls | grep -E "^[0-9]{4,}" | xargs rm -r
cd ../../..



# postgres --optimizer=1 -D datadirs/optimizer0 --port=40000 
nohup postgres --optimizer=1 --dir=datadirs/optimizer0/ --host=127.0.0.1 --port=8999 --bthread_concurrency=4 --database=template1 --dbid=1 --endpoint=127.0.0.1:9000 --s3user=minioadmin --s3passwd=minioadmin --isminio=true --bucket=sdb1 --region=us-east-1 > datadirs/optimizer0/optimizer0.log 2>&1 &
nohup postgres --worker=1 --dir=datadirs/worker1/ --host=127.0.0.1 --port=9101 --bthread_concurrency=4 --database=template1 --dbid=1 --endpoint=127.0.0.1:9000 --s3user=minioadmin --s3passwd=minioadmin --isminio=true --bucket=sdb1 --region=us-east-1 > datadirs/worker1/worker1.log 2>&1 &
nohup postgres --worker=1 --dir=datadirs/worker2/ --host=127.0.0.1 --port=9102 --bthread_concurrency=4 --database=template1 --dbid=1 --endpoint=127.0.0.1:9000 --s3user=minioadmin --s3passwd=minioadmin --isminio=true --bucket=sdb1 --region=us-east-1 > datadirs/worker2/worker2.log 2>&1 &
nohup postgres --worker=1 --dir=datadirs/worker3/ --host=127.0.0.1 --port=9103 --bthread_concurrency=4 --database=template1 --dbid=1 --endpoint=127.0.0.1:9000 --s3user=minioadmin --s3passwd=minioadmin --isminio=true --bucket=sdb1 --region=us-east-1 > datadirs/worker3/worker3.log 2>&1 &
nohup postgres --worker=1 --dir=datadirs/worker4/ --host=127.0.0.1 --port=9104 --bthread_concurrency=4 --database=template1 --dbid=1 --endpoint=127.0.0.1:9000 --s3user=minioadmin --s3passwd=minioadmin --isminio=true --bucket=sdb1 --region=us-east-1 > datadirs/worker4/worker4.log 2>&1 &
nohup postgres --worker=1 --dir=datadirs/worker5/ --host=127.0.0.1 --port=9105 --bthread_concurrency=4 --database=template1 --dbid=1 --endpoint=127.0.0.1:9000 --s3user=minioadmin --s3passwd=minioadmin --isminio=true --bucket=sdb1 --region=us-east-1 > datadirs/worker5/worker5.log 2>&1 &
nohup postgres --worker=1 --dir=datadirs/worker6/ --host=127.0.0.1 --port=9106 --bthread_concurrency=4 --database=template1 --dbid=1 --endpoint=127.0.0.1:9000 --s3user=minioadmin --s3passwd=minioadmin --isminio=true --bucket=sdb1 --region=us-east-1 > datadirs/worker6/worker6.log 2>&1 &
