export TCMALLOC_SAMPLE_PARAMETER=524288
export TCMALLOC_RELEASE_RATE=10
pkill -9 postgres
#sudo bash -c 'echo 1 > /proc/sys/net/ipv4/ip_forward'
sudo bash -c 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse'
sleep 1

worker_num = 20

source /usr/local/sdb/greenplum_path.sh

rm datadirs/optimizer0 -rf
cp -r datadirs/initdb0 datadirs/optimizer0
rm -rf datadirs/optimizer0/base/1/*
cd datadirs/optimizer0/global/
ls | grep -E "^[0-9]{4,}" | xargs rm -r
cd ../../..
#ls datadirs/optimizer0/global/ -a | grep -E "^[0-9]{4,}" 
nohup postgres --optimizer=1 --dir=datadirs/optimizer0/ --port=8999 --bthread_concurrency=4 --database=template1 --dbid=1 --endpoint=127.0.0.1:9000 --s3user=minioadmin --s3passwd=minioadmin --isminio=true --bucket=sdb1 --region=ap1 > datadirs/optimizer0/optimizer0.log 2>&1 &


for (( worker_id=1; worker_id<=$worker_num; worker_id++ ))
do
    rm datadirs/worker{$worker_id} -rf
    cp -r datadirs/initdb0 datadirs/worker{$worker_id}
    rm -rf datadirs/worker{$worker_id}/base/1/*
    cd datadirs/worker{$worker_id}/global/
    ls | grep -E "^[0-9]{4,}" | xargs rm -r
    cd ../../..
done


for (( worker_id=1; worker_id<=$worker_num; worker_id++ ))
do
    nohup postgres --worker=1 --dir=datadirs/worker1/ --port=9101 --bthread_concurrency=4 --database=template1 --dbid=1 --endpoint=127.0.0.1:9000 --s3user=minioadmin --s3passwd=minioadmin --isminio=true --bucket=sdb1 --region=ap1 > datadirs/worker1/worker1.log 2>&1 &
done