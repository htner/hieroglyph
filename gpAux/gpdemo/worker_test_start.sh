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

#ls datadirs/optimizer0/global/ -a | grep -E "^[0-9]{4,}" 
pkill -9 sdb_proxy
pkill -9 sdb_schedule 
pkill -9 sdb_lake 
pkill -9 sdb_account 
pkill -9 sdb_lockmgr

sleep 2

nohup sdb_proxy > log/proxy.log 2>&1 &
nohup sdb_schedule > log/schedule.log 2>&1 &
nohup sdb_lake > log/lake.log 2>&1 &
nohup sdb_account > log/account.log 2>&1 &
nohup sdb_lockmgr > log/lockmgr.log 2>&1 &

sleep 2

echo ""
echo "create account"
echo "-----------------------------"
sdb_stool createaccount -a gpadmin -p 123 -o test 
#sdb_stool createdb -o test -d template1 

echo ""
echo "create user"
echo "-----------------------------"
sdb_stool createuser -o test -u test -p 123 
sdb_stool createuser -o test -u gpadmin


echo ""
echo "clone database"
echo "-----------------------------"
sdb_stool clonedb -o test -d postgres -s sdb -b template1

# postgres --optimizer=1 -D datadirs/optimizer0 --port=40000 
#
rm datadirs/optimizer0 -rf
cp -r datadirs/initdb0 datadirs/optimizer0
rm -rf datadirs/optimizer0/base/1/*
cd datadirs/optimizer0/global/
ls | grep -E "^[0-9]{4,}" | xargs rm -r
cd ../../..

nohup postgres --optimizer=1 --dir=datadirs/optimizer0/ --port=8999 --database=postgres --dbid=2 --endpoint=127.0.0.1:9000 --s3user=minioadmin --s3passwd=minioadmin --isminio=true --bucket=sdb1 --region=us-east-1 > datadirs/optimizer0/optimizer0.log 2>&1 &

for i in {1..9}
do
rm datadirs/worker${i} -rf
cp -r datadirs/initdb0 datadirs/worker${i}
rm -rf datadirs/worker${i}/base/1/*
cd datadirs/worker${i}/global/
ls | grep -E "^[0-9]{4,}" | xargs rm -r
cd ../../..
nohup postgres --worker=1 --dir=datadirs/worker${i}/ --port=910${i} --database=postgres --dbid=2 --endpoint=127.0.0.1:9000 --s3user=minioadmin --s3passwd=minioadmin --isminio=true --bucket=sdb1 --region=us-east-1 > datadirs/worker${i}/worker${i}.log 2>&1 &
done

echo "start all"
