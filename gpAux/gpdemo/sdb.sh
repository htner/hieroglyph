pkill -9 postgres
rm /tmp/output.log
source /usr/local/sdb/greenplum_path.sh
source ./gpdemo-env.sh

nohup postgres --optimizer=1 -D datadirs/qddir/demoDataDir-1/ --port=40000 >> /tmp/output.log 2>&1 &

nohup postgres --worker=1 -D datadirs/qddir/demoDataDir-2/ --port=40001 >> /tmp/output.log 2>&1 &
nohup postgres --worker=1 -D datadirs/dbfast1/demoDataDir0/ --port=40002 >> /tmp/output.log 2>&1 &
nohup postgres --worker=1 -D datadirs/dbfast2/demoDataDir1/ --port=40003 >> /tmp/output.log 2>&1 &
nohup postgres --worker=1 -D datadirs/dbfast3/demoDataDir2/ --port=40004 >> /tmp/output.log 2>&1 &
