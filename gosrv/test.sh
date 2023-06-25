mkdir -p log
pkill -9 proxy
pkill -9 schedule 
pkill -9 lake 
pkill -9 consul

consul agent -dev > log/consul.log 2>&1 &
sleep 1

nohup ./bin/proxy > log/proxy.log 2>&1 &
nohup ./bin/schedule > log/schedule.log 2>&1 &
nohup ./bin/lake > log/lake.log 2>&1 &
