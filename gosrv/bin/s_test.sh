mkdir -p log
pkill -9 proxy
pkill -9 schedule 
pkill -9 lake 
pkill -9 consul
pkill -9 account 

#cmd="configure tenant_mode=disabled; writemode on; begin; clearrange \"\" \\xFF;  commit;"
#fdbcli --exec "$cmd" || true

consul agent -dev > log/consul.log 2>&1 &
sleep 1

#make -C ../Makefile
nohup ./proxy > log/proxy.log 2>&1 &
nohup ./schedule > log/schedule.log 2>&1 &
nohup ./lake > log/lake.log 2>&1 &
nohup ./account > log/account.log 2>&1 &

netstat -lnpt |  grep proxy
netstat -lnpt |  grep schedule 
netstat -lnpt |  grep lake 
netstat -lnpt |  grep account 


