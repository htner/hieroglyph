mkdir -p log
pkill -9 proxy
pkill -9 schedule 
pkill -9 lake 
pkill -9 consul
pkill -9 account

cmd="configure tenant_mode=disabled; writemode on; begin; clearrange \"\" \\xFF;  commit;"
fdbcli --exec "$cmd" || true
