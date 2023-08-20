mkdir -p log
pkill -9 proxy
pkill -9 schedule 
pkill -9 lake 
pkill -9 consul
pkill -9 account

cmd="configure tenant_mode=disabled; writemode on; begin; clearrange \"\" \\xFF;  commit;"
fdbcli --exec "$cmd" || true

echo ""
echo "create account"
echo "-----------------------------"
./stool createaccount -a sdb -p 123 -o sdb 

echo ""
echo "create db"
echo "-----------------------------"
./stool createdb -o sdb -d template1 

echo ""
echo "create user"
echo "-----------------------------"
./stool createuser -o sdb -u sdb -p 123 

echo ""
echo "create account"
echo "-----------------------------"
./stool createaccount -a test -p 123 -o test

echo ""
echo "create user"
echo "-----------------------------"
./stool createuser -o test -u test -p 123 
