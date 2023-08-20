
## Create service
```bash
GOPROXY=https://goproxy.cn/,direct go install github.com/zeromicro/go-zero/tools/goctl@latest
make
```

## How to Start 
```bash
cd bin
./transaction -f transactionservice.yaml
```

## Need etcd install before

