.PHONY: test test_lake test_lock clean

test: test_lake

test_lake:
	go test --v --parallel 15 -run="TestLake*"

test_lock:
	go test --v --parallel 15 -run="TestLock*"
