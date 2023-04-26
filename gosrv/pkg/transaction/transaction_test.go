package transaction

import (
	"fmt"
	"testing"
)

func setup() {
	fmt.Println("Before all tests")
}

func teardown() {
	fmt.Println("After all tests")
}

func TestStart(t *testing.T) {
	//transaction := NewTranscation()
	//transaction.Start()
}
