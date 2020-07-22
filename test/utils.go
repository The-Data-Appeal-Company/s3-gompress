package test

import (
	"io/ioutil"
	"testing"
)

func ReadFileOrError(t *testing.T, fileName string) []byte {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
