#!/bin/bash -x
rm mr-map-intermediate*
rm mr-out-0
go build -race -buildmode=plugin ../mrapps/wc.go
go run -race mrcoordinator.go pg-*.txt &
go run -race mrworker.go wc.so
