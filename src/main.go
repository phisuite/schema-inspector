package main

import (
	"github.com/phiskills/grpc-api.go"
	"github.com/phisuite/schema.go"
)

func main() {
	api := grpc.New()
	schema.RegisterEventReadAPIServer(api.Server, &eventServer{})
	schema.RegisterEntityReadAPIServer(api.Server, &entityServer{})
	schema.RegisterProcessReadAPIServer(api.Server, &processServer{})
	api.Start()
}
