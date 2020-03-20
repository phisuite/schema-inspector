package main

import (
	"github.com/phiskills/grpc-api.go"
	"github.com/phisuite/schema.go"
)

func main() {
	api := grpc.New()
	schema.RegisterEventAPIServer(api.Server, &eventServer{})
	schema.RegisterEntityAPIServer(api.Server, &entityServer{})
	schema.RegisterProcessAPIServer(api.Server, &processServer{})
	api.Start()
}
