package main

import (
	"github.com/phiskills/grpc-api.go"
	"github.com/phiskills/neo4j-client.go"
	"github.com/phisuite/schema.go"
	"os"
	"strconv"
)

func main() {
	port, err := strconv.Atoi(os.Getenv("STORE_POST"))
	if err != nil {
		port = 7687
	}
	client := &neo4j.Client{
		Host:     os.Getenv("STORE_HOST"),
		Port:     port,
		Username: os.Getenv("STORE_USER"),
		Password: os.Getenv("STORE_PASS"),
	}
	api := grpc.New("Schema Inspector")
	schema.RegisterEventReadAPIServer(api.Server(), &eventServer{store: &store{
		client: client,
		kind:   "Event",
	}})
	schema.RegisterEntityReadAPIServer(api.Server(), &entityServer{store: &store{
		client: client,
		kind:   "Entity",
	}})
	schema.RegisterProcessReadAPIServer(api.Server(), &processServer{store: &store{
		client: client,
		kind:   "Process",
	}})
	api.Start()
}
