package main

import (
	"context"
	"fmt"
	"github.com/phisuite/schema.go"
	"log"
)

type processServer struct {
	schema.UnimplementedProcessReadAPIServer
}

func (p processServer) List(_ *schema.Options, stream schema.ProcessReadAPI_ListServer) error {
	for i := 1; i < 5; i++ {
		version := fmt.Sprintf("0.0.%d", i)
		process := &schema.Process{Name: "dummy", Version: version}
		log.Printf("Stream: %v", process)
		if err := stream.Send(process); err != nil {
			return err
		}
	}
	return nil
}

func (p processServer) Get(context.Context, *schema.Options) (*schema.Process, error) {
	process := &schema.Process{Name: "dummy", Version: "0.0.1"}
	log.Printf("Send: %v", process)
	return process, nil
}
