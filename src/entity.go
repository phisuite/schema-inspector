package main

import (
	"context"
	"fmt"
	"github.com/phisuite/schema.go"
	"log"
)

type entityServer struct {
	schema.UnimplementedEntityReadAPIServer
}

func (e entityServer) List(_ *schema.Options, stream schema.EntityReadAPI_ListServer) error {
	for i := 1; i < 5; i++ {
		version := fmt.Sprintf("0.0.%d", i)
		entity := &schema.Entity{Name: "dummy", Version: version}
		log.Printf("Stream: %v", entity)
		if err := stream.Send(entity); err != nil {
			return err
		}
	}
	return nil
}

func (e entityServer) Get(context.Context, *schema.Options) (*schema.Entity, error) {
	entity := &schema.Entity{Name: "dummy", Version: "0.0.1"}
	log.Printf("Send: %v", entity)
	return entity, nil
}
