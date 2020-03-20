package main

import (
	"context"
	"fmt"
	"github.com/phisuite/schema.go"
	"log"
)

type eventServer struct {
	schema.UnimplementedEventAPIServer
}

func (e eventServer) List(_ *schema.Options, stream schema.EventAPI_ListServer) error {
	for i := 1; i < 5; i++ {
		version := fmt.Sprintf("0.0.%d", i)
		event := &schema.Event{Name:"dummy", Version:version}
		log.Printf("Stream: %v", event)
		if err := stream.Send(event); err != nil {
			return err
		}
	}
	return nil
}

func (e eventServer) Get(context.Context, *schema.Options) (*schema.Event, error) {
	event := &schema.Event{Name:"dummy", Version:"0.0.1"}
	log.Printf("Send: %v", event)
	return event, nil
}
