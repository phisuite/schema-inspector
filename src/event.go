package main

import (
	"context"
	"github.com/phiskills/neo4j-client.go"
	"github.com/phisuite/schema.go"
)

type eventServer struct {
	schema.UnimplementedEventReadAPIServer
	store *store
}

func (e *eventServer) List(options *schema.Options, stream schema.EventReadAPI_ListServer) error {
	name, version, status := e.store.ExtractIdentifiers(options)
	skip, limit := e.store.ExtractPagination(options)
	records, err := e.store.ListData(name, version, status, skip, limit)
	if err != nil {
		return err
	}
	var current *schema.Event
	for _, record := range records {
		previous, next := e.extract(current, record)
		current = next
		if previous == nil {
			continue
		}
		if err := stream.Send(previous); err != nil {
			return err
		}
	}
	if current == nil {
		return nil
	}
	if err := stream.Send(current); err != nil {
		return err
	}
	return nil
}

func (e *eventServer) Get(_ context.Context, options *schema.Options) (*schema.Event, error) {
	name, version, status := e.store.ExtractIdentifiers(options)
	records, err := e.store.ListData(name, version, status, 0, 0)
	if err != nil {
		return nil, err
	}
	var current *schema.Event
	for _, record := range records {
		previous, next := e.extract(current, record)
		if previous != nil {
			break
		}
		current = next
	}
	return current, nil
}

func (e *eventServer) extract(current *schema.Event, record neo4j.Records) (previous, next *schema.Event) {
	event :=  &schema.Event{
		Name:    record["event.name"].(string),
		Version: record["event.version"].(string),
		Status:  e.store.ExtractStatus("event.status", record),
	}
	field := &schema.Field{
		Name:     record["field.name"].(string),
		Type:     e.store.ExtractFieldType("field.type", record),
		Category: e.store.ExtractFieldCategory("has.category", record),
	}
	if current == nil {
		event.Payload = []*schema.Field{field}
		next = event
		return
	}
	if current.Name != event.Name || current.Version != event.Version {
		previous = current
		current = event
	}
	next = current
	next.Payload = append(next.Payload, field)
	return
}
