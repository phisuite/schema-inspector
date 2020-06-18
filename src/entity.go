package main

import (
	"context"
	"github.com/phiskills/neo4j-client.go"
	"github.com/phisuite/schema.go"
)

type entityServer struct {
	schema.UnimplementedEntityReadAPIServer
	store *store
}

func (e *entityServer) List(options *schema.Options, stream schema.EntityReadAPI_ListServer) error {
	name, version, status := e.store.ExtractIdentifiers(options)
	skip, limit := e.store.ExtractPagination(options)
	records, err := e.store.ListData(name, version, status, skip, limit)
	if err != nil {
		return err
	}
	var current *schema.Entity
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

func (e *entityServer) Get(_ context.Context, options *schema.Options) (*schema.Entity, error) {
	name, version, status := e.store.ExtractIdentifiers(options)
	records, err := e.store.ListData(name, version, status, 0, 0)
	if err != nil {
		return nil, err
	}
	var current *schema.Entity
	for _, record := range records {
		previous, next := e.extract(current, record)
		if previous != nil {
			break
		}
		current = next
	}
	return current, nil
}

func (e *entityServer) extract(current *schema.Entity, record neo4j.Records) (previous, next *schema.Entity) {
	entity :=  &schema.Entity{
		Name:    record["entity.name"].(string),
		Version: record["entity.version"].(string),
		Status:  e.store.ExtractStatus("entity.status", record),
	}
	field := &schema.Field{
		Name:     record["field.name"].(string),
		Type:     e.store.ExtractFieldType("field.type", record),
		Category: e.store.ExtractFieldCategory("has.category", record),
	}
	if current == nil {
		entity.Data = []*schema.Field{field}
		next = entity
		return
	}
	if current.Name != entity.Name || current.Version != entity.Version {
		previous = current
		current = entity
	}
	next = current
	next.Data = append(next.Data, field)
	return
}
