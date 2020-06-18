package main

import (
	"context"
	"fmt"
	"github.com/phiskills/neo4j-client.go"
	"github.com/phisuite/schema.go"
)

type processServer struct {
	schema.UnimplementedProcessReadAPIServer
	store *store
}

type processCategory string
const (
	processInput  processCategory = "Input"
	processOutput processCategory = "Output"
	processError  processCategory = "Error"
)

func (p *processServer) List(options *schema.Options, stream schema.ProcessReadAPI_ListServer) error {
	name, version, status := p.store.ExtractIdentifiers(options)
	skip, limit := p.store.ExtractPagination(options)
	records, err := p.store.ListProcesses(name, version, status, skip, limit)
	if err != nil {
		return err
	}
	for _, record := range records {
		current := p.extract(record)
		if err := stream.Send(current); err != nil {
			return err
		}
	}
	return nil
}

func (p *processServer) Get(_ context.Context, options *schema.Options) (*schema.Process, error) {
	name, version, status := p.store.ExtractIdentifiers(options)
	records, err := p.store.ListProcesses(name, version, status, 0, 0)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		return p.extract(record), nil
	}
	return nil, nil
}

func (p *processServer) extract(record neo4j.Records) *schema.Process {
	return &schema.Process{
		Name:       record["process.name"].(string),
		Version:    record["process.version"].(string),
		Status:     p.store.ExtractStatus("process.status", record),
		Definition: &schema.Process_Definition{
			Input:  &schema.Process_Data{
				Event:  p.extractEvent(processInput, record),
				Entity: p.extractEntity(processInput, record),
			},
			Output: &schema.Process_Data{
				Event:  p.extractEvent(processOutput, record),
				Entity: p.extractEntity(processOutput, record),
			},
			Error:  &schema.Process_Data{
				Event:  p.extractEvent(processError, record),
				Entity: p.extractEntity(processError, record),
			},
		},
	}
}

func (p processServer) extractEvent(category processCategory, record neo4j.Records) *schema.Event {
	alias := fmt.Sprintf("event%s", category)
	return &schema.Event{
		Name: record[alias+".name"].(string),
		Version: record[alias+".version"].(string),
		Status:  p.store.ExtractStatus(alias+".status", record),
	}
}

func (p processServer) extractEntity(category processCategory, record neo4j.Records) *schema.Entity {
	alias := fmt.Sprintf("entity%s", category)
	if _, ok := record[alias+".name"]; !ok {
		return nil
	}
	return &schema.Entity{
		Name: record[alias+".name"].(string),
		Version: record[alias+".version"].(string),
		Status:  p.store.ExtractStatus(alias+".status", record),
	}
}
