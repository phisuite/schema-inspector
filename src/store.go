package main

import (
	"fmt"
	"github.com/phiskills/neo4j-client.go"
	"github.com/phisuite/schema.go"
	"strings"
)

type store struct {
	client        *neo4j.Client
	kind          string
	defaultStatus string
}

func (s *store) ExtractIdentifiers(options *schema.Options) (name, version, status string) {
	name = options.Name
	version = options.Version
	if options.Status != 0 {
		status = options.Status.String()
	}
	return
}

func (s *store) ExtractPagination(options *schema.Options) (skip, limit int) {
	skip = int(options.Skip)
	limit = int(options.Limit)
	return
}

func (s *store) ExtractStatus(key string, record neo4j.Records) schema.Status {
	value := schema.Status_value[record[key].(string)]
	return schema.Status(value)
}

func (s *store) ExtractFieldType(key string, record neo4j.Records) schema.Field_Type {
	value := schema.Field_Type_value[record[key].(string)]
	return schema.Field_Type(value)
}

func (s *store) ExtractFieldCategory(key string, record neo4j.Records) schema.Field_Category {
	value := schema.Field_Category_value[record[key].(string)]
	return schema.Field_Category(value)
}

func (s *store) ListData(name, version, status string, skip, limit int) ([]neo4j.Records, error) {
	return s.client.Read(func(j neo4j.Job) (result neo4j.Result, err error) {
		ref, properties := s.reference(name, version, status)
		field, has, props := s.hasField()
		properties = append(properties, props...)
		order := ref.Properties("name", "version")
		path := &neo4j.Path{
			Origin:       ref,
			Relationship: has,
			Destination:  field,
		}
		query := s.client.NewRequest()
		query = query.Match(path).Return(properties...)
		query = s.buildPaginationQuery(query, order, skip, limit)
		return j.Execute(query)
	})
}

func (s *store) ListProcesses(name, version, status string, skip, limit int) ([]neo4j.Records, error) {
	return s.client.Read(func(j neo4j.Job) (result neo4j.Result, err error) {
		ref, properties := s.reference(name, version, status)
		query := s.client.NewRequest()
		query = query.Match(ref)
		order := ref.Properties("name", "version")
		ids := []string{ref.Id}
		for _, category := range []processCategory{processInput, processOutput, processError} {
			eventPath, entityPath, props := s.buildProcessData(ref.Id, category)
			query = query.With(ids...)
			query = query.Match(eventPath)
			query = query.Optional().Match(entityPath)
			properties = append(properties, props...)
			ids = append(ids, eventPath.Origin.Id, entityPath.Origin.Id)
		}
		query = query.Return(properties...)
		query = s.buildPaginationQuery(query, order, skip, limit)
		return j.Execute(query)
	})
}

func (s *store) reference(name, version, status string) (ref *neo4j.Node, properties []neo4j.Property) {
	ref = &neo4j.Node{
		Id:     strings.ToLower(s.kind),
		Labels: []string{s.kind},
		Props:  neo4j.Records{},
	}
	if name != "" {
		ref.Props["name"] = name
	}
	if version != "" {
		ref.Props["version"] = version
	}
	if status != "" {
		ref.Props["status"] = status
	}
	properties = ref.Properties("name", "version", "status")
	return
}

func (s *store) hasField() (field *neo4j.Node, has *neo4j.Relationship, properties []neo4j.Property) {
	has = &neo4j.Relationship{
		Id:   "has",
		Type: "Has",
	}
	field = &neo4j.Node{
		Id:     "field",
		Labels: []string{"Field"},
	}
	properties = append(
		has.Properties("category"),
		field.Properties("name", "type")...
	)
	return
}

func (s *store) buildProcessData(processId string, category processCategory) (eventData, entityData *neo4j.Path, properties []neo4j.Property) {
	ref := &neo4j.Node{Id: processId}
	rel := &neo4j.Relationship{Type: string(category)}
	event := &neo4j.Node{
		Id:     fmt.Sprintf("event%s", category),
		Labels: []string{"Event"},
	}
	entity := &neo4j.Node{
		Id:     fmt.Sprintf("entity%s", category),
		Labels: []string{"Entity"},
	}
	eventData = &neo4j.Path{
		Origin:       event,
		Relationship: rel,
		Destination:  ref,
	}
	entityData = &neo4j.Path{
		Origin:       entity,
		Relationship: rel,
		Destination:  ref,
	}
	properties = append(
		event.Properties("name", "version", "status"),
		entity.Properties("name", "version", "status")...
	)
	return
}

func (s *store) buildPaginationQuery(query neo4j.Query, order []neo4j.Property, skip, limit int) neo4j.Query {
	query = query.OrderBy(order...)
	if skip != 0 {
		query = query.Skip(skip)
	}
	if limit != 0 {
		query = query.Limit(limit)
	}
	return query
}
