package graphdb

import "context"

func (c *Client) ListRelationSchemas(ctx context.Context) (out RelationSchemaCatalog, err error) {
	err = c.doJSON(ctx, "GET", "/v1/relation-schemas", "", nil, nil, &out)
	return out, err
}

func (c *Client) PutRelationSchema(ctx context.Context, schema RelationSchema) (out RelationSchemaCatalog, err error) {
	err = c.doJSON(ctx, "PUT", "/v1/relation-schemas/"+pathEscape(schema.RelationType), "", nil, schema, &out)
	return out, err
}

func (c *Client) DeleteRelationSchema(ctx context.Context, relationType string) (out RelationSchemaCatalog, err error) {
	err = c.doJSON(ctx, "DELETE", "/v1/relation-schemas/"+pathEscape(relationType), "", nil, nil, &out)
	return out, err
}
