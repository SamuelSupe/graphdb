package graph

type uniqueStringCollector struct {
	values *[]string
	seen   map[string]struct{}
}

func newUniqueStringCollector(values *[]string) *uniqueStringCollector {
	collector := &uniqueStringCollector{values: values, seen: make(map[string]struct{}, len(*values))}
	for _, value := range *values {
		if value != "" {
			collector.seen[value] = struct{}{}
		}
	}
	return collector
}

func (c *uniqueStringCollector) add(values ...string) {
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := c.seen[value]; ok {
			continue
		}
		c.seen[value] = struct{}{}
		*c.values = append(*c.values, value)
	}
}
