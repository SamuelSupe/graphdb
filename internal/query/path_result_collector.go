package query

import "github.com/SamuelSupe/graphdb/v2/internal/graph"

type pathResultCollector struct {
	request Request
	sorted  *boundedResults
	acc     *aggregateAccumulator
	groups  *groupAccumulator
	count   int
}

func newPathResultCollector(request Request) *pathResultCollector {
	keep := pathPageResultLimit(request)
	if request.Path.MaxPaths > 0 {
		keep = min(keep, request.Path.MaxPaths)
	}
	acc, groups := newQueryAggregators(request)
	return &pathResultCollector{
		request: request,
		sorted:  newBoundedResults(request.Sort, keep),
		acc:     acc,
		groups:  groups,
	}
}

func (collector *pathResultCollector) add(path graph.Path) error {
	result := pathResult(path, collector.request.Project)
	collector.count++
	if err := collector.acc.add(result); err != nil {
		return err
	}
	if err := collector.groups.add(result); err != nil {
		return err
	}
	collector.sorted.Add(result)
	return nil
}

func (collector *pathResultCollector) results() []Result {
	return collector.sorted.Sorted()
}

func (collector *pathResultCollector) aggregates() map[string]any {
	return collector.acc.results()
}

func (collector *pathResultCollector) aggregateGroups() []AggregateGroup {
	return collector.groups.results(
		collector.request.Having,
		collector.request.HavingExpr,
	)
}

func requiresCompletePathScan(request Request) bool {
	return len(request.Sort) > 0 ||
		len(request.Aggregate) > 0 ||
		len(request.GroupBy) > 0
}
