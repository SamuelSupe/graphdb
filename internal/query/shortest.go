package query

import (
	"fmt"

	"graphdb/internal/graph"
)

func executeShortestPath(g *graph.Graph, request Request, cursor cursorState, budget *budget) (Response, error) {
	if request.ID == "" || request.TargetID == "" {
		return Response{}, fmt.Errorf("%w: shortest_path requires id and target_id", ErrInvalid)
	}
	if err := validateDirection(request.Direction); err != nil {
		return Response{}, err
	}
	start, ok, err := materializeEntity(g, request.ID, request, budget)
	if err != nil {
		return Response{}, err
	}
	if !ok {
		if lazyExecution(g, budget) {
			return Response{}, ErrIndexUnavailable
		}
		return Response{}, fmt.Errorf("%w: entity %q not found", ErrInvalid, request.ID)
	}
	target, ok, err := materializeEntity(g, request.TargetID, request, budget)
	if err != nil {
		return Response{}, err
	}
	if !ok {
		if lazyExecution(g, budget) {
			return Response{}, ErrIndexUnavailable
		}
		return Response{}, fmt.Errorf("%w: entity %q not found", ErrInvalid, request.TargetID)
	}
	request.Depth = normalizedDepth(request.Depth)
	var path graph.Path
	var found bool
	if err := budget.measure("shortest-bfs", fmt.Sprintf("depth=%d", request.Depth), 0, func() (int, error) {
		var err error
		path, found, err = shortestPath(g, start, target, request, budget)
		if found {
			return len(path.Edges), err
		}
		return 0, err
	}); err != nil {
		return Response{}, err
	}
	results := []Result{}
	if found {
		results = pathResults([]graph.Path{path}, request.Project)
	}
	return buildResponse(g.Version, results, request, cursor, budget)
}

func shortestPath(g *graph.Graph, start graph.Entity, target graph.Entity, request Request, budget *budget) (graph.Path, bool, error) {
	depth := normalizedDepth(request.Depth)
	if !pathAllowsKind(start.Kind, request.Path) {
		return graph.Path{}, false, nil
	}
	startState := shortestState{id: start.ID}
	queue := []shortestState{startState}
	stepped := len(request.Path.Steps) > 0
	seen := map[string]struct{}{start.ID: {}}
	frontiers := map[shortestLevel][]shortestState{}
	active := map[int]bool{startState.serial: true}
	nextSerial := 1
	previous := map[shortestState]shortestPredecessor{}
	entities := map[shortestState]graph.Entity{startState: start}
	for head := 0; head < len(queue); head++ {
		if err := budget.check(); err != nil {
			return graph.Path{}, false, err
		}
		current := queue[head]
		if stepped && !active[current.serial] {
			continue
		}
		if current.depth >= depth {
			continue
		}
		neighbors, err := neighborsForBudget(g, current.id, request, budget)
		if err != nil {
			return graph.Path{}, false, err
		}
		for _, neighbor := range neighbors {
			budget.visited++
			if shortestPathContains(current, neighbor.Entity.ID, previous) || !shortestStepAllows(current.depth, neighbor, request.Path) {
				continue
			}
			next := shortestState{id: neighbor.Entity.ID, depth: current.depth + 1, serial: nextSerial}
			nextSerial++
			isTarget := neighbor.Entity.ID == target.ID
			if isTarget && !shortestTargetAllows(neighbor.Entity, next.depth, request.Path) {
				continue
			}
			previous[next] = shortestPredecessor{state: current, edge: neighbor.Edge}
			entities[next] = neighbor.Entity
			if isTarget {
				return rebuildShortestPath(startState, next, previous, entities), true, nil
			}
			if stepped {
				if !retainSteppedShortestState(next, current, previous, frontiers, active) {
					delete(previous, next)
					delete(entities, next)
					continue
				}
			} else {
				if _, ok := seen[next.id]; ok {
					delete(previous, next)
					delete(entities, next)
					continue
				}
				seen[next.id] = struct{}{}
			}
			queue = append(queue, next)
		}
	}
	return graph.Path{}, false, nil
}

type shortestState struct {
	id     string
	depth  int
	serial int
}

type shortestLevel struct {
	id    string
	depth int
}

type shortestPredecessor struct {
	state shortestState
	edge  graph.Edge
}

func shortestPathContains(state shortestState, id string, previous map[shortestState]shortestPredecessor) bool {
	for {
		if state.id == id {
			return true
		}
		item, ok := previous[state]
		if !ok {
			return false
		}
		state = item.state
	}
}

func retainSteppedShortestState(candidate shortestState, parent shortestState, previous map[shortestState]shortestPredecessor, frontiers map[shortestLevel][]shortestState, active map[int]bool) bool {
	level := shortestLevel{id: candidate.id, depth: candidate.depth}
	existing := frontiers[level]
	for _, state := range existing {
		if active[state.serial] && shortestStateSubsetOfCandidate(state, parent, candidate.id, previous) {
			return false
		}
	}
	kept := existing[:0]
	for _, state := range existing {
		if !active[state.serial] {
			continue
		}
		if shortestCandidateSubsetOfState(parent, candidate.id, state, previous) {
			active[state.serial] = false
			continue
		}
		kept = append(kept, state)
	}
	frontiers[level] = append(kept, candidate)
	active[candidate.serial] = true
	return true
}

func shortestStateSubsetOfCandidate(state shortestState, parent shortestState, candidateID string, previous map[shortestState]shortestPredecessor) bool {
	for {
		if !shortestCandidateContains(parent, candidateID, state.id, previous) {
			return false
		}
		item, ok := previous[state]
		if !ok {
			return true
		}
		state = item.state
	}
}

func shortestCandidateSubsetOfState(parent shortestState, candidateID string, state shortestState, previous map[shortestState]shortestPredecessor) bool {
	if !shortestPathContains(state, candidateID, previous) {
		return false
	}
	for {
		if !shortestPathContains(state, parent.id, previous) {
			return false
		}
		item, ok := previous[parent]
		if !ok {
			return true
		}
		parent = item.state
	}
}

func shortestCandidateContains(parent shortestState, candidateID string, id string, previous map[shortestState]shortestPredecessor) bool {
	return candidateID == id || shortestPathContains(parent, id, previous)
}

func shortestStepAllows(stepIndex int, neighbor graph.Neighbor, filter PathFilter) bool {
	if !pathAllowsKind(neighbor.Entity.Kind, filter) {
		return false
	}
	if len(filter.Steps) == 0 {
		return true
	}
	if stepIndex >= len(filter.Steps) {
		return false
	}
	step := filter.Steps[stepIndex]
	return relationAllowed(neighbor.Edge.Type, stringSet(step.RelationTypes)) &&
		edgeMatches(neighbor.Edge, step.EdgeWhere) &&
		edgeExprMatches(neighbor.Edge, step.EdgeWhereExpr) &&
		stringAllowed(neighbor.Entity.Kind, stringSet(step.NodeKinds)) &&
		entityMatches(neighbor.Entity, step.Where) &&
		entityExprMatches(neighbor.Entity, step.WhereExpr)
}

func shortestTargetAllows(target graph.Entity, depth int, filter PathFilter) bool {
	if len(filter.Steps) > 0 && depth != len(filter.Steps) {
		return false
	}
	if filter.EndKind != "" && target.Kind != filter.EndKind {
		return false
	}
	return entityMatches(target, filter.EndWhere) && entityExprMatches(target, filter.EndWhereExpr)
}

func rebuildShortestPath(start shortestState, target shortestState, previous map[shortestState]shortestPredecessor, entities map[shortestState]graph.Entity) graph.Path {
	path := graph.Path{}
	for current := target; ; {
		path.Entities = append(path.Entities, entities[current])
		if current == start {
			break
		}
		item := previous[current]
		path.Edges = append(path.Edges, item.edge)
		current = item.state
	}
	reverseEntities(path.Entities)
	reverseEdges(path.Edges)
	return path
}

func reverseEntities(items []graph.Entity) {
	for left, right := 0, len(items)-1; left < right; left, right = left+1, right-1 {
		items[left], items[right] = items[right], items[left]
	}
}

func reverseEdges(items []graph.Edge) {
	for left, right := 0, len(items)-1; left < right; left, right = left+1, right-1 {
		items[left], items[right] = items[right], items[left]
	}
}
