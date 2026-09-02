package retrieval

import (
	"sort"
	"strings"
)

const DefaultRRFK = 60

type RankedList struct {
	Name       string
	Weight     float64
	Candidates []RankedCandidate
}

type ChannelContribution struct {
	Rank         int     `json:"rank"`
	RawScore     float64 `json:"raw_score"`
	Weight       float64 `json:"weight"`
	Contribution float64 `json:"contribution"`
}

type FusedCandidate struct {
	ID       string                         `json:"id"`
	Rank     int                            `json:"rank"`
	Score    float64                        `json:"score"`
	Channels map[string]ChannelContribution `json:"channels"`
}

func ReciprocalRankFusion(lists []RankedList, k int, limit int) ([]FusedCandidate, error) {
	if limit < 0 {
		return nil, invalidf("fusion limit must be >= 0")
	}
	if limit == 0 || len(lists) == 0 {
		return nil, nil
	}
	if k <= 0 {
		k = DefaultRRFK
	}
	fused := make(map[string]*FusedCandidate)
	channelNames := make(map[string]struct{}, len(lists))
	for _, list := range lists {
		name := strings.TrimSpace(list.Name)
		if name == "" {
			return nil, invalidf("fusion channel name is required")
		}
		if _, ok := channelNames[name]; ok {
			return nil, invalidf("duplicate fusion channel %q", name)
		}
		channelNames[name] = struct{}{}
		if list.Weight <= 0 {
			return nil, invalidf("fusion channel %q weight must be greater than zero", name)
		}
		seen := make(map[string]struct{}, len(list.Candidates))
		for i, candidate := range list.Candidates {
			if candidate.ID == "" {
				return nil, invalidf("fusion channel %q contains an empty id", name)
			}
			if _, ok := seen[candidate.ID]; ok {
				return nil, invalidf("fusion channel %q contains duplicate id %q", name, candidate.ID)
			}
			seen[candidate.ID] = struct{}{}
			rank := candidate.Rank
			if rank <= 0 {
				rank = i + 1
			}
			contribution := list.Weight / float64(k+rank)
			item := fused[candidate.ID]
			if item == nil {
				item = &FusedCandidate{
					ID:       candidate.ID,
					Channels: make(map[string]ChannelContribution),
				}
				fused[candidate.ID] = item
			}
			item.Score += contribution
			item.Channels[name] = ChannelContribution{
				Rank:         rank,
				RawScore:     candidate.Score,
				Weight:       list.Weight,
				Contribution: contribution,
			}
		}
	}
	result := make([]FusedCandidate, 0, len(fused))
	for _, candidate := range fused {
		result = append(result, *candidate)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Score != result[j].Score {
			return result[i].Score > result[j].Score
		}
		return result[i].ID < result[j].ID
	})
	if len(result) > limit {
		result = result[:limit]
	}
	for i := range result {
		result[i].Rank = i + 1
	}
	return result, nil
}
