package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

const (
	indexPackPrefix          = "pack_"
	indexPackTargetRows      = 2048
	indexPackMaxLogicalItems = 64
)

type indexPackItem struct {
	ID    string
	Group string
	Rows  int
}

type indexPackGroup struct {
	ID    string
	Items []indexPackItem
}

func planIndexPacks(items []indexPackItem) []indexPackGroup {
	if len(items) == 0 {
		return nil
	}
	items = append([]indexPackItem(nil), items...)
	sort.Slice(items, func(i, j int) bool {
		if items[i].Group == items[j].Group {
			return items[i].ID < items[j].ID
		}
		return items[i].Group < items[j].Group
	})
	groups := make([]indexPackGroup, 0, len(items))
	current := make([]indexPackItem, 0)
	currentGroup := ""
	currentRows := 0
	flush := func() {
		if len(current) == 0 {
			return
		}
		groups = append(groups, indexPackGroup{ID: indexPackID(current), Items: current})
		current = nil
		currentGroup = ""
		currentRows = 0
	}
	for _, item := range items {
		if item.Rows >= indexPackTargetRows {
			flush()
			groups = append(groups, indexPackGroup{ID: item.ID, Items: []indexPackItem{item}})
			continue
		}
		if len(current) > 0 && (item.Group != currentGroup || currentRows+item.Rows > indexPackTargetRows || len(current) >= indexPackMaxLogicalItems) {
			flush()
		}
		current = append(current, item)
		currentGroup = item.Group
		currentRows += item.Rows
	}
	flush()
	return groups
}

func indexPackID(items []indexPackItem) string {
	if len(items) == 1 {
		return items[0].ID
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	sort.Strings(ids)
	sum := sha256.Sum256([]byte(strings.Join(ids, "\x00")))
	return indexPackPrefix + ids[0] + "_" + hex.EncodeToString(sum[:6])
}

func indexPackMap(groups []indexPackGroup) map[string]string {
	out := map[string]string{}
	for _, group := range groups {
		for _, item := range group.Items {
			out[item.Group+"\x00"+item.ID] = group.ID
		}
	}
	return out
}

func isIndexPackID(value string) bool {
	return strings.HasPrefix(value, indexPackPrefix)
}
