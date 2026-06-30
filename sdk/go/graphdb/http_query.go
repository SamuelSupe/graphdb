package graphdb

import (
	"net/url"
	"strconv"
)

func queryValues(pairs ...string) url.Values {
	values := url.Values{}
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			values.Set(pairs[i], pairs[i+1])
		}
	}
	return values
}

func setInt(values url.Values, key string, value int) {
	if value > 0 {
		values.Set(key, strconv.Itoa(value))
	}
}

func setInt64(values url.Values, key string, value int64) {
	if value > 0 {
		values.Set(key, strconv.FormatInt(value, 10))
	}
}

func setBool(values url.Values, key string, value bool) {
	if value {
		values.Set(key, "true")
	}
}
