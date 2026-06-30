package graphdb

import "net/url"

func pathEscape(value string) string {
	return url.PathEscape(value)
}
