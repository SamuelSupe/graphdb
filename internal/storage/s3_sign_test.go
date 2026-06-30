package storage

import (
	"net/url"
	"testing"
)

func TestCanonicalQueryUsesAWSPercentEncoding(t *testing.T) {
	values := url.Values{}
	values.Set("continuation-token", "a+b=c/ d")
	values.Set("prefix", "objects/")
	got := canonicalQuery(values)
	want := "continuation-token=a%2Bb%3Dc%2F%20d&prefix=objects%2F"
	if got != want {
		t.Fatalf("canonical query = %q, want %q", got, want)
	}
}

func TestAWSEscapeLeavesOnlyUnreservedCharacters(t *testing.T) {
	got := awsPathEscape("AZaz09-_.~ +=/")
	want := "AZaz09-_.~%20%2B%3D%2F"
	if got != want {
		t.Fatalf("aws escape = %q, want %q", got, want)
	}
}
