package storage

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

func (s *S3Store) authorization(method, canonicalURI, query, host, payloadHash, amzDate, date string) string {
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		query,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
	scope := date + "/" + s.region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	signature := hex.EncodeToString(hmacSHA256(s.signingKey(date), []byte(stringToSign)))
	return "AWS4-HMAC-SHA256 Credential=" + s.accessKey + "/" + scope +
		", SignedHeaders=" + signedHeaders +
		", Signature=" + signature
}

func (s *S3Store) signingKey(date string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+s.secretKey), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(s.region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func canonicalQuery(values url.Values) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0)
	for _, key := range keys {
		vals := append([]string(nil), values[key]...)
		sort.Strings(vals)
		for _, value := range vals {
			parts = append(parts, awsPathEscape(key)+"="+awsPathEscape(value))
		}
	}
	return strings.Join(parts, "&")
}

func awsPathEscape(value string) string {
	var builder strings.Builder
	for _, b := range []byte(value) {
		if awsUnreserved(b) {
			builder.WriteByte(b)
			continue
		}
		builder.WriteString(fmt.Sprintf("%%%02X", b))
	}
	return builder.String()
}

func awsUnreserved(b byte) bool {
	return (b >= 'A' && b <= 'Z') ||
		(b >= 'a' && b <= 'z') ||
		(b >= '0' && b <= '9') ||
		b == '-' || b == '_' || b == '.' || b == '~'
}
