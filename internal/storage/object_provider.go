package storage

import "strings"

const (
	ObjectProviderGenericS3  = "generic-s3"
	ObjectProviderAliyunOSS  = "aliyun-oss"
	ObjectProviderHuaweiOBS  = "huawei-obs"
	ObjectProviderTencentCOS = "tencent-cos"

	WriterTopologyCAS    = "cas"
	WriterTopologySingle = "single"

	BucketVersioningDisabled = "disabled"
)

func NormalizeObjectProvider(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ObjectProviderGenericS3
	}
	return value
}

func IsKnownObjectProvider(value string) bool {
	switch NormalizeObjectProvider(value) {
	case ObjectProviderGenericS3, ObjectProviderAliyunOSS, ObjectProviderHuaweiOBS, ObjectProviderTencentCOS:
		return true
	default:
		return false
	}
}

func IsNativeObjectProvider(value string) bool {
	switch NormalizeObjectProvider(value) {
	case ObjectProviderAliyunOSS, ObjectProviderHuaweiOBS, ObjectProviderTencentCOS:
		return true
	default:
		return false
	}
}
