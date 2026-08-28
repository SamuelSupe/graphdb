package bootstrap

import (
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/config"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestNewObjectStoreSelectsNativeSingleWriterProfiles(t *testing.T) {
	cases := []struct {
		provider string
		endpoint string
		region   string
	}{
		{provider: storage.ObjectProviderAliyunOSS, endpoint: "https://oss-cn-hangzhou.aliyuncs.com", region: "cn-hangzhou"},
		{provider: storage.ObjectProviderHuaweiOBS, endpoint: "https://obs.cn-north-4.myhuaweicloud.com", region: "cn-north-4"},
		{provider: storage.ObjectProviderTencentCOS, endpoint: "https://graphdb-1250000000.cos.ap-guangzhou.myqcloud.com", region: "ap-guangzhou"},
	}
	for _, testCase := range cases {
		t.Run(testCase.provider, func(t *testing.T) {
			objects, err := newObjectStore(config.Config{
				StoreKind:         "s3",
				S3Provider:        testCase.provider,
				S3Versioning:      storage.BucketVersioningDisabled,
				WriterTopology:    storage.WriterTopologySingle,
				S3Endpoint:        testCase.endpoint,
				S3Bucket:          "graphdb-1250000000",
				S3Region:          testCase.region,
				S3AccessKeyID:     "access-key",
				S3SecretAccessKey: "secret-key",
			})
			if err != nil {
				t.Fatalf("newObjectStore: %v", err)
			}
			if _, ok := objects.(*storage.SingleWriterObjectStore); !ok {
				t.Fatalf("store type = %T, want *storage.SingleWriterObjectStore", objects)
			}
		})
	}
}
