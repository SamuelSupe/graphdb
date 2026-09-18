package backupstore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

var ErrNotFound = errors.New("object backup not found")

type Config struct {
	Bucket, Prefix, Endpoint, Region           string
	AccessKeyID, SecretAccessKey, SessionToken string
	PathStyle                                  bool
}

func (c Config) Validate() error {
	if c.Bucket == "" {
		if c.Prefix != "" || c.Endpoint != "" || c.Region != "" || c.AccessKeyID != "" || c.SecretAccessKey != "" || c.SessionToken != "" || c.PathStyle {
			return fmt.Errorf("GRAPHDB_BACKUP_S3_BUCKET is required when configuring object backups")
		}
		return nil
	}
	if !regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`).MatchString(c.Bucket) || strings.Contains(c.Bucket, "..") {
		return fmt.Errorf("invalid GRAPHDB_BACKUP_S3_BUCKET")
	}
	if c.Prefix != strings.Trim(c.Prefix, "/") || strings.ContainsAny(c.Prefix, "\\\x00\r\n") {
		return fmt.Errorf("invalid GRAPHDB_BACKUP_S3_PREFIX")
	}
	if c.Prefix != "" {
		for _, part := range strings.Split(c.Prefix, "/") {
			if part == "" || part == "." || part == ".." {
				return fmt.Errorf("invalid GRAPHDB_BACKUP_S3_PREFIX")
			}
		}
	}
	if (c.AccessKeyID == "") != (c.SecretAccessKey == "") || (c.SessionToken != "" && c.AccessKeyID == "") {
		return fmt.Errorf("object backup static credentials require both access key ID and secret access key")
	}
	if c.Endpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return fmt.Errorf("GRAPHDB_BACKUP_S3_ENDPOINT must be an HTTP(S) origin")
		}
	}
	return nil
}

// Repository only transfers immutable backups; it is not an online graph store.
type Repository struct {
	client         *s3.Client
	bucket, prefix string
}

func New(ctx context.Context, c Config) (*Repository, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c.Bucket == "" {
		return nil, nil
	}
	region := c.Region
	if region == "" {
		region = "us-east-1"
	}
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
		awsconfig.WithHTTPClient(&http.Client{Timeout: 5 * time.Minute}),
	}
	if c.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, c.SessionToken)))
	}
	config, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(config, func(o *s3.Options) {
		o.UsePathStyle = c.PathStyle
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
		// The manifest supplies an end-to-end SHA-256, including on S3-compatible
		// services that do not implement the SDK's optional checksum extensions.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &Repository{client: client, bucket: c.Bucket, prefix: c.Prefix}, nil
}

func isAPIError(err error, codes ...string) bool {
	var api smithy.APIError
	if !errors.As(err, &api) {
		return false
	}
	for _, code := range codes {
		if api.ErrorCode() == code {
			return true
		}
	}
	return false
}
