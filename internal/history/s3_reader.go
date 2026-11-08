package history

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3Options override the standard AWS lookup (env, profile, SSO, IMDS).
type (
	S3Options struct {
		Region    string // "" → AWS_REGION / profile; R2 takes "auto"
		Endpoint  string // R2, Ceph or another S3-compatible service
		PathStyle bool   // most self-hosted endpoints need it
	}

	s3Reader struct {
		bucket    string
		region    string
		regionFix string // how to point at the right region, for wrap's message
		client    *s3.Client
	}
)

var _ objectReader = (*s3Reader)(nil)

func NewS3Store(ctx context.Context, u ObjectURL, opts S3Options, streamFor func(SnapshotKey) string) (SnapshotStore, error) {
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("not an s3:// URL: %s", u)
	}
	r, err := newS3Reader(ctx, u.Bucket, opts)
	if err != nil {
		return nil, err
	}
	return newObjectStore(r, u, streamFor), nil
}

func newS3Reader(ctx context.Context, bucket string, opts S3Options) (*s3Reader, error) {
	// a stalled body must not hang the pull; BuildableClient keeps AWS_CA_BUNDLE and IMDS timeouts
	httpClient := awshttp.NewBuildableClient().WithTimeout(5 * time.Minute).WithTransportOptions(func(tr *http.Transport) {
		tr.ResponseHeaderTimeout = 30 * time.Second
	})
	load := []func(*config.LoadOptions) error{config.WithHTTPClient(httpClient)}
	if opts.Region != "" {
		load = append(load, config.WithRegion(opts.Region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, load...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("no AWS region for s3://%s: set AWS_REGION or the remote's region", bucket)
	}
	// credentials load lazily: fail here with a next step, not on the first GET
	if _, err := cfg.Credentials.Retrieve(ctx); err != nil {
		return nil, fmt.Errorf("no usable AWS credentials for s3://%s (set AWS_PROFILE, run `aws sso login` or `aws configure`): %w", bucket, err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		}
		o.UsePathStyle = opts.PathStyle
		// objects without a checksum (R2, multipart) would log a WARN per get
		o.DisableLogOutputChecksumValidationSkipped = true
	})
	regionFix := "set AWS_REGION=%s or the remote's region"
	if opts.Region != "" {
		regionFix = "set the remote's region to %s"
	}
	return &s3Reader{bucket: bucket, region: cfg.Region, regionFix: regionFix, client: client}, nil
}

func (r *s3Reader) list(ctx context.Context, prefix string) ([]string, error) {
	in := &s3.ListObjectsV2Input{Bucket: aws.String(r.bucket)}
	if prefix != "" {
		in.Prefix = aws.String(prefix)
	}
	var names []string
	for p := s3.NewListObjectsV2Paginator(r.client, in); p.HasMorePages(); {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, r.wrap(prefix, err, false)
		}
		for _, o := range page.Contents {
			names = append(names, aws.ToString(o.Key))
		}
	}
	return names, nil
}

func (r *s3Reader) get(ctx context.Context, name string) ([]byte, error) {
	out, err := r.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(r.bucket), Key: aws.String(name)})
	if err != nil {
		return nil, r.wrap(name, err, true)
	}
	defer out.Body.Close()
	raw, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, r.wrap(name, err, true)
	}
	return raw, nil
}

// Only NoSuchKey on a get is "expired": a 403 for a missing key is a permission
// error, since S3 answers that way when the caller lacks s3:ListBucket.
func (r *s3Reader) wrap(obj string, err error, isGet bool) error {
	where := "s3://" + r.bucket + "/" + obj
	var (
		noKey    *types.NoSuchKey
		noBucket *types.NoSuchBucket
		resp     *awshttp.ResponseError
		api      smithy.APIError
	)
	// a 301 or 400 AuthorizationHeaderMalformed carries the bucket's region
	if errors.As(err, &resp) {
		if region := resp.Response.Header.Get("X-Amz-Bucket-Region"); region != "" && region != r.region {
			return fmt.Errorf("bucket s3://%s is in region %s, not %s: %s", r.bucket, region, r.region, fmt.Sprintf(r.regionFix, region))
		}
	}
	switch {
	case isGet && errors.As(err, &noKey):
		return fmt.Errorf("%s: %w", where, errObjectNotFound)
	case errors.As(err, &noBucket):
		return fmt.Errorf("bucket s3://%s not found: %w", r.bucket, err)
	case errors.As(err, &api) && api.ErrorCode() == "AccessDenied":
		return fmt.Errorf("no read access to %s (needs s3:ListBucket and s3:GetObject): %w", where, err)
	}
	return fmt.Errorf("%s: %w", where, err)
}
