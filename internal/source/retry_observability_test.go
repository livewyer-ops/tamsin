package source

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/livewyer-ops/tamsin/internal/observability"
)

func TestS3ObservabilityPreservesConfiguredAttemptLimit(t *testing.T) {
	// These are process-wide AWS configuration inputs, so this test must not be
	// parallel. Parallel package tests remain paused until sequential tests end.
	for _, testCase := range []struct {
		name      string
		configure func(*testing.T)
	}{
		{
			name: "environment",
			configure: func(t *testing.T) {
				t.Setenv("AWS_MAX_ATTEMPTS", "2")
				t.Setenv("AWS_RETRY_MODE", "standard")
			},
		},
		{
			name: "adaptive environment",
			configure: func(t *testing.T) {
				t.Setenv("AWS_MAX_ATTEMPTS", "2")
				t.Setenv("AWS_RETRY_MODE", "adaptive")
			},
		},
		{
			name: "shared config",
			configure: func(t *testing.T) {
				directory := t.TempDir()
				filename := filepath.Join(directory, "config")
				if err := os.WriteFile(filename, []byte("[default]\nregion=us-east-1\nmax_attempts=2\nretry_mode=standard\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("AWS_CONFIG_FILE", filename)
				t.Setenv("AWS_MAX_ATTEMPTS", "")
				t.Setenv("AWS_RETRY_MODE", "")
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Retry precedence is the subject of this test. Isolate unrelated
			// machine-wide AWS endpoint policy: a runner with FIPS enabled in its
			// default profile rejects the explicit loopback S3 endpoint before the
			// fake server sees an attempt, while the shared-config case happens to
			// replace that profile and masks the leak.
			directory := t.TempDir()
			neutralConfig := filepath.Join(directory, "neutral-config")
			if err := os.WriteFile(neutralConfig, []byte("[default]\nregion=us-east-1\nuse_fips_endpoint=false\nuse_dualstack_endpoint=false\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			neutralCredentials := filepath.Join(directory, "neutral-credentials")
			if err := os.WriteFile(neutralCredentials, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("AWS_CONFIG_FILE", neutralConfig)
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", neutralCredentials)
			t.Setenv("AWS_USE_FIPS_ENDPOINT", "false")
			t.Setenv("AWS_USE_DUALSTACK_ENDPOINT", "false")
			t.Setenv("AWS_ENDPOINT_URL", "")
			t.Setenv("AWS_ENDPOINT_URL_S3", "")
			t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "true")
			t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
			t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
			t.Setenv("AWS_NEW_RETRIES_2026", "false")
			t.Setenv("AWS_PROFILE", "default")
			testCase.configure(t)

			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				attempts.Add(1)
				http.Error(writer, "provider message with signed material", http.StatusServiceUnavailable)
			}))
			defer server.Close()
			run := observability.New("cd62f89d-eeca-4c40-8a3f-57a47702e996", nil)
			resolver := New(Config{
				S3: S3Config{
					Region: "us-east-1", Endpoint: server.URL, UsePathStyle: true, HTTPClient: server.Client(),
				},
				MetadataTimeout: 10 * time.Second, Observability: run,
			})
			_, err := resolver.Resolve(context.Background(), []string{"s3://bucket/object"})
			if err == nil {
				t.Fatal("permanent S3 failure unexpectedly succeeded")
			}
			if got := attempts.Load(); got != 2 {
				t.Fatalf("S3 attempts = %d, want configured total of 2", got)
			}
			if got := run.Snapshot().Retries; got != 1 {
				t.Fatalf("observed retries = %d, want the one additional S3 attempt", got)
			}
		})
	}
}

func TestObservedRetryerDelegatesPolicyDelayAndQuota(t *testing.T) {
	t.Setenv("AWS_NEW_RETRIES_2026", "false")
	policy := &recordingRetryer{delay: 37 * time.Millisecond, max: 7}
	run := observability.New("0bf4f7b8-dd8a-4d31-aa9a-eb1f9908561d", nil)
	retryer := observedRetryer{Retryer: policy, run: run}
	cause := errors.New("opaque AWS provider failure")

	if !retryer.IsErrorRetryable(cause) || retryer.MaxAttempts() != 7 {
		t.Fatal("wrapper changed retry policy")
	}
	delay, err := retryer.RetryDelay(1, cause)
	if err != nil || delay != policy.delay {
		t.Fatalf("RetryDelay() = %v, %v", delay, err)
	}
	release, err := retryer.GetRetryToken(context.Background(), cause)
	if err != nil {
		t.Fatal(err)
	}
	if err := release(nil); err != nil {
		t.Fatal(err)
	}
	initial := retryer.GetInitialToken()
	if err := initial(nil); err != nil {
		t.Fatal(err)
	}
	attempt, err := retryer.GetAttemptToken(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := attempt(nil); err != nil {
		t.Fatal(err)
	}
	if policy.delayCalls.Load() != 1 || policy.retryTokens.Load() != 1 ||
		policy.initialTokens.Load() != 1 || policy.attemptTokens.Load() != 1 {
		t.Fatalf("delegate calls: delay=%d retry-token=%d initial-token=%d attempt-token=%d",
			policy.delayCalls.Load(), policy.retryTokens.Load(), policy.initialTokens.Load(), policy.attemptTokens.Load())
	}
	if got := run.Snapshot().Retries; got != 1 {
		t.Fatalf("observed retries = %d, want 1", got)
	}
}

func TestAWS2026ObservedBackoffMatchesRetryAfterClamp(t *testing.T) {
	t.Parallel()
	response := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     http.Header{"X-Amz-Retry-After": []string{"2500"}},
			Body:       http.NoBody,
		}},
		Err: errors.New("opaque provider detail"),
	}
	if got := aws2026RetryAfter(time.Second, response); got != 2500*time.Millisecond {
		t.Fatalf("observed Retry-After = %v, want 2.5s", got)
	}
	response.Response.Header.Set("X-Amz-Retry-After", "9000")
	if got := aws2026RetryAfter(time.Second, response); got != 6*time.Second {
		t.Fatalf("observed clamped Retry-After = %v, want 6s", got)
	}
}

type recordingRetryer struct {
	delay         time.Duration
	max           int
	delayCalls    atomic.Int32
	retryTokens   atomic.Int32
	initialTokens atomic.Int32
	attemptTokens atomic.Int32
}

func (*recordingRetryer) IsErrorRetryable(error) bool { return true }
func (r *recordingRetryer) MaxAttempts() int          { return r.max }
func (r *recordingRetryer) RetryDelay(int, error) (time.Duration, error) {
	r.delayCalls.Add(1)
	return r.delay, nil
}
func (r *recordingRetryer) GetRetryToken(context.Context, error) (func(error) error, error) {
	r.retryTokens.Add(1)
	return func(error) error { return nil }, nil
}
func (r *recordingRetryer) GetInitialToken() func(error) error {
	r.initialTokens.Add(1)
	return func(error) error { return nil }
}
func (r *recordingRetryer) GetAttemptToken(context.Context) (func(error) error, error) {
	r.attemptTokens.Add(1)
	return func(error) error { return nil }, nil
}
