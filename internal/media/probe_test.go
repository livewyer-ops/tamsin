package media

import (
	"context"
	"slices"
	"strings"
	"testing"
)

func TestValidateToolVersion(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name    string
		report  string
		wantErr bool
	}{
		{name: "minimum", report: "ffprobe version 5.1.0-0+deb12u1"},
		{name: "current", report: "ffmpeg version 8.0.1 Copyright"},
		{name: "release prefix", report: "ffmpeg version n7.1"},
		{name: "older minor", report: "ffprobe version 5.0.3", wantErr: true},
		{name: "older major", report: "ffmpeg version 4.4", wantErr: true},
		{name: "unparseable", report: "custom media tool", wantErr: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateToolVersion(testCase.report, "FFmpeg")
			if (err != nil) != testCase.wantErr {
				t.Fatalf("ValidateToolVersion(%q) error = %v, wantErr %v", testCase.report, err, testCase.wantErr)
			}
		})
	}
}

func TestMediaToolEnvironmentUsesAnExplicitAllowlist(t *testing.T) {
	t.Parallel()
	environ := mediaToolEnvironment([]string{
		"PATH=/usr/bin", "LANG=en_GB.UTF-8", "LC_NUMERIC=C", "TMPDIR=/tmp/media",
		"LD_LIBRARY_PATH=/opt/ffmpeg/lib", "CUDA_VISIBLE_DEVICES=0", "SSL_CERT_FILE=/etc/ssl/cert.pem",
		"TAMSIN_AUTH_TOKEN=tams-secret", "AWS_SECRET_ACCESS_KEY=aws-secret",
		"HTTPS_PROXY=https://proxy-user:proxy-secret@example.test", "FFREPORT=file=leak.log", "CUSTOM_SECRET=secret",
	})
	for _, expected := range []string{
		"PATH=/usr/bin", "LANG=en_GB.UTF-8", "LC_NUMERIC=C", "TMPDIR=/tmp/media",
		"LD_LIBRARY_PATH=/opt/ffmpeg/lib", "CUDA_VISIBLE_DEVICES=0", "SSL_CERT_FILE=/etc/ssl/cert.pem",
	} {
		if !slices.Contains(environ, expected) {
			t.Errorf("allowed environment entry %q was removed: %v", expected, environ)
		}
	}
	joined := strings.Join(environ, "\n")
	for _, secret := range []string{"tams-secret", "aws-secret", "proxy-secret", "leak.log", "CUSTOM_SECRET"} {
		if strings.Contains(joined, secret) {
			t.Errorf("media tool environment contains forbidden value %q: %v", secret, environ)
		}
	}
}

func TestToolCommandAppliesTheMediaEnvironment(t *testing.T) {
	t.Setenv("TAMSIN_AUTH_TOKEN", "must-not-reach-child")
	t.Setenv("LANG", "en_GB.UTF-8")
	command := toolCommand(context.Background(), "ffprobe", "-version")
	joined := strings.Join(command.Env, "\n")
	if strings.Contains(joined, "must-not-reach-child") {
		t.Fatal("toolCommand inherited a TAMSin credential")
	}
	if !strings.Contains(joined, "LANG=en_GB.UTF-8") {
		t.Fatal("toolCommand removed an allowed locale setting")
	}
}
