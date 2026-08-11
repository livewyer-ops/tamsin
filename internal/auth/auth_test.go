package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/oauth2"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

// isolatedTransport gives each test its own connection pool. These tests run in
// parallel, and http.DefaultTransport is process-wide: one test's server
// teardown closing idle connections can break another's in-flight request,
// which shows up as a rare "CloseIdleConnections called" failure rather than
// anything to do with the code under test.
func isolatedTransport() *http.Transport {
	return http.DefaultTransport.(*http.Transport).Clone()
}

func TestHeaderAndURLAuthentication(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		config Config
		assert func(*testing.T, *http.Request)
	}{
		{
			name: "basic", config: Config{Mode: ModeBasic, Username: "user", Password: "password"},
			assert: func(t *testing.T, request *http.Request) {
				username, password, ok := request.BasicAuth()
				if !ok || username != "user" || password != "password" {
					t.Fatalf("unexpected basic credentials: %q %q %v", username, password, ok)
				}
			},
		},
		{
			name: "bearer", config: Config{Mode: ModeBearer, BearerToken: "secret"},
			assert: func(t *testing.T, request *http.Request) {
				if actual := request.Header.Get("Authorization"); actual != "Bearer secret" {
					t.Fatalf("Authorization = %q", actual)
				}
			},
		},
		{
			name: "url-token", config: Config{Mode: ModeURLToken, URLToken: "secret"},
			assert: func(t *testing.T, request *http.Request) {
				if actual := request.URL.Query().Get("access_token"); actual != "secret" {
					t.Fatalf("access_token = %q", actual)
				}
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			testCase.config.Endpoint = "https://tams.example/service"
			base := roundTripFunc(func(request *http.Request) (*http.Response, error) {
				testCase.assert(t, request)
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
			})
			transport, mode, err := NewRoundTripper(context.Background(), testCase.config, base, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if mode != testCase.config.Mode {
				t.Fatalf("mode = %q", mode)
			}
			request, _ := http.NewRequest(http.MethodGet, "https://tams.example/service?existing=value", nil)
			request.Header.Set("X-Test", "preserved")
			if _, err := transport.RoundTrip(request); err != nil {
				t.Fatal(err)
			}
			if request.Header.Get("Authorization") != "" || request.URL.Query().Get("access_token") != "" {
				t.Fatal("authentication transport mutated the caller's request")
			}
		})
	}
}

func TestOAuthClientCredentials(t *testing.T) {
	t.Parallel()
	tokenServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if err := request.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if request.Form.Get("grant_type") != "client_credentials" {
			t.Fatalf("grant_type = %q", request.Form.Get("grant_type"))
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{"access_token": "oauth-token", "token_type": "Bearer", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer oauth-token" {
			t.Fatalf("Authorization = %q", request.Header.Get("Authorization"))
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer apiServer.Close()

	transport, mode, err := NewRoundTripper(context.Background(), Config{
		Mode: ModeOAuthClient, Endpoint: apiServer.URL,
		TokenURL: tokenServer.URL, ClientID: "client", ClientSecret: "secret",
		AllowInsecureLoopback: true,
	}, isolatedTransport(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if mode != ModeOAuthClient {
		t.Fatalf("mode = %q", mode)
	}
	response, err := (&http.Client{Transport: transport}).Get(apiServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
}
func TestOAuthTokenErrorsDoNotLeakCredentialsOrResponseBodies(t *testing.T) {
	t.Parallel()
	tokenServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "client secret was secret-value", http.StatusUnauthorized)
	}))
	defer tokenServer.Close()

	_, _, err := NewRoundTripper(context.Background(), Config{
		Mode: ModeOAuthClient, Endpoint: "https://tams.example.test",
		TokenURL: tokenServer.URL, ClientID: "client", ClientSecret: "secret-value",
		AllowInsecureLoopback: true,
	}, isolatedTransport(), io.Discard)
	if err == nil {
		t.Fatal("token acquisition unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), "secret-value") || strings.Contains(err.Error(), "client secret") {
		t.Fatalf("OAuth error leaked sensitive response content: %q", err)
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("OAuth error omitted response status: %q", err)
	}
}

func TestOAuthTokenErrorIgnoresUntrustedReasonPhrase(t *testing.T) {
	t.Parallel()
	err := oauthTokenError("obtain token", &oauth2.RetrieveError{Response: &http.Response{
		StatusCode: http.StatusUnauthorized,
		Status:     "401 client-secret was rejected",
	}})
	if strings.Contains(err.Error(), "client-secret") || !strings.Contains(err.Error(), "401 Unauthorized") {
		t.Fatalf("unsafe OAuth status error: %v", err)
	}
}
func TestOAuthTokenRequestDoesNotFollowRedirect(t *testing.T) {
	t.Parallel()
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		redirected.Add(1)
		writer.WriteHeader(http.StatusUnauthorized)
	}))
	defer target.Close()
	tokenServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer tokenServer.Close()

	_, _, err := NewRoundTripper(context.Background(), Config{
		Mode: ModeOAuthClient, Endpoint: "https://tams.example.test",
		TokenURL: tokenServer.URL, ClientID: "client", ClientSecret: "secret-value",
		AllowInsecureLoopback: true,
	}, isolatedTransport(), io.Discard)
	if err == nil {
		t.Fatal("redirected token acquisition unexpectedly succeeded")
	}
	if redirected.Load() != 0 {
		t.Fatal("OAuth client secret body was forwarded through a redirect")
	}
}

func TestOAuthAuthorizationCode(t *testing.T) {
	t.Parallel()
	tokenServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if err := request.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if request.Form.Get("grant_type") != "authorization_code" ||
			request.Form.Get("code") != "one-time-code" ||
			request.Form.Get("code_verifier") != "0123456789012345678901234567890123456789012" {
			t.Fatalf("unexpected authorization exchange: %v", request.Form)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"access_token":"code-token","token_type":"Bearer","expires_in":3600}`)
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer code-token" {
			t.Fatalf("Authorization = %q", request.Header.Get("Authorization"))
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer apiServer.Close()

	transport, mode, err := NewRoundTripper(context.Background(), Config{
		Mode: ModeOAuthCode, Endpoint: apiServer.URL, TokenURL: tokenServer.URL,
		ClientID: "client", RedirectURL: "https://app.example/callback", OAuthCode: "one-time-code",
		PKCEVerifier:          "0123456789012345678901234567890123456789012",
		AllowInsecureLoopback: true,
	}, isolatedTransport(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if mode != ModeOAuthCode {
		t.Fatalf("mode = %q", mode)
	}
	response, err := (&http.Client{Transport: transport}).Get(apiServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
}

func TestResolveModeRequiresUnambiguousOAuthGrant(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name    string
		config  Config
		want    Mode
		wantErr bool
	}{
		{
			name: "pre-obtained authorization code",
			config: Config{
				OAuthCode: "one-time-code", TokenURL: "https://identity.example/token",
				ClientID: "client", RedirectURL: "https://application.example/callback",
			},
			want: ModeOAuthCode,
		},
		{
			name: "authorization endpoint signals code grant",
			config: Config{
				AuthURL: "https://identity.example/authorize", TokenURL: "https://identity.example/token",
				ClientID: "client", RedirectURL: "http://localhost:53682/callback",
			},
			want: ModeOAuthCode,
		},
		{
			name: "client secret signals client grant",
			config: Config{
				ClientSecret: "secret", TokenURL: "https://identity.example/token", ClientID: "client",
			},
			want: ModeOAuthClient,
		},
		{
			name: "shared settings are ambiguous",
			config: Config{
				TokenURL: "https://identity.example/token", ClientID: "client",
			},
			wantErr: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			mode, err := testCase.config.ResolveMode()
			if (err != nil) != testCase.wantErr {
				t.Fatalf("ResolveMode() error = %v, want error %v", err, testCase.wantErr)
			}
			if mode != testCase.want {
				t.Fatalf("ResolveMode() = %q, want %q", mode, testCase.want)
			}
		})
	}
}

func TestPreobtainedAuthorizationCodeDoesNotRequireAuthorizationEndpoint(t *testing.T) {
	t.Parallel()
	tokenServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if err := request.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if request.Form.Get("code") != "one-time-code" {
			t.Fatalf("code = %q", request.Form.Get("code"))
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"access_token":"code-token","token_type":"Bearer"}`)
	}))
	defer tokenServer.Close()

	transport, mode, err := NewRoundTripper(context.Background(), Config{
		Endpoint: "https://tams.example.test", TokenURL: tokenServer.URL,
		ClientID: "client", RedirectURL: "https://application.example/callback",
		OAuthCode: "one-time-code", AllowInsecureLoopback: true,
	}, isolatedTransport(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if mode != ModeOAuthCode || transport == nil {
		t.Fatalf("mode = %q, transport nil = %v", mode, transport == nil)
	}
}

func TestExtractAndRedactURLToken(t *testing.T) {
	t.Parallel()
	endpoint, token, err := ExtractURLToken("https://user:password@example.test/tams?access_token=secret&other=value")
	if err != nil {
		t.Fatal(err)
	}
	if token != "secret" || strings.Contains(endpoint, "access_token") {
		t.Fatalf("endpoint = %q, token = %q", endpoint, token)
	}
	redacted := RedactURL("https://user:password@example.test/object?X-Amz-Signature=secret&access_token=token")
	if strings.Contains(redacted, "password") || strings.Contains(redacted, "=secret") || strings.Contains(redacted, "=token") {
		t.Fatalf("RedactURL leaked a credential: %s", redacted)
	}
}

func TestConfiguredSecretRedactionIncludesWireRepresentations(t *testing.T) {
	t.Parallel()
	config := Config{
		Username: "basic-user", Password: "basic secret/with-symbols",
		URLToken: "query secret/with-symbols",
	}
	values := strings.Join(config.RedactionValues(), "\n")
	basicPayload := base64.StdEncoding.EncodeToString([]byte(config.Username + ":" + config.Password))
	for _, expected := range []string{
		config.Password,
		basicPayload,
		url.QueryEscape(config.Password),
		config.URLToken,
		url.QueryEscape(config.URLToken),
	} {
		if !strings.Contains(values, expected) {
			t.Errorf("RedactionValues() omitted %q", expected)
		}
	}
}

func TestCredentialEndpointPolicy(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name      string
		url       string
		allowHTTP bool
		wantOK    bool
	}{
		{name: "HTTPS remote", url: "HTTPS://media.example.test/tams", wantOK: true},
		{name: "HTTP localhost opted in", url: "http://localhost:8080/tams", allowHTTP: true, wantOK: true},
		{name: "HTTP uppercase localhost opted in", url: "HTTP://LOCALHOST:8080/tams", allowHTTP: true, wantOK: true},
		{name: "HTTP IPv4 loopback range opted in", url: "http://127.42.0.9:8080/tams", allowHTTP: true, wantOK: true},
		{name: "HTTP IPv6 loopback opted in", url: "http://[0:0:0:0:0:0:0:1]:8080/tams", allowHTTP: true, wantOK: true},
		{name: "HTTP mapped IPv4 loopback opted in", url: "http://[::ffff:127.0.0.1]:8080/tams", allowHTTP: true, wantOK: true},
		{name: "HTTP localhost requires opt in", url: "http://localhost:8080/tams"},
		{name: "HTTP remote remains forbidden with opt in", url: "http://192.0.2.10/tams", allowHTTP: true},
		{name: "localhost subdomain is remote", url: "http://localhost.example.test/tams", allowHTTP: true},
		{name: "localhost suffix is remote", url: "http://127.0.0.1.example.test/tams", allowHTTP: true},
		{name: "trailing dot is not the exact localhost name", url: "http://localhost./tams", allowHTTP: true},
		{name: "legacy integer IPv4 is not accepted", url: "http://2130706433/tams", allowHTTP: true},
		{name: "abbreviated IPv4 is not accepted", url: "http://127.1/tams", allowHTTP: true},
		{name: "octal-looking IPv4 is not accepted", url: "http://0177.0.0.1/tams", allowHTTP: true},
		{name: "unspecified IPv6 is not loopback", url: "http://[::]/tams", allowHTTP: true},
		{name: "userinfo is forbidden", url: "https://user:secret-value@example.test/tams"},
		{name: "unsupported scheme", url: "ftp://localhost/tams", allowHTTP: true},
		{name: "relative URL", url: "/tams", allowHTTP: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			err := validateCredentialURL("TAMS endpoint", testCase.url, testCase.allowHTTP)
			if (err == nil) != testCase.wantOK {
				t.Fatalf("validateCredentialURL() error = %v, want success %v", err, testCase.wantOK)
			}
			if err != nil && strings.Contains(err.Error(), "secret-value") {
				t.Fatalf("transport-policy error leaked URL credentials: %v", err)
			}
		})
	}
}

func TestAuthenticatedModesRequireConfiguredTAMSEndpoint(t *testing.T) {
	t.Parallel()
	testCases := []Config{
		{Mode: ModeBasic, Username: "user", Password: "password"},
		{Mode: ModeBearer, BearerToken: "token"},
		{Mode: ModeURLToken, URLToken: "token"},
		{Mode: ModeOAuthClient, TokenURL: "https://identity.example/token", ClientID: "client", ClientSecret: "secret"},
		{
			Mode: ModeOAuthCode, TokenURL: "https://identity.example/token", ClientID: "client",
			RedirectURL: "https://application.example/callback", OAuthCode: "code",
		},
	}
	for _, config := range testCases {
		config := config
		t.Run(string(config.Mode), func(t *testing.T) {
			t.Parallel()
			if err := config.Validate(config.Mode); err == nil || !strings.Contains(err.Error(), "TAMS endpoint") {
				t.Fatalf("Validate() error = %v, want missing TAMS endpoint", err)
			}
		})
	}
}

func TestOAuthRedirectURLPolicy(t *testing.T) {
	t.Parallel()
	base := Config{
		Mode: ModeOAuthCode, Endpoint: "https://tams.example.test",
		TokenURL: "https://identity.example/token", ClientID: "client", OAuthCode: "code",
	}
	testCases := []struct {
		name     string
		redirect string
		wantOK   bool
	}{
		{name: "HTTPS callback", redirect: "https://application.example/callback", wantOK: true},
		{name: "uppercase HTTPS callback", redirect: "HTTPS://APPLICATION.EXAMPLE/callback", wantOK: true},
		{name: "HTTP localhost callback", redirect: "http://localhost:53682/callback", wantOK: true},
		{name: "HTTP IPv4 loopback callback", redirect: "http://127.0.0.1:53682/callback", wantOK: true},
		{name: "HTTP remote callback", redirect: "http://application.example/callback"},
		{name: "userinfo callback", redirect: "https://user:secret@application.example/callback"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			config := base
			config.RedirectURL = testCase.redirect
			err := config.Validate(config.Mode)
			if (err == nil) != testCase.wantOK {
				t.Fatalf("Validate() error = %v, want success %v", err, testCase.wantOK)
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatalf("redirect validation leaked userinfo: %v", err)
			}
		})
	}
}

func TestAuthenticatedTAMSModesRejectRemoteHTTPBeforeTransport(t *testing.T) {
	t.Parallel()
	testCases := []Config{
		{Mode: ModeBasic, Username: "user", Password: "basic-secret"},
		{Mode: ModeBearer, BearerToken: "bearer-secret"},
		{Mode: ModeURLToken, URLToken: "query-secret"},
	}
	for _, config := range testCases {
		config := config
		t.Run(string(config.Mode), func(t *testing.T) {
			t.Parallel()
			config.Endpoint = "http://service.example.test/tams?configured=endpoint-secret"
			config.AllowInsecureLoopback = true
			var calls atomic.Int32
			base := roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, nil
			})
			_, _, err := NewRoundTripper(context.Background(), config, base, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "HTTPS") {
				t.Fatalf("NewRoundTripper() error = %v, want HTTPS rejection", err)
			}
			if calls.Load() != 0 {
				t.Fatalf("base transport called %d times", calls.Load())
			}
			for _, secret := range []string{"basic-secret", "bearer-secret", "query-secret", "endpoint-secret"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("transport-policy error leaked %q: %v", secret, err)
				}
			}
		})
	}
}

func TestCredentialTransportsRejectRuntimeHTTPBeforeInjection(t *testing.T) {
	t.Parallel()
	testCases := []Config{
		{Mode: ModeBasic, Username: "user", Password: "basic-secret"},
		{Mode: ModeBearer, BearerToken: "bearer-secret"},
		{Mode: ModeURLToken, URLToken: "query-secret"},
	}
	for _, config := range testCases {
		config := config
		t.Run(string(config.Mode), func(t *testing.T) {
			t.Parallel()
			config.Endpoint = "https://service.example.test/tams"
			var calls atomic.Int32
			base := roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, nil
			})
			transport, _, err := NewRoundTripper(context.Background(), config, base, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			request, requestErr := http.NewRequest(http.MethodGet,
				"http://service.example.test/tams?access_token=redirect-secret", nil)
			if requestErr != nil {
				t.Fatal(requestErr)
			}
			_, err = transport.RoundTrip(request)
			if err == nil || !strings.Contains(err.Error(), "HTTPS") {
				t.Fatalf("RoundTrip() error = %v, want HTTPS rejection", err)
			}
			if calls.Load() != 0 {
				t.Fatalf("base transport called %d times", calls.Load())
			}
			if strings.Contains(err.Error(), "redirect-secret") {
				t.Fatalf("runtime transport-policy error leaked URL token: %v", err)
			}
			if request.Header.Get("Authorization") != "" || request.URL.Query().Get("access_token") != "redirect-secret" {
				t.Fatal("rejected transport mutated the caller's request")
			}
		})
	}
}

func TestCredentialTransportBlocksHTTPSDowngradeRedirect(t *testing.T) {
	t.Parallel()
	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetRequests.Add(1)
	}))
	defer target.Close()

	redirector := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer bearer-secret" {
			t.Errorf("initial Authorization = %q", request.Header.Get("Authorization"))
		}
		http.Redirect(writer, request, target.URL+"/captured", http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	transport, _, err := NewRoundTripper(context.Background(), Config{
		Mode: ModeBearer, Endpoint: redirector.URL, BearerToken: "bearer-secret",
	}, redirector.Client().Transport, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Transport: transport}).Get(redirector.URL)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("redirect error = %v, want HTTPS rejection", err)
	}
	if strings.Contains(err.Error(), "bearer-secret") {
		t.Fatalf("redirect error leaked bearer token: %v", err)
	}
	if targetRequests.Load() != 0 {
		t.Fatalf("plaintext redirect target received %d request(s)", targetRequests.Load())
	}
}

func TestCredentialTransportBlocksCrossOriginHTTPSRedirect(t *testing.T) {
	t.Parallel()
	var targetRequests atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		targetRequests.Add(1)
	}))
	defer target.Close()

	redirector := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer bearer-secret" {
			t.Errorf("initial Authorization = %q", request.Header.Get("Authorization"))
		}
		http.Redirect(writer, request, target.URL+"/captured", http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	transport, _, err := NewRoundTripper(context.Background(), Config{
		Mode: ModeBearer, Endpoint: redirector.URL, BearerToken: "bearer-secret",
	}, redirector.Client().Transport, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Transport: transport}).Get(redirector.URL)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "configured TAMS origin") {
		t.Fatalf("redirect error = %v, want cross-origin rejection", err)
	}
	if strings.Contains(err.Error(), "bearer-secret") {
		t.Fatalf("redirect error leaked bearer token: %v", err)
	}
	if targetRequests.Load() != 0 {
		t.Fatalf("cross-origin redirect target received %d request(s)", targetRequests.Load())
	}
}

func TestOAuthEndpointsRejectRemoteHTTPBeforeNetwork(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name   string
		config Config
	}{
		{
			name: "client credential TAMS endpoint",
			config: Config{
				Mode: ModeOAuthClient, Endpoint: "http://tams.example.test?access_token=endpoint-secret",
				TokenURL: "https://identity.example.test/token", ClientID: "client", ClientSecret: "client-secret",
			},
		},
		{
			name: "client credential token endpoint",
			config: Config{
				Mode: ModeOAuthClient, Endpoint: "https://tams.example.test",
				TokenURL: "http://identity.example.test/token?client_secret=url-secret", ClientID: "client", ClientSecret: "client-secret",
			},
		},
		{
			name: "authorization code token endpoint",
			config: Config{
				Mode: ModeOAuthCode, Endpoint: "https://tams.example.test",
				TokenURL: "http://identity.example.test/token?code=code-secret", AuthURL: "https://identity.example.test/authorize",
				ClientID: "client", RedirectURL: "https://application.example.test/callback", OAuthCode: "one-time-secret",
			},
		},
		{
			name: "authorization endpoint",
			config: Config{
				Mode: ModeOAuthCode, Endpoint: "https://tams.example.test",
				TokenURL: "https://identity.example.test/token", AuthURL: "http://identity.example.test/authorize?state=url-secret",
				ClientID: "client", RedirectURL: "https://application.example.test/callback", OAuthCode: "one-time-secret",
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			base := roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, nil
			})
			_, _, err := NewRoundTripper(context.Background(), testCase.config, base, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "HTTPS") {
				t.Fatalf("NewRoundTripper() error = %v, want HTTPS rejection", err)
			}
			if calls.Load() != 0 {
				t.Fatalf("base transport called %d times", calls.Load())
			}
			for _, secret := range []string{"endpoint-secret", "url-secret", "client-secret", "code-secret", "one-time-secret"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("transport-policy error leaked %q: %v", secret, err)
				}
			}
		})
	}
}

func TestUnauthenticatedHTTPRemainsAvailable(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	base := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	})
	transport, mode, err := NewRoundTripper(context.Background(), Config{
		Mode: ModeNone, Endpoint: "http://service.example.test/tams",
	}, base, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if mode != ModeNone {
		t.Fatalf("mode = %q", mode)
	}
	request, _ := http.NewRequest(http.MethodGet, "http://service.example.test/tams", nil)
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if calls.Load() != 1 {
		t.Fatalf("base transport called %d times", calls.Load())
	}
}

func TestInteractiveCallbackRemainsHTTPLoopbackOnly(t *testing.T) {
	t.Parallel()
	accepted := []string{
		"http://localhost:53682/callback",
		"HTTP://LOCALHOST:53682/callback",
		"http://127.0.0.1:53682/callback",
		"http://127.12.3.4:53682/callback",
		"http://[::1]:53682/callback",
		"http://[0:0:0:0:0:0:0:1]:53682/callback",
	}
	for _, rawURL := range accepted {
		if _, err := parseInteractiveRedirect(rawURL); err != nil {
			t.Errorf("parseInteractiveRedirect(%q) error = %v", rawURL, err)
		}
	}

	rejected := []string{
		"https://localhost:53682/callback",
		"http://localhost.example.test:53682/callback",
		"http://127.0.0.1.example.test:53682/callback",
		"http://192.0.2.10:53682/callback",
		"http://[::]:53682/callback",
		"http://user:callback-secret@localhost:53682/callback",
	}
	for _, rawURL := range rejected {
		_, err := parseInteractiveRedirect(rawURL)
		if err == nil {
			t.Errorf("parseInteractiveRedirect(%q) unexpectedly succeeded", rawURL)
			continue
		}
		if strings.Contains(err.Error(), "callback-secret") {
			t.Errorf("callback validation leaked userinfo: %v", err)
		}
	}
}
