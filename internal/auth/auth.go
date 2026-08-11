package auth

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// Mode identifies one of the authentication mechanisms supported by TAMS 8.1.
type Mode string

const (
	ModeAuto        Mode = "auto"
	ModeNone        Mode = "none"
	ModeBasic       Mode = "basic"
	ModeBearer      Mode = "bearer"
	ModeURLToken    Mode = "url-token"
	ModeOAuthClient Mode = "oauth-client"
	ModeOAuthCode   Mode = "oauth-code"
)

// Config contains credentials and OAuth discovery-independent endpoints. TAMS
// deliberately does not prescribe OAuth endpoints, so callers must supply them.
type Config struct {
	Mode Mode
	// Endpoint is the TAMS API endpoint that the returned transport will
	// authenticate. Supplying it lets NewRoundTripper reject an insecure target
	// before an OAuth grant or any other network work begins.
	Endpoint     string
	Username     string
	Password     string
	BearerToken  string
	URLToken     string
	TokenURL     string
	AuthURL      string
	ClientID     string
	ClientSecret string
	RedirectURL  string
	Scopes       []string
	OAuthCode    string
	PKCEVerifier string
	Timeout      time.Duration
	// AllowInsecureLoopback permits credential-bearing HTTP only when the URL
	// names localhost or a literal loopback address. It exists solely for an
	// explicitly opted-in local development service; it never permits remote
	// plaintext authentication.
	AllowInsecureLoopback bool
}

// ResolveMode selects a single mechanism using a documented, deterministic
// precedence. Explicit Mode always wins.
func (c Config) ResolveMode() (Mode, error) {
	if c.Mode != "" && c.Mode != ModeAuto {
		return c.Mode, nil
	}

	codeIntent := c.OAuthCode != "" || c.PKCEVerifier != "" || c.AuthURL != ""
	clientIntent := c.ClientSecret != ""
	sharedOAuth := c.ClientID != "" || c.TokenURL != "" || len(c.Scopes) > 0
	switch {
	case c.URLToken != "":
		return ModeURLToken, nil
	case c.BearerToken != "":
		return ModeBearer, nil
	case codeIntent:
		return ModeOAuthCode, nil
	case clientIntent:
		return ModeOAuthClient, nil
	case sharedOAuth:
		return "", errors.New("automatic authentication cannot infer an OAuth grant from shared OAuth settings; configure an authorization URL/code or client secret, or set the mode explicitly")
	case c.Username != "" || c.Password != "":
		return ModeBasic, nil
	default:
		return ModeNone, nil
	}
}

// Validate rejects incomplete credentials and unsafe credential destinations
// before any network request.
func (c Config) Validate(mode Mode) error {
	require := func(values ...string) bool {
		for _, value := range values {
			if value == "" {
				return false
			}
		}
		return true
	}

	switch mode {
	case ModeNone:
		return nil
	case ModeBasic:
		if !require(c.Username, c.Password) {
			return errors.New("basic authentication requires username and password")
		}
	case ModeBearer:
		if !require(c.BearerToken) {
			return errors.New("bearer authentication requires a token")
		}
	case ModeURLToken:
		if !require(c.URLToken) {
			return errors.New("URL-token authentication requires an access token")
		}
	case ModeOAuthClient:
		if !require(c.TokenURL, c.ClientID, c.ClientSecret) {
			return errors.New("OAuth client credentials require token URL, client ID, and client secret")
		}
	case ModeOAuthCode:
		if !require(c.TokenURL, c.ClientID, c.RedirectURL) {
			return errors.New("OAuth authorization code requires token URL, client ID, and redirect URL")
		}
		if c.OAuthCode == "" && c.AuthURL == "" {
			return errors.New("interactive OAuth authorization code requires an authorization URL")
		}
		if c.OAuthCode == "" && c.PKCEVerifier != "" {
			return errors.New("an OAuth PKCE verifier requires a pre-obtained authorization code")
		}
	default:
		return fmt.Errorf("unsupported authentication mode %q", mode)
	}
	return c.validateCredentialEndpoints(mode)
}

// RedactionValues returns configured secret representations that a peer might
// echo in an error response. This includes the wire forms that differ from the
// configured value, notably HTTP Basic's base64 payload and URL encoding.
// OAuth access tokens are acquired dynamically, so callers must still suppress
// untrusted TAMS response bodies while using an OAuth mode.
func (c Config) RedactionValues() []string {
	values := []string{
		c.Password,
		c.BearerToken,
		c.URLToken,
		c.ClientSecret,
		c.OAuthCode,
		c.PKCEVerifier,
	}
	if c.Username != "" && c.Password != "" {
		values = append(values, base64.StdEncoding.EncodeToString([]byte(c.Username+":"+c.Password)))
	}
	for _, value := range append([]string(nil), values...) {
		if escaped := url.QueryEscape(value); value != "" && escaped != value {
			values = append(values, escaped)
		}
	}
	return values
}

// NewRoundTripper constructs the transport for one TAMS session.
func NewRoundTripper(ctx context.Context, cfg Config, base http.RoundTripper, prompt io.Writer) (http.RoundTripper, Mode, error) {
	if base == nil {
		base = http.DefaultTransport
	}
	if prompt == nil {
		prompt = io.Discard
	}

	mode, err := cfg.ResolveMode()
	if err != nil {
		return nil, "", err
	}
	if err := cfg.Validate(mode); err != nil {
		return nil, "", err
	}
	policy, err := newCredentialPolicy(cfg, mode)
	if err != nil {
		return nil, "", err
	}

	switch mode {
	case ModeNone:
		return base, mode, nil
	case ModeBasic:
		return basicTransport{
			base: base, username: cfg.Username, password: cfg.Password,
			policy: policy,
		}, mode, nil
	case ModeBearer:
		return bearerTransport{
			base: base, token: cfg.BearerToken,
			policy: policy,
		}, mode, nil
	case ModeURLToken:
		return urlTokenTransport{
			base: base, token: cfg.URLToken,
			policy: policy,
		}, mode, nil
	case ModeOAuthClient:
		tokenContext := oauthHTTPContext(ctx, cfg.Timeout, base)
		source := (&clientcredentials.Config{
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			TokenURL:     cfg.TokenURL,
			Scopes:       cfg.Scopes,
		}).TokenSource(tokenContext)
		token, tokenErr := source.Token()
		if tokenErr != nil {
			return nil, "", oauthTokenError("obtain OAuth client credentials token", tokenErr)
		}
		return credentialTransport{
			base:   &oauth2.Transport{Source: oauth2.ReuseTokenSource(token, source), Base: base},
			policy: policy,
		}, mode, nil
	case ModeOAuthCode:
		token, tokenErr := authorizationCodeToken(ctx, cfg, base, prompt)
		if tokenErr != nil {
			return nil, "", tokenErr
		}
		tokenContext := oauthHTTPContext(ctx, cfg.Timeout, base)
		source := oauthAuthorizationConfig(cfg).TokenSource(tokenContext, token)
		return credentialTransport{
			base:   &oauth2.Transport{Source: oauth2.ReuseTokenSource(token, source), Base: base},
			policy: policy,
		}, mode, nil
	default:
		return nil, "", fmt.Errorf("unsupported authentication mode %q", mode)
	}
}

type basicTransport struct {
	base               http.RoundTripper
	username, password string
	policy             credentialPolicy
}

func (t basicTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := t.policy.validateRequest(request); err != nil {
		return nil, err
	}
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	clone.SetBasicAuth(t.username, t.password)
	return t.base.RoundTrip(clone)
}

type bearerTransport struct {
	base   http.RoundTripper
	token  string
	policy credentialPolicy
}

func (t bearerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := t.policy.validateRequest(request); err != nil {
		return nil, err
	}
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	clone.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(clone)
}

type urlTokenTransport struct {
	base   http.RoundTripper
	token  string
	policy credentialPolicy
}

func (t urlTokenTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := t.policy.validateRequest(request); err != nil {
		return nil, err
	}
	clone := request.Clone(request.Context())
	clone.URL = cloneURL(request.URL)
	query := clone.URL.Query()
	query.Set("access_token", t.token)
	clone.URL.RawQuery = query.Encode()
	return t.base.RoundTrip(clone)
}

// credentialTransport guards transports such as oauth2.Transport that inject
// their own Authorization header. The check is outside that transport so a
// rejected request cannot trigger token refresh before its destination has
// been found safe.
type credentialTransport struct {
	base   http.RoundTripper
	policy credentialPolicy
}

func (t credentialTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := t.policy.validateRequest(request); err != nil {
		return nil, err
	}
	return t.base.RoundTrip(request)
}

func (c Config) validateCredentialEndpoints(mode Mode) error {
	if mode == ModeNone {
		return nil
	}
	if c.Endpoint == "" {
		return errors.New("authenticated TAMS endpoint is required")
	}
	if err := validateCredentialURL("TAMS endpoint", c.Endpoint, c.AllowInsecureLoopback); err != nil {
		return err
	}
	if mode == ModeOAuthClient || mode == ModeOAuthCode {
		if err := validateCredentialURL("OAuth token endpoint", c.TokenURL, c.AllowInsecureLoopback); err != nil {
			return err
		}
	}
	if mode == ModeOAuthCode && c.AuthURL != "" {
		if err := validateCredentialURL("OAuth authorization endpoint", c.AuthURL, c.AllowInsecureLoopback); err != nil {
			return err
		}
	}
	if mode == ModeOAuthCode {
		if err := validateOAuthRedirectURL(c.RedirectURL); err != nil {
			return err
		}
	}
	return nil
}

type credentialPolicy struct {
	origin                string
	allowInsecureLoopback bool
}

func newCredentialPolicy(config Config, mode Mode) (credentialPolicy, error) {
	if mode == ModeNone {
		return credentialPolicy{}, nil
	}
	parsed, err := url.Parse(config.Endpoint)
	if err != nil {
		return credentialPolicy{}, errors.New("TAMS endpoint is not a valid absolute URL")
	}
	return credentialPolicy{
		origin: credentialOrigin(parsed), allowInsecureLoopback: config.AllowInsecureLoopback,
	}, nil
}

func (p credentialPolicy) validateRequest(request *http.Request) error {
	if request == nil || request.URL == nil {
		return errors.New("authenticated request has no valid destination")
	}
	if err := validateParsedCredentialURL("authenticated request", request.URL, p.allowInsecureLoopback); err != nil {
		return err
	}
	if credentialOrigin(request.URL) != p.origin {
		return errors.New("authenticated request must remain on the configured TAMS origin")
	}
	return nil
}

func validateCredentialURL(label, rawURL string, allowInsecureLoopback bool) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("%s is not a valid absolute URL", label)
	}
	return validateParsedCredentialURL(label, parsed, allowInsecureLoopback)
}

func validateParsedCredentialURL(label string, parsed *url.URL, allowInsecureLoopback bool) error {
	if !parsed.IsAbs() || parsed.Host == "" {
		return fmt.Errorf("%s is not a valid absolute URL", label)
	}
	if parsed.User != nil {
		return fmt.Errorf("%s must not contain userinfo", label)
	}
	if parsed.Scheme == "https" {
		return nil
	}
	if parsed.Scheme == "http" && allowInsecureLoopback && isLoopbackHost(parsed.Hostname()) {
		return nil
	}
	if parsed.Scheme == "http" {
		return fmt.Errorf("%s must use HTTPS before authentication credentials can be sent; plaintext is limited to explicit loopback development", label)
	}
	return fmt.Errorf("%s must use HTTPS before authentication credentials can be sent", label)
}

func validateOAuthRedirectURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" || parsed.User != nil {
		return errors.New("OAuth redirect URL is not a valid absolute URL without userinfo")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme == "https" || scheme == "http" && isLoopbackHost(parsed.Hostname()) {
		return nil
	}
	return errors.New("OAuth redirect URL must use HTTPS or HTTP on localhost or a literal loopback IP")
}

func credentialOrigin(value *url.URL) string {
	scheme := strings.ToLower(value.Scheme)
	host := strings.ToLower(value.Hostname())
	port := value.Port()
	if scheme == "http" && port == "80" || scheme == "https" && port == "443" {
		port = ""
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return scheme + "://" + host
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func cloneURL(input *url.URL) *url.URL {
	clone := *input
	return &clone
}

// ExtractURLToken removes a TAMS access_token query parameter so it cannot be
// copied into logs or accidentally discarded while resolving API paths.
func ExtractURLToken(rawEndpoint string) (endpoint, token string, err error) {
	parsed, err := url.Parse(rawEndpoint)
	if err != nil {
		return "", "", errors.New("TAMS endpoint is not a valid URL")
	}
	query := parsed.Query()
	token = query.Get("access_token")
	query.Del("access_token")
	parsed.RawQuery = query.Encode()
	return strings.TrimRight(parsed.String(), "/"), token, nil
}

// RedactURL removes userinfo and every query value before a URL appears in diagnostics.
func RedactURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "<invalid-url>"
	}
	if parsed.User != nil {
		parsed.User = url.User("REDACTED")
	}
	query := parsed.Query()
	for key := range query {
		query.Set(key, "REDACTED")
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func oauthAuthorizationConfig(cfg Config) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		Endpoint: oauth2.Endpoint{
			AuthURL:  cfg.AuthURL,
			TokenURL: cfg.TokenURL,
		},
		RedirectURL: cfg.RedirectURL,
		Scopes:      cfg.Scopes,
	}
}
func oauthHTTPContext(ctx context.Context, timeout time.Duration, base http.RoundTripper) context.Context {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithValue(ctx, oauth2.HTTPClient, &http.Client{
		Transport: base, Timeout: timeout, CheckRedirect: rejectRedirect,
	})
}

func oauthTokenError(action string, err error) error {
	var retrieveError *oauth2.RetrieveError
	if errors.As(err, &retrieveError) && retrieveError.Response != nil {
		// The HTTP reason phrase is peer-controlled and may echo a submitted
		// client secret or authorization code. Report only the numeric status and
		// Go's canonical text rather than the raw response.Status string.
		return fmt.Errorf("%s: token endpoint returned %s", action, safeHTTPStatus(retrieveError.Response.StatusCode))
	}
	return fmt.Errorf("%s: token endpoint request failed", action)
}

func safeHTTPStatus(code int) string {
	if text := http.StatusText(code); text != "" {
		return fmt.Sprintf("%d %s", code, text)
	}
	return fmt.Sprintf("%d", code)
}
func rejectRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

func authorizationCodeToken(ctx context.Context, cfg Config, base http.RoundTripper, prompt io.Writer) (*oauth2.Token, error) {
	oauthConfig := oauthAuthorizationConfig(cfg)
	exchangeContext := oauthHTTPContext(ctx, cfg.Timeout, base)
	if cfg.OAuthCode != "" {
		var options []oauth2.AuthCodeOption
		if cfg.PKCEVerifier != "" {
			options = append(options, oauth2.VerifierOption(cfg.PKCEVerifier))
		}
		token, err := oauthConfig.Exchange(exchangeContext, cfg.OAuthCode, options...)
		if err != nil {
			return nil, oauthTokenError("exchange OAuth authorization code", err)
		}
		return token, nil
	}

	redirect, err := parseInteractiveRedirect(cfg.RedirectURL)
	if err != nil {
		return nil, err
	}
	callbackPath := redirect.Path
	if callbackPath == "" {
		callbackPath = "/"
	}

	stateBytes := make([]byte, 32)
	if _, err := rand.Read(stateBytes); err != nil {
		return nil, fmt.Errorf("create OAuth state: %w", err)
	}
	state := base64.RawURLEncoding.EncodeToString(stateBytes)

	listener, err := net.Listen("tcp", redirect.Host)
	if err != nil {
		return nil, fmt.Errorf("listen for OAuth callback at %s: %w", redirect.Host, err)
	}
	defer listener.Close()

	type callback struct {
		code string
		err  error
	}
	result := make(chan callback, 1)
	mux := http.NewServeMux()
	mux.HandleFunc(callbackPath, func(writer http.ResponseWriter, request *http.Request) {
		if subtle.ConstantTimeCompare([]byte(request.URL.Query().Get("state")), []byte(state)) != 1 {
			http.Error(writer, "invalid OAuth state", http.StatusBadRequest)
			select {
			case result <- callback{err: errors.New("OAuth callback state did not match")}:
			default:
			}
			return
		}
		if providerError := request.URL.Query().Get("error"); providerError != "" {
			http.Error(writer, "authorization failed", http.StatusBadRequest)
			select {
			case result <- callback{err: errors.New("OAuth provider returned an authorization error")}:
			default:
			}
			return
		}
		code := request.URL.Query().Get("code")
		if code == "" {
			http.Error(writer, "missing authorization code", http.StatusBadRequest)
			select {
			case result <- callback{err: errors.New("OAuth callback did not include a code")}:
			default:
			}
			return
		}
		_, _ = io.WriteString(writer, "Authorization complete. Return to Tamsin.\n")
		select {
		case result <- callback{code: code}:
		default:
		}
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = server.Serve(listener)
	}()
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
	}()

	verifier := oauth2.GenerateVerifier()
	authURL := oauthConfig.AuthCodeURL(state, oauth2.AccessTypeOffline, oauth2.S256ChallengeOption(verifier))
	_, _ = fmt.Fprintf(prompt, "Open this URL to authorize Tamsin:\n%s\n", authURL)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case callbackResult := <-result:
		if callbackResult.err != nil {
			return nil, callbackResult.err
		}
		token, err := oauthConfig.Exchange(exchangeContext, callbackResult.code, oauth2.VerifierOption(verifier))
		if err != nil {
			return nil, oauthTokenError("exchange OAuth authorization code", err)
		}
		return token, nil
	}
}

func parseInteractiveRedirect(rawURL string) (*url.URL, error) {
	redirect, err := url.Parse(rawURL)
	if err != nil {
		return nil, errors.New("OAuth redirect URL is not valid")
	}
	// This URL is an inbound callback listener, not an outbound credential
	// destination. OAuth native-app guidance deliberately uses loopback HTTP;
	// keep that exception separate from AllowInsecureLoopback and constrain it
	// to an exact localhost name or literal IPv4/IPv6 loopback address.
	if !redirect.IsAbs() || redirect.Scheme != "http" || redirect.Host == "" ||
		redirect.User != nil || !isLoopbackHost(redirect.Hostname()) {
		return nil, errors.New("interactive OAuth redirect URL must use HTTP on localhost or a literal loopback IP; supply oauth-code for another redirect")
	}
	return redirect, nil
}
