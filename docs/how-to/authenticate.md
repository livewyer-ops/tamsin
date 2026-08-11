# Authenticate against a TAMS store

TAMSin implements every authentication mechanism in the pinned TAMS 8.1 contract and both OAuth grants its bearer-token description recommends. Pick the one your store offers.

Prefer the environment over flags: secret flags exist for interactive use, but are visible in process listings.

## Bearer token

```sh
export TAMSIN_AUTH_MODE=bearer
export TAMSIN_AUTH_TOKEN='...'
```

## OAuth2 client credentials

For unattended jobs holding a client ID and secret:

```sh
export TAMSIN_AUTH_MODE=oauth-client
export TAMSIN_AUTH_TOKEN_URL='https://identity.example.com/oauth/token'
export TAMSIN_AUTH_CLIENT_ID='...'
export TAMSIN_AUTH_CLIENT_SECRET='...'
export TAMSIN_AUTH_SCOPES='tams.write'   # optional
```

If your client ID begins with `-`, set it through the environment as above; passed as a flag it would be parsed as an option, and `--client-id=-value` is needed instead.

## HTTP basic

```sh
export TAMSIN_AUTH_MODE=basic
export TAMSIN_AUTH_USERNAME='...'
export TAMSIN_AUTH_PASSWORD='...'
```

## URL token

TAMS defines an `access_token` query parameter. Either put it in the endpoint or supply it separately:

```sh
export TAMSIN_AUTH_MODE=url-token
export TAMSIN_ENDPOINT='https://tams.example.com?access_token=...'
```

## OAuth2 authorization code

For interactive use, TAMSin opens a localhost callback and generates PKCE automatically:

```sh
export TAMSIN_AUTH_MODE=oauth-code
export TAMSIN_AUTH_TOKEN_URL='https://identity.example.com/oauth/token'
export TAMSIN_AUTH_AUTHORIZATION_URL='https://identity.example.com/oauth/authorize'
export TAMSIN_AUTH_CLIENT_ID='...'
```

The interactive command prints the complete provider authorization URL because
its query parameters are required to complete OAuth. Treat that prompt as
sensitive terminal output; it is the deliberate exception to TAMSin's normal
URL-query redaction. `tamsin doctor --online` never starts this flow: give
Doctor a pre-obtained code, or authorize with another command first.

With a code obtained elsewhere, supply `TAMSIN_AUTH_CODE` and, where the provider used PKCE, `TAMSIN_AUTH_PKCE_VERIFIER`. Exchanging a pre-obtained code does not require an authorization URL; it still requires the token URL, client ID, and the redirect URL used when the code was issued.

## Let TAMSin choose

The default `auto` mode selects by what you have configured, in this order: URL token, static bearer, authorization code, client credentials, basic, then none. A code, PKCE verifier, or authorization URL signals the authorization-code grant; a client secret signals client credentials. Shared OAuth settings such as only a client ID, token URL, or scopes do not identify a grant, so `auto` reports an incomplete/ambiguous configuration instead of silently choosing no authentication.

Set the mode explicitly for anything unattended. With both a token and OAuth credentials present, `auto` picks bearer and the OAuth path is never exercised — which is rarely what you meant.

## Transport security

TAMSin sends Basic credentials, bearer tokens and URL tokens only to HTTPS TAMS endpoints. OAuth token and authorization endpoints must also use HTTPS. Every credential-bearing TAMS request is pinned to the configured endpoint's canonical origin, so even a cross-origin HTTPS redirect is rejected before credentials are attached. OAuth token redirects are not followed. These checks happen before token acquisition or a TAMS request.

For a development service listening directly on the same machine, plaintext authentication requires a separate, explicit opt-in:

```sh
tamsin --allow-insecure-auth-loopback \
  --endpoint http://127.0.0.1:8000 \
  --auth bearer --token "$DEV_TOKEN" api service
```

The exception accepts only the exact hostname `localhost` (case-insensitive), an address in `127.0.0.0/8`, or IPv6 `::1`. It does not permit private-network addresses, localhost-like suffixes, abbreviated or integer IPv4 spellings, or remote HTTP even when the flag is set. Unauthenticated TAMS endpoints may still use HTTP.

The default authorization-code callback is deliberately different: it is an inbound HTTP listener on loopback and never sends a credential to that URL. It remains available without the unsafe flag, including with a literal IPv6 `::1` callback. Token and authorization endpoints still require HTTPS.

## Check it works

```sh
tamsin doctor --online
```

The `auth` field reports which mechanism was used. Outside the explicit
interactive authorization prompt described above, URLs in output and
diagnostics have userinfo removed and every query value redacted, so tokens do
not leak into logs.

## Self-signed certificates

Against a development store with an untrusted certificate:

```sh
export TAMSIN_HTTP_INSECURE_SKIP_VERIFY=true
```

This disables certificate verification for the API, storage URLs, HTTP inputs and S3 alike. It is unsafe by design and intended for local testing.

It does not permit plaintext authentication. Use `--allow-insecure-auth-loopback` separately when a loopback-only development endpoint has no TLS listener.

## See also

- [Configuration reference](../reference/configuration.md) — every key, flag and environment name
