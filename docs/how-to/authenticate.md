# Authenticate against TAMS

Prefer environment variables to secret flags, which may appear in process
listings. Set the mode explicitly for unattended work.

## Bearer token

```sh
export TAMSIN_AUTH_MODE=bearer
export TAMSIN_AUTH_TOKEN='...'
```

## OAuth client credentials

```sh
export TAMSIN_AUTH_MODE=oauth-client
export TAMSIN_AUTH_TOKEN_URL='https://identity.example.com/oauth/token'
export TAMSIN_AUTH_CLIENT_ID='...'
export TAMSIN_AUTH_CLIENT_SECRET='...'
export TAMSIN_AUTH_SCOPES='tams.write' # optional
```

## Pre-obtained OAuth code

TAMSin exchanges a code obtained by another application. It does not open a
browser or listen for an OAuth callback.

```sh
export TAMSIN_AUTH_MODE=oauth-code
export TAMSIN_AUTH_TOKEN_URL='https://identity.example.com/oauth/token'
export TAMSIN_AUTH_CLIENT_ID='...'
export TAMSIN_AUTH_REDIRECT_URL='https://client.example.com/callback'
export TAMSIN_AUTH_CODE='...'
export TAMSIN_AUTH_PKCE_VERIFIER='...' # only when used to obtain the code
```

## HTTP Basic or URL token

```sh
export TAMSIN_AUTH_MODE=basic
export TAMSIN_AUTH_USERNAME='...'
export TAMSIN_AUTH_PASSWORD='...'
```

For the TAMS `access_token` query mechanism, use an endpoint containing that
parameter or set `TAMSIN_AUTH_URL_TOKEN` with mode `url-token`.

## Automatic selection

The default `auto` mode selects URL token, bearer, OAuth code, OAuth client
credentials, Basic, then no authentication according to the complete
credentials present. Incomplete or ambiguous OAuth settings fail rather than
silently choosing another mode.

## Transport policy

Credentials are sent only over HTTPS and only to the configured TAMS origin.
OAuth token redirects are not followed. For a development service on the same
machine, plaintext credentials require an explicit exception:

```sh
tamsin --allow-insecure-auth-loopback \
  --endpoint http://127.0.0.1:8000 \
  --auth bearer --token "$DEV_TOKEN" doctor --online
```

The exception accepts only `localhost`, `127.0.0.0/8` and `::1`. It does not
permit private-network or remote HTTP endpoints.

For a development TLS service with an untrusted certificate,
`TAMSIN_HTTP_INSECURE_SKIP_VERIFY=true` disables certificate checks for API,
storage, HTTP input and S3 requests. This is unsafe and does not permit
plaintext authentication.

Run `tamsin doctor --online` to verify the resolved mode and endpoint without
mutating TAMS.
