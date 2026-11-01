package flightsvc

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"

	cedarsec "github.com/bbockelm/cedar/security"
)

// SciToken bearer authentication.
//
// A SciToken is what every ADBC client can already carry -- it goes in gRPC metadata as an
// ordinary bearer credential -- and it is verified by the same code CEDAR's SciToken method
// uses, so the trust root is the one the pool already has rather than a second one invented
// here. The identity it yields is `<subject>@<issuer-host>`, which is the mapping CEDAR's
// SciToken path produces, so an ALLOW_READ rule written for one works for the other.
//
// WHAT IS NOT HERE. HTCondor's own IDTOKEN is the other credential a pool holds, and it is not
// supported yet: verifying one means reading the signing keys from passwords.d as root and
// applying the issuer/audience rules, which is a separate piece of work and the wrong thing to
// half-do in an authentication path. A client with only an IDTOKEN is refused with a message
// saying so, rather than admitted on weaker evidence.

// SciTokenConfig is what a SciToken verification needs from the pool's configuration.
//
// Issuers is required, and that is cedar's design rather than a choice made here:
// SciTokenVerifyOptions.Accept documents that a nil accept function accepts NOTHING, mirroring
// HTCondor, which admits a SciToken only when its map file names the issuer and subject. So an
// endpoint with no configured issuers verifies no tokens, and saying so at construction is
// better than appearing to work and refusing everything at runtime.
type SciTokenConfig struct {
	// Issuers are the token issuers this endpoint trusts
	// (SEC_SCITOKENS_ALLOWED_ISSUERS / the SCITOKENS map-file entries).
	Issuers []string
	// Audiences is SCITOKENS_SERVER_AUDIENCE. A token naming an audience must name one of
	// these; a token with no audience claim is accepted only when this is empty.
	Audiences []string
}

type sciTokenAuth struct {
	opts cedarsec.SciTokenVerifyOptions
}

// SciTokenAuthenticator verifies a bearer SciToken and maps it to an HTCondor identity. It
// fails when no issuer is trusted, because such an endpoint could never admit anyone and an
// authenticator that refuses everything is a misconfiguration, not a security posture.
func SciTokenAuthenticator(cfg SciTokenConfig) (Authenticator, error) {
	if len(cfg.Issuers) == 0 {
		return nil, fmt.Errorf("no trusted SciToken issuers configured, so no token could be accepted")
	}
	return sciTokenAuth{opts: cedarsec.SciTokenVerifyOptions{
		Accept:    cedarsec.AcceptIssuers(cfg.Issuers...),
		Audiences: cfg.Audiences,
	}}, nil
}

func (sciTokenAuth) Name() string { return "SCITOKEN" }

func (a sciTokenAuth) Authenticate(ctx context.Context, md metadata.MD) (string, error) {
	tok, ok := BearerToken(md)
	if !ok {
		return "", fmt.Errorf("no bearer credential in the request metadata")
	}
	if !cedarsec.IsSciToken(tok) {
		// Saying which credential was presented saves an operator from guessing why a
		// token that works against the schedd is refused here.
		return "", fmt.Errorf("the bearer credential is not a SciToken (an HTCondor IDTOKEN is not supported by this endpoint yet)")
	}
	claims, err := cedarsec.VerifySciToken(ctx, tok, a.opts)
	if err != nil {
		return "", fmt.Errorf("verifying the SciToken: %w", err)
	}
	user, err := identityFromClaims(claims)
	if err != nil {
		return "", err
	}
	return user, nil
}

// identityFromClaims maps verified claims to a fully-qualified identity.
//
// A token with no subject is refused rather than mapped to its issuer alone: an issuer is not a
// principal, and authorizing one would grant every holder of any token from that issuer
// whatever the issuer was granted.
func identityFromClaims(c *cedarsec.SciTokenClaims) (string, error) {
	sub := strings.TrimSpace(c.Subject)
	if sub == "" {
		return "", fmt.Errorf("the SciToken carries no subject, so it names no principal")
	}
	host := issuerHost(c.Issuer)
	if host == "" {
		return "", fmt.Errorf("the SciToken carries no usable issuer")
	}
	if strings.Contains(sub, "@") {
		// Already qualified; leave it rather than appending a second domain.
		return sub, nil
	}
	return sub + "@" + host, nil
}

// issuerHost reduces an issuer URL to its host, which is the domain half of an HTCondor
// identity. A bare host (no scheme) is accepted as itself.
func issuerHost(issuer string) string {
	s := strings.TrimSpace(issuer)
	if s == "" {
		return ""
	}
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if i := strings.IndexAny(s, "/:"); i >= 0 {
		s = s[:i]
	}
	return s
}

// Optional and anonymous authentication, matching CEDAR's posture.
//
// HTCondor's SEC_*_AUTHENTICATION defaults to OPTIONAL, and with OPTIONAL at both ends the
// negotiation settles on no authentication at all -- so CEDAR routinely serves a peer whose
// identity is the empty string, and the ALLOW_/DENY_ policy decides what that peer may read.
// These authenticators let the Flight endpoint behave the same way, which means a pool's
// existing ALLOW_READ governs both doors rather than one being quietly stricter than the other.
//
// Being permissive here is not the same as being open. An anonymous caller still has to satisfy
// ALLOW_READ, still gets READ level only, and still has private attributes stripped. What
// changes is who decides -- the pool's policy rather than this package.

type anonAuth struct{}

// AnonymousAuthenticator maps every caller to the anonymous identity, leaving the whole
// decision to the authorization policy. It is the direct analogue of a CEDAR connection that
// negotiated no authentication.
func AnonymousAuthenticator() Authenticator { return anonAuth{} }

func (anonAuth) Name() string { return "NONE" }

func (anonAuth) Authenticate(context.Context, metadata.MD) (string, error) { return "", nil }

type optionalAuth struct {
	inner Authenticator
}

// OptionalAuth verifies a credential when one is presented and falls back to the anonymous
// identity when none is. This is what HTCondor means by OPTIONAL.
//
// A credential that is PRESENT but does not verify is still an error. Downgrading it to
// anonymous would leave a caller who meant to authenticate wondering why half the data is
// missing, with nothing anywhere saying their token was rejected -- and it would turn a
// revoked or expired token into a silent loss of privilege rather than a reported one.
func OptionalAuth(inner Authenticator) Authenticator {
	if inner == nil {
		return AnonymousAuthenticator()
	}
	return optionalAuth{inner: inner}
}

func (o optionalAuth) Name() string { return "OPTIONAL(" + o.inner.Name() + ")" }

func (o optionalAuth) Authenticate(ctx context.Context, md metadata.MD) (string, error) {
	if _, ok := BearerToken(md); !ok {
		return "", nil // no credential offered: anonymous, as CEDAR would
	}
	return o.inner.Authenticate(ctx, md)
}
