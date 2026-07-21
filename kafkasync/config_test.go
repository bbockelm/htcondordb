package kafkasync

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func baseCfg() Config {
	return Config{Table: "jobs", Brokers: []string{"b:9092"}, Topic: "t"}
}

// TestConfigNeverStoresPassword is the core guarantee: a SASL config references its password
// (file/env) and the marshaled config contains no password material or "password" field.
func TestConfigNeverStoresPassword(t *testing.T) {
	c := baseCfg()
	c.SASL = &SASLConfig{Username: "u", PasswordFile: "/etc/kafkasync/pw"}
	c, err := c.Validate()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := c.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	// The reference is stored; no secret and no bare "password" key are.
	if !strings.Contains(s, "passwordFile") {
		t.Fatalf("expected passwordFile reference in config: %s", s)
	}
	if strings.Contains(s, `"password"`) {
		t.Fatalf("config must not contain a \"password\" field: %s", s)
	}
	// SASLConfig has no field that could carry a secret value.
	if strings.Contains(strings.ToLower(s), "hunter2") { // sanity: no value ever leaks
		t.Fatal("secret value found in marshaled config")
	}
}

func TestConfigValidateSASL(t *testing.T) {
	cases := []struct {
		name string
		sasl *SASLConfig
		ok   bool
	}{
		{"file", &SASLConfig{Username: "u", PasswordFile: "/p"}, true},
		{"env", &SASLConfig{Username: "u", PasswordEnv: "PW"}, true},
		{"no-username", &SASLConfig{PasswordFile: "/p"}, false},
		{"no-source", &SASLConfig{Username: "u"}, false},
		{"both-sources", &SASLConfig{Username: "u", PasswordFile: "/p", PasswordEnv: "PW"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := baseCfg()
			c.SASL = tc.sasl
			_, err := c.Validate()
			if (err == nil) != tc.ok {
				t.Fatalf("Validate ok=%v, err=%v", tc.ok, err)
			}
		})
	}
}

func TestConfigValidateTLS(t *testing.T) {
	// cert without key (and vice versa) is rejected; both or neither is fine.
	for _, tc := range []struct {
		name string
		tls  *TLSConfig
		ok   bool
	}{
		{"system-roots", &TLSConfig{}, true},
		{"ca-only", &TLSConfig{CAFile: "/ca.pem"}, true},
		{"cert-without-key", &TLSConfig{CertFile: "/c.pem"}, false},
		{"key-without-cert", &TLSConfig{KeyFile: "/k.pem"}, false},
		{"mtls", &TLSConfig{CertFile: "/c.pem", KeyFile: "/k.pem"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := baseCfg()
			c.TLS = tc.tls
			if _, err := c.Validate(); (err == nil) != tc.ok {
				t.Fatalf("Validate ok=%v, err=%v", tc.ok, err)
			}
		})
	}
}

func TestResolvePassword(t *testing.T) {
	dir := t.TempDir()
	pf := filepath.Join(dir, "pw")
	if err := os.WriteFile(pf, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := (&SASLConfig{PasswordFile: pf}).ResolvePassword(); err != nil || got != "s3cret" {
		t.Fatalf("file resolve = %q, %v (want s3cret; whitespace trimmed)", got, err)
	}
	t.Setenv("KS_TEST_PW", "envpass")
	if got, err := (&SASLConfig{PasswordEnv: "KS_TEST_PW"}).ResolvePassword(); err != nil || got != "envpass" {
		t.Fatalf("env resolve = %q, %v", got, err)
	}
	if _, err := (&SASLConfig{PasswordEnv: "KS_TEST_UNSET"}).ResolvePassword(); err == nil {
		t.Fatal("unset env should error")
	}
	if _, err := (&SASLConfig{}).ResolvePassword(); err == nil {
		t.Fatal("no source should error")
	}
}

// TestBuildTLSConfigMTLS generates a CA and a client cert/key on disk and confirms
// buildTLSConfig loads them into RootCAs + a client certificate (mutual TLS).
func TestBuildTLSConfigMTLS(t *testing.T) {
	dir := t.TempDir()
	caPEM, certPEM, keyPEM := genCertKey(t)
	caFile := writeFile(t, dir, "ca.pem", caPEM)
	certFile := writeFile(t, dir, "cert.pem", certPEM)
	keyFile := writeFile(t, dir, "key.pem", keyPEM)

	tc, err := buildTLSConfig(&TLSConfig{CAFile: caFile, CertFile: certFile, KeyFile: keyFile, ServerName: "broker.local"})
	if err != nil {
		t.Fatal(err)
	}
	if tc.RootCAs == nil {
		t.Error("RootCAs not set from caFile")
	}
	if len(tc.Certificates) != 1 {
		t.Errorf("expected 1 client certificate for mTLS, got %d", len(tc.Certificates))
	}
	if tc.ServerName != "broker.local" {
		t.Errorf("ServerName = %q", tc.ServerName)
	}
	// A bad CA file is a clear error.
	if _, err := buildTLSConfig(&TLSConfig{CAFile: writeFile(t, dir, "junk.pem", []byte("not a cert"))}); err == nil {
		t.Error("expected error for a CA file with no certificates")
	}
}

func writeFile(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// genCertKey returns (caPEM, certPEM, keyPEM) for a self-signed cert usable as both CA and
// leaf in the test.
func genCertKey(t *testing.T) ([]byte, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kafkasync-test"},
		NotBefore:             time.Unix(0, 0),
		NotAfter:              time.Unix(1<<31-1, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, certPEM, keyPEM
}
