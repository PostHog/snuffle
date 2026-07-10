package snuffle

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestServerTLSConfig(t *testing.T) {
	t.Run("generated certificate", func(t *testing.T) {
		cfg, err := serverTLSConfig(Config{TLSEnabled: true, HTTPHost: "snuffle.example"})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MinVersion != tls.VersionTLS12 || len(cfg.Certificates) != 1 {
			t.Fatalf("TLS config = min version %d certificates %d", cfg.MinVersion, len(cfg.Certificates))
		}
		cert, err := x509.ParseCertificate(cfg.Certificates[0].Certificate[0])
		if err != nil {
			t.Fatal(err)
		}
		if err := cert.VerifyHostname("snuffle.example"); err != nil {
			t.Fatalf("generated certificate hostname: %v", err)
		}
	})

	t.Run("configured certificate", func(t *testing.T) {
		cert, err := selfSignedCertificate("snuffle.example")
		if err != nil {
			t.Fatal(err)
		}
		key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		certFile := filepath.Join(dir, "cert.pem")
		keyFile := filepath.Join(dir, "key.pem")
		if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := serverTLSConfig(Config{TLSEnabled: true, TLSCertFile: certFile, TLSKeyFile: keyFile})
		if err != nil || len(cfg.Certificates) != 1 {
			t.Fatalf("load configured certificate = (%v, %v)", cfg, err)
		}
	})

	if _, err := serverTLSConfig(Config{TLSEnabled: true, TLSCertFile: "cert.pem"}); err == nil {
		t.Fatal("TLS config accepted a certificate without a key")
	}
}
