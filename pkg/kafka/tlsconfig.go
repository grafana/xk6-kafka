package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
)

// tlsVersion maps a contract TLS_VERSIONS value to a crypto/tls constant.
func tlsVersion(name string) (uint16, error) {
	switch name {
	case tls10:
		return tls.VersionTLS10, nil
	case tls11:
		return tls.VersionTLS11, nil
	case tls12:
		return tls.VersionTLS12, nil
	case tls13:
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("unknown TLS version %q", name)
	}
}

// buildTLSConfig turns a TLSConfig into a *tls.Config. It returns (nil, nil)
// when TLS is not enabled.
func buildTLSConfig(tc *TLSConfig) (*tls.Config, error) {
	if tc == nil || !tc.EnableTLS {
		return nil, nil //nolint:nilnil // a nil config means "no TLS", not an error
	}

	cfg := &tls.Config{}
	if tc.InsecureSkipTLSVerify {
		cfg.InsecureSkipVerify = true // #nosec G402 -- documented, user-controlled opt-out for testing
	}

	if tc.MinVersion != "" {
		v, err := tlsVersion(tc.MinVersion)
		if err != nil {
			return nil, err
		}
		cfg.MinVersion = v
	}

	if tc.ClientCertPem != "" || tc.ClientKeyPem != "" {
		cert, err := tls.X509KeyPair([]byte(tc.ClientCertPem), []byte(tc.ClientKeyPem))
		if err != nil {
			return nil, fmt.Errorf("loading client certificate and key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	if tc.ServerCaPem != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(tc.ServerCaPem)) {
			return nil, errors.New("invalid server CA PEM")
		}
		cfg.RootCAs = pool
	}

	return cfg, nil
}
