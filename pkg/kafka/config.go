package kafka

// Config structs decoded from the JS-side objects declared in index.d.ts. JSON
// tags match the contract's field names; the k6 field-name mapper uses them when
// exporting sobek values into these structs.

// SASLConfig configures SASL authentication.
type SASLConfig struct {
	Username   string `json:"username"`
	Password   string `json:"password"` //nolint:gosec // config field name, not a hardcoded credential
	Algorithm  string `json:"algorithm"`
	AWSProfile string `json:"awsProfile"`
}

// TLSConfig configures a TLS connection.
type TLSConfig struct {
	EnableTLS             bool   `json:"enableTls"`
	InsecureSkipTLSVerify bool   `json:"insecureSkipTlsVerify"`
	MinVersion            string `json:"minVersion"`
	ClientCertPem         string `json:"clientCertPem"`
	ClientKeyPem          string `json:"clientKeyPem"`
	ServerCaPem           string `json:"serverCaPem"`
}

// ConnectionConfig configures a Connection.
type ConnectionConfig struct {
	Address string      `json:"address"`
	SASL    *SASLConfig `json:"sasl"`
	TLS     *TLSConfig  `json:"tls"`
}

// JKSConfig configures loading a Java KeyStore.
type JKSConfig struct {
	Path     string `json:"path"`
	Password string `json:"password"` //nolint:gosec // config field name, not a hardcoded credential
	// ClientCertAlias is accepted for contract compatibility but not used: the
	// client certificate chain comes from the private-key entry (ClientKeyAlias).
	ClientCertAlias   string `json:"clientCertAlias"`
	ClientKeyAlias    string `json:"clientKeyAlias"`
	ClientKeyPassword string `json:"clientKeyPassword"`
	ServerCaAlias     string `json:"serverCaAlias"`
}

// JKS is the PEM material extracted from a Java KeyStore.
type JKS struct {
	ClientCertsPem []string `json:"clientCertsPem"`
	ClientKeyPem   string   `json:"clientKeyPem"`
	ServerCaPem    string   `json:"serverCaPem"`
}
