package kafka

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/hamba/avro"
)

// BasicAuth holds Schema Registry basic auth credentials.
type BasicAuth struct {
	Username string
	Password string
}

// Schema represents a schema fetched from or registered with Schema Registry.
type Schema struct {
	ID         int    `json:"id"`
	Subject    string `json:"subject"`
	Version    int    `json:"version"`
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
}

// SchemaRegistryConfig holds Schema Registry connection settings.
type SchemaRegistryConfig struct {
	URL       string
	BasicAuth *BasicAuth
	TLS       *TLSConfig
}

// SchemaRegistry is a client for Schema Registry and serdes operations.
type SchemaRegistry struct {
	config *SchemaRegistryConfig
	client *http.Client
}

// NewSchemaRegistry creates a new SchemaRegistry client.
func NewSchemaRegistry(config *SchemaRegistryConfig) (*SchemaRegistry, error) {
	if config == nil {
		// Standalone mode: no registry
		return &SchemaRegistry{config: nil}, nil
	}

	if config.URL == "" {
		return nil, fmt.Errorf("SchemaRegistry: url is required")
	}

	// Build HTTP client with TLS config
	httpClient := &http.Client{}
	if config.TLS != nil {
		tlsConfig := &tls.Config{}
		if config.TLS.InsecureSkipTLSVerify {
			tlsConfig.InsecureSkipVerify = true
		}
		httpClient.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	}

	// Validate connectivity with /config endpoint (requires auth)
	req, err := http.NewRequest("GET", config.URL+"/config", nil)
	if err != nil {
		return nil, fmt.Errorf("SchemaRegistry: failed to create request: %w", err)
	}
	if config.BasicAuth != nil {
		req.SetBasicAuth(config.BasicAuth.Username, config.BasicAuth.Password)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("SchemaRegistry: failed to reach registry at %s: %w", config.URL, err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("SchemaRegistry: registry returned %d", resp.StatusCode)
	}

	sr := &SchemaRegistry{
		config: config,
		client: httpClient,
	}
	return sr, nil
}

// GetSchema fetches a schema from the registry.
func (sr *SchemaRegistry) GetSchema(schemaParam *Schema) (*Schema, error) {
	if sr.config == nil {
		return nil, fmt.Errorf("SchemaRegistry: GetSchema requires registry configuration (standalone mode not supported)")
	}
	if schemaParam == nil || schemaParam.Subject == "" {
		return nil, fmt.Errorf("SchemaRegistry: GetSchema requires schema with subject")
	}

	var version *int
	if schemaParam.Version > 0 {
		version = &schemaParam.Version
	}
	return sr.getSchema(schemaParam.Subject, version)
}

// getSchema fetches a schema from the registry (internal helper).
func (sr *SchemaRegistry) getSchema(subject string, version *int) (*Schema, error) {
	if sr.config == nil {
		return nil, fmt.Errorf("SchemaRegistry: getSchema requires registry configuration (standalone mode not supported)")
	}

	path := fmt.Sprintf("/subjects/%s/versions", subject)
	if version != nil {
		path = fmt.Sprintf("/subjects/%s/versions/%d", subject, *version)
	} else {
		path = fmt.Sprintf("/subjects/%s/versions/latest", subject)
	}

	req, err := http.NewRequest("GET", sr.config.URL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("SchemaRegistry: failed to create request: %w", err)
	}

	if sr.config.BasicAuth != nil {
		req.SetBasicAuth(sr.config.BasicAuth.Username, sr.config.BasicAuth.Password)
	}

	resp, err := sr.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("SchemaRegistry: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("SchemaRegistry: GET %s returned %d: %s", path, resp.StatusCode, string(body))
	}

	var schema Schema
	if err := json.NewDecoder(resp.Body).Decode(&schema); err != nil {
		return nil, fmt.Errorf("SchemaRegistry: failed to decode response: %w", err)
	}

	return &schema, nil
}

// CreateSchema registers a schema in the registry.
func (sr *SchemaRegistry) CreateSchema(schemaParam *Schema) (*Schema, error) {
	if sr.config == nil {
		return nil, fmt.Errorf("SchemaRegistry: CreateSchema requires registry configuration (standalone mode not supported)")
	}
	if schemaParam == nil || schemaParam.Subject == "" || schemaParam.Schema == "" || schemaParam.SchemaType == "" {
		return nil, fmt.Errorf("SchemaRegistry: CreateSchema requires schema with subject, schema, and schemaType")
	}

	return sr.createSchema(schemaParam.Subject, schemaParam.Schema, schemaParam.SchemaType)
}

// createSchema registers a schema in the registry (internal helper).
func (sr *SchemaRegistry) createSchema(subject string, schemaStr string, schemaType string) (*Schema, error) {
	if sr.config == nil {
		return nil, fmt.Errorf("SchemaRegistry: createSchema requires registry configuration (standalone mode not supported)")
	}

	reqBody := map[string]interface{}{
		"schema":     schemaStr,
		"schemaType": schemaType,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("SchemaRegistry: failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", sr.config.URL+"/subjects/"+subject+"/versions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("SchemaRegistry: failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	if sr.config.BasicAuth != nil {
		req.SetBasicAuth(sr.config.BasicAuth.Username, sr.config.BasicAuth.Password)
	}

	resp, err := sr.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("SchemaRegistry: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("SchemaRegistry: POST /subjects/%s/versions returned %d: %s", subject, resp.StatusCode, string(respBody))
	}

	// Decode registry response for ID and version
	var registryResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&registryResp); err != nil {
		return nil, fmt.Errorf("SchemaRegistry: failed to decode response: %w", err)
	}

	id, ok := registryResp["id"].(float64)
	if !ok {
		return nil, fmt.Errorf("SchemaRegistry: response missing schema id")
	}

	version := 1
	if v, ok := registryResp["version"].(float64); ok {
		version = int(v)
	}

	// Return complete Schema with input fields + registry-provided ID/version
	return &Schema{
		ID:         int(id),
		Subject:    subject,
		Version:    version,
		Schema:     schemaStr,
		SchemaType: schemaType,
	}, nil
}

// GetSubjectName returns the subject name for a topic and element using TopicNameStrategy.
func (sr *SchemaRegistry) GetSubjectName(topic string, element string, strategy string) string {
	return sr.getSubjectName(topic, element, strategy)
}

// getSubjectName returns the subject name for a topic and element using TopicNameStrategy.
func (sr *SchemaRegistry) getSubjectName(topic string, element string, strategy string) string {
	// Only TopicNameStrategy supported in v1
	if strategy == "TopicNameStrategy" {
		if element == "key" {
			return topic + "-key"
		}
		return topic + "-value"
	}
	return topic + "-" + strings.ToLower(element)
}

// encodeWireFormat encodes a 5-byte Confluent magic envelope.
func encodeWireFormat(schemaID int) []byte {
	buf := make([]byte, 5)
	buf[0] = 0x00
	binary.BigEndian.PutUint32(buf[1:], uint32(schemaID))
	return buf
}

// decodeWireFormat decodes a 5-byte Confluent magic envelope and returns (schemaID, remainingBytes).
func decodeWireFormat(data []byte) (int, []byte, error) {
	if len(data) < 5 {
		return 0, nil, fmt.Errorf("SchemaRegistry: wire format data too short (need 5 bytes, got %d)", len(data))
	}

	if data[0] != 0x00 {
		return 0, nil, fmt.Errorf("SchemaRegistry: invalid magic byte: expected 0x00, got 0x%02x", data[0])
	}

	schemaID := int(binary.BigEndian.Uint32(data[1:5]))
	return schemaID, data[5:], nil
}

// Serialize encodes data to bytes. Takes a Container-like object with data, schemaType, and schema.
func (sr *SchemaRegistry) Serialize(data interface{}, schemaType string, schema *Schema) ([]byte, error) {
	return sr.serialize(data, schemaType, schema)
}

// serialize encodes data to bytes.
func (sr *SchemaRegistry) serialize(data interface{}, schemaType string, schema *Schema) ([]byte, error) {
	switch schemaType {
	case "STRING":
		if str, ok := data.(string); ok {
			return []byte(str), nil
		}
		return nil, fmt.Errorf("SchemaRegistry: STRING serialize expects string, got %T", data)

	case "BYTES":
		if b, ok := data.([]byte); ok {
			return b, nil
		}
		return nil, fmt.Errorf("SchemaRegistry: BYTES serialize expects []byte, got %T", data)

	case "AVRO":
		if schema == nil {
			return nil, fmt.Errorf("SchemaRegistry: AVRO serialize requires schema")
		}
		avroSchema, err := avro.Parse(schema.Schema)
		if err != nil {
			return nil, fmt.Errorf("SchemaRegistry: failed to parse Avro schema: %w", err)
		}
		encoded, err := avro.Marshal(avroSchema, data)
		if err != nil {
			return nil, fmt.Errorf("SchemaRegistry: Avro encode failed: %w", err)
		}
		if schema.ID != 0 {
			// Add wire format envelope
			encoded = append(encodeWireFormat(schema.ID), encoded...)
		}
		return encoded, nil

	case "JSON":
		if schema == nil {
			return nil, fmt.Errorf("SchemaRegistry: JSON serialize requires schema")
		}
		// Validate data against schema (basic: required fields)
		dataMap, ok := data.(map[string]interface{})
		if ok {
			if err := validateJSONRequired(dataMap, schema.Schema); err != nil {
				return nil, fmt.Errorf("SchemaRegistry: JSON validation failed: %w", err)
			}
		}
		// Encode to JSON
		encoded, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("SchemaRegistry: JSON encode failed: %w", err)
		}
		if schema.ID != 0 {
			// Add wire format envelope
			encoded = append(encodeWireFormat(schema.ID), encoded...)
		}
		return encoded, nil

	default:
		return nil, fmt.Errorf("SchemaRegistry: unsupported schema type: %s", schemaType)
	}
}

// Deserialize decodes bytes to data. Takes a Container-like object with data, schemaType, and schema.
func (sr *SchemaRegistry) Deserialize(data []byte, schemaType string, schema *Schema) (interface{}, error) {
	return sr.deserialize(data, schemaType, schema)
}

// deserialize decodes bytes to data.
func (sr *SchemaRegistry) deserialize(data []byte, schemaType string, schema *Schema) (interface{}, error) {
	switch schemaType {
	case "STRING":
		// Check for wire format envelope
		if schema != nil && schema.ID != 0 && len(data) >= 5 && data[0] == 0x00 {
			schemaID, remaining, err := decodeWireFormat(data)
			if err != nil {
				return nil, err
			}
			if schemaID != schema.ID {
				return nil, fmt.Errorf("SchemaRegistry: schema ID mismatch: expected %d, got %d", schema.ID, schemaID)
			}
			data = remaining
		}
		// Validate UTF-8
		if !utf8.Valid(data) {
			return nil, fmt.Errorf("SchemaRegistry: STRING deserialize: invalid UTF-8")
		}
		return string(data), nil

	case "BYTES":
		// Check for wire format envelope
		if schema != nil && schema.ID != 0 && len(data) >= 5 && data[0] == 0x00 {
			schemaID, remaining, err := decodeWireFormat(data)
			if err != nil {
				return nil, err
			}
			if schemaID != schema.ID {
				return nil, fmt.Errorf("SchemaRegistry: schema ID mismatch: expected %d, got %d", schema.ID, schemaID)
			}
			data = remaining
		}
		return data, nil

	case "AVRO":
		if schema == nil {
			return nil, fmt.Errorf("SchemaRegistry: AVRO deserialize requires schema")
		}

		// Check for wire format envelope
		if schema.ID != 0 && len(data) >= 5 && data[0] == 0x00 {
			schemaID, remaining, err := decodeWireFormat(data)
			if err != nil {
				return nil, err
			}
			if schemaID != schema.ID {
				return nil, fmt.Errorf("SchemaRegistry: schema ID mismatch: expected %d, got %d", schema.ID, schemaID)
			}
			data = remaining
		}

		avroSchema, err := avro.Parse(schema.Schema)
		if err != nil {
			return nil, fmt.Errorf("SchemaRegistry: failed to parse Avro schema: %w", err)
		}

		var result interface{}
		if err := avro.Unmarshal(avroSchema, data, &result); err != nil {
			return nil, fmt.Errorf("SchemaRegistry: Avro decode failed: %w", err)
		}
		return result, nil

	case "JSON":
		if schema == nil {
			return nil, fmt.Errorf("SchemaRegistry: JSON deserialize requires schema")
		}

		// Check for wire format envelope
		if schema.ID != 0 && len(data) >= 5 && data[0] == 0x00 {
			schemaID, remaining, err := decodeWireFormat(data)
			if err != nil {
				return nil, err
			}
			if schemaID != schema.ID {
				return nil, fmt.Errorf("SchemaRegistry: schema ID mismatch: expected %d, got %d", schema.ID, schemaID)
			}
			data = remaining
		}

		var result map[string]interface{}
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, fmt.Errorf("SchemaRegistry: JSON decode failed: %w", err)
		}
		// Validate against schema (basic: required fields)
		if err := validateJSONRequired(result, schema.Schema); err != nil {
			return nil, fmt.Errorf("SchemaRegistry: JSON validation failed: %w", err)
		}
		return result, nil

	default:
		return nil, fmt.Errorf("SchemaRegistry: unsupported schema type: %s", schemaType)
	}
}

// validateJSONRequired performs basic JSON Schema validation: checks required fields.
func validateJSONRequired(data map[string]interface{}, schemaStr string) error {
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
		// Can't parse schema, skip validation
		return nil
	}

	required, ok := schema["required"].([]interface{})
	if !ok {
		// No required fields, validation passes
		return nil
	}

	for _, fieldI := range required {
		field, ok := fieldI.(string)
		if !ok {
			continue
		}
		if _, present := data[field]; !present {
			return fmt.Errorf("required field missing: %s", field)
		}
	}
	return nil
}
