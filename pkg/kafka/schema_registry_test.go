package kafka

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test wire format encoding/decoding (task 3.3)
func TestWireFormatRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		schemaID int
	}{
		{"zero", 0},
		{"one", 1},
		{"max_int32", math.MaxInt32},
		{"typical", 12345},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Encode
			encoded := encodeWireFormat(tt.schemaID)
			require.Equal(t, 5, len(encoded))
			require.Equal(t, byte(0x00), encoded[0])

			// Decode
			decoded, remaining, err := decodeWireFormat(encoded)
			require.NoError(t, err)
			require.Equal(t, tt.schemaID, decoded)
			require.Equal(t, 0, len(remaining))
		})
	}
}

func TestDecodeWireFormatErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		data   []byte
		errMsg string
	}{
		{"too_short", []byte{0x00, 0x00, 0x00}, "too short"},
		{"invalid_magic", []byte{0x01, 0x00, 0x00, 0x00, 0x00}, "invalid magic byte"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := decodeWireFormat(tt.data)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

// Test STRING serdes (task 4.5)
func TestStringSerdes(t *testing.T) {
	t.Parallel()
	sr := &SchemaRegistry{config: nil}

	tests := []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"ascii", "hello world"},
		{"special_chars", "hello\nworld\t!"},
		{"unicode", "こんにちは世界"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Serialize
			encoded, err := sr.serialize(tt.value, "STRING", nil)
			require.NoError(t, err)

			// Deserialize
			decoded, err := sr.deserialize(encoded, "STRING", nil)
			require.NoError(t, err)
			require.Equal(t, tt.value, decoded)
		})
	}
}

func TestStringDeserializeInvalidUTF8(t *testing.T) {
	t.Parallel()
	sr := &SchemaRegistry{config: nil}
	invalidUTF8 := []byte{0xFF, 0xFE}

	_, err := sr.deserialize(invalidUTF8, "STRING", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid UTF-8")
}

// Test BYTES serdes (task 4.5)
func TestBytesSerdes(t *testing.T) {
	t.Parallel()
	sr := &SchemaRegistry{config: nil}

	tests := []struct {
		name  string
		value []byte
	}{
		{"empty", []byte{}},
		{"binary", []byte{0x00, 0x01, 0x02, 0xFF}},
		{"text_as_bytes", []byte("hello")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Serialize
			encoded, err := sr.serialize(tt.value, "BYTES", nil)
			require.NoError(t, err)
			require.Equal(t, tt.value, encoded)

			// Deserialize
			decoded, err := sr.deserialize(encoded, "BYTES", nil)
			require.NoError(t, err)
			require.Equal(t, tt.value, decoded)
		})
	}
}

// Test Avro serdes (tasks 5.3-5.4)
func TestAvroSerdes(t *testing.T) {
	t.Parallel()
	sr := &SchemaRegistry{config: nil}

	// Simple record schema
	schema := &Schema{
		ID:         0,
		Schema:     `{"type":"record","name":"Test","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`,
		SchemaType: "AVRO",
	}

	data := map[string]any{
		"name": "Alice",
		"age":  30,
	}

	// Serialize
	encoded, err := sr.serialize(data, "AVRO", schema)
	require.NoError(t, err)
	require.Greater(t, len(encoded), 0)

	// Deserialize
	decoded, err := sr.deserialize(encoded, "AVRO", schema)
	require.NoError(t, err)

	// Verify round-trip (Avro preserves types)
	decodedMap := decoded.(map[string]any)
	require.Equal(t, "Alice", decodedMap["name"])
	// Age can be int or float64 depending on Avro version
	switch v := decodedMap["age"].(type) {
	case int:
		require.Equal(t, 30, v)
	case float64:
		require.Equal(t, float64(30), v)
	default:
		t.Fatalf("unexpected type for age: %T", v)
	}
}

func TestAvroWireFormatRoundTrip(t *testing.T) {
	t.Parallel()
	sr := &SchemaRegistry{config: nil}

	schema := &Schema{
		ID:         12345,
		Schema:     `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`,
		SchemaType: "AVRO",
	}

	data := map[string]any{"id": 42}

	// Serialize (with wire format envelope)
	encoded, err := sr.serialize(data, "AVRO", schema)
	require.NoError(t, err)

	// Check wire format envelope present
	require.GreaterOrEqual(t, len(encoded), 5)
	require.Equal(t, byte(0x00), encoded[0])

	// Deserialize (strips envelope, verifies schema ID)
	decoded, err := sr.deserialize(encoded, "AVRO", schema)
	require.NoError(t, err)
	decodedMap := decoded.(map[string]any)
	// Avro may preserve int or convert to float64 depending on version
	switch v := decodedMap["id"].(type) {
	case int:
		require.Equal(t, 42, v)
	case float64:
		require.Equal(t, float64(42), v)
	default:
		t.Fatalf("unexpected type for id: %T", v)
	}
}

func TestAvroSchemaIDMismatch(t *testing.T) {
	t.Parallel()
	sr := &SchemaRegistry{config: nil}

	schema := &Schema{
		ID:         12345,
		Schema:     `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`,
		SchemaType: "AVRO",
	}

	data := map[string]any{"id": 42}

	// Serialize with schema ID 12345
	encoded, err := sr.serialize(data, "AVRO", schema)
	require.NoError(t, err)

	// Try to deserialize with different schema ID
	wrongSchema := &Schema{
		ID:         99999,
		Schema:     schema.Schema,
		SchemaType: "AVRO",
	}

	_, err = sr.deserialize(encoded, "AVRO", wrongSchema)
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema ID mismatch")
}

// Test JSON serdes (tasks 6.3-6.5)
func TestJSONSerdes(t *testing.T) {
	t.Parallel()
	sr := &SchemaRegistry{config: nil}

	schema := &Schema{
		ID:         0,
		Schema:     `{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name"]}`,
		SchemaType: "JSON",
	}

	data := map[string]any{
		"name": "Bob",
		"age":  25,
	}

	// Serialize
	encoded, err := sr.serialize(data, "JSON", schema)
	require.NoError(t, err)

	// Deserialize
	decoded, err := sr.deserialize(encoded, "JSON", schema)
	require.NoError(t, err)
	decodedMap := decoded.(map[string]any)
	require.Equal(t, "Bob", decodedMap["name"])
	require.Equal(t, float64(25), decodedMap["age"])
}

func TestJSONRequiredFieldValidation(t *testing.T) {
	t.Parallel()
	sr := &SchemaRegistry{config: nil}

	schema := &Schema{
		Schema:     `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`,
		SchemaType: "JSON",
	}

	// Missing required field
	data := map[string]any{
		"age": 25,
	}

	_, err := sr.serialize(data, "JSON", schema)
	require.Error(t, err)
	require.Contains(t, err.Error(), "required field missing")
}

func TestJSONWireFormatRoundTrip(t *testing.T) {
	t.Parallel()
	sr := &SchemaRegistry{config: nil}

	schema := &Schema{
		ID:         54321,
		Schema:     `{"type":"object","properties":{"msg":{"type":"string"}},"required":["msg"]}`,
		SchemaType: "JSON",
	}

	data := map[string]any{
		"msg": "hello",
	}

	// Serialize (with wire format envelope)
	encoded, err := sr.serialize(data, "JSON", schema)
	require.NoError(t, err)

	// Check wire format envelope
	require.GreaterOrEqual(t, len(encoded), 5)
	require.Equal(t, byte(0x00), encoded[0])

	// Deserialize (strips envelope, verifies schema ID)
	decoded, err := sr.deserialize(encoded, "JSON", schema)
	require.NoError(t, err)
	decodedMap := decoded.(map[string]any)
	require.Equal(t, "hello", decodedMap["msg"])
}

func TestJSONMalformedBytes(t *testing.T) {
	t.Parallel()
	sr := &SchemaRegistry{config: nil}

	schema := &Schema{
		Schema:     `{}`,
		SchemaType: "JSON",
	}

	_, err := sr.deserialize([]byte(`{invalid json`), "JSON", schema)
	require.Error(t, err)
	require.Contains(t, err.Error(), "JSON decode failed")
}

// Error cases
func TestUnsupportedSchemaType(t *testing.T) {
	t.Parallel()
	sr := &SchemaRegistry{config: nil}

	_, err := sr.serialize("data", "UNKNOWN", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported schema type")
}

func TestStringSerializeWrongType(t *testing.T) {
	t.Parallel()
	sr := &SchemaRegistry{config: nil}

	_, err := sr.serialize(123, "STRING", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "STRING serialize expects string")
}

func TestBytesSerializeWrongType(t *testing.T) {
	t.Parallel()
	sr := &SchemaRegistry{config: nil}

	_, err := sr.serialize("not bytes", "BYTES", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "BYTES serialize expects []byte")
}
