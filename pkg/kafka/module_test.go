package kafka

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/js/modulestest"
)

// newTestModule builds a module instance on a test runtime and exposes its
// default export as the global `kafka`.
func newTestModule(t *testing.T) *modulestest.Runtime {
	t.Helper()
	rt := modulestest.NewRuntime(t)
	mi := new(RootModule).NewModuleInstance(rt.VU)
	require.NoError(t, rt.VU.Runtime().Set("kafka", mi.Exports().Default))
	return rt
}

func TestModuleExportsAndConstruction(t *testing.T) {
	t.Parallel()

	rt := newTestModule(t)
	_, err := rt.VU.Runtime().RunString(`
		// Scaffold constructors still present and construct without error.
		// (Connection now connects eagerly and needs a broker, so it is covered
		// by the gated integration tests, not here.)
		new kafka.Writer({ brokers: ["localhost:9092"], topic: "t" });
		new kafka.Reader({ brokers: ["localhost:9092"], topic: "t" });
		new kafka.SchemaRegistry();

		// LoadJKS is present as a function.
		if (typeof kafka.LoadJKS !== "function") {
			throw new Error("LoadJKS is not a function");
		}

		// Flat constants carry the contract values.
		if (kafka.CODEC_SNAPPY !== "snappy") throw new Error("CODEC_SNAPPY");
		if (kafka.KEY !== "key" || kafka.VALUE !== "value") throw new Error("element types");
		if (kafka.SECOND !== 1000000000) throw new Error("SECOND");

		// Grouped values are flat, not enum objects.
		if (typeof kafka.COMPRESSION_CODECS !== "undefined") {
			throw new Error("COMPRESSION_CODECS should not be exported");
		}
	`)
	require.NoError(t, err)
}
