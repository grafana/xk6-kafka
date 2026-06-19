// Package kafka implements the k6/x/kafka extension: the official,
// Grafana-owned, pure-Go k6 extension for load testing Apache Kafka.
//
// This file provides the module scaffold — registration, the exported constant
// surface, and the public symbols (Writer, Reader, Connection, SchemaRegistry,
// LoadJKS). Method behavior is implemented by later changes; see index.d.ts for
// the authoritative API contract.
package kafka

import (
	"github.com/grafana/sobek"
	"go.k6.io/k6/v2/js/common"
	"go.k6.io/k6/v2/js/modules"
)

// RootModule is the global module factory; one is created per k6 process.
type RootModule struct{}

// Module is the per-VU instance of the k6/x/kafka module.
type Module struct {
	vu      modules.VU
	exports *sobek.Object
}

var (
	_ modules.Module   = (*RootModule)(nil)
	_ modules.Instance = (*Module)(nil)
)

// NewModuleInstance implements modules.Module. It builds the module's default
// export object with the flat constants and the public symbols.
func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	m := &Module{vu: vu, exports: vu.Runtime().NewObject()}
	m.defineConstants()
	m.defineSymbols()
	return m
}

// Exports implements modules.Instance. The module members are exposed as the
// default export object; named imports resolve to its properties.
func (m *Module) Exports() modules.Exports {
	return modules.Exports{Default: m.exports}
}

// defineConstants attaches the flat top-level constants to the export object.
func (m *Module) defineConstants() {
	rt := m.vu.Runtime()
	for name, value := range moduleConstants() {
		if err := m.exports.Set(name, value); err != nil {
			common.Throw(rt, err)
		}
	}
}

// defineSymbols registers the public constructors and the LoadJKS function.
// Construction succeeds; method behavior is delivered by later changes.
func (m *Module) defineSymbols() {
	rt := m.vu.Runtime()
	set := func(name string, value any) {
		if err := m.exports.Set(name, value); err != nil {
			common.Throw(rt, err)
		}
	}

	// Scaffold constructors: invocable with `new` and construct without error.
	// Full class/prototype semantics (e.g. `instanceof`) are established when
	// instance methods are added by later changes; the scaffold only guarantees
	// the symbols exist and construct.
	set("Writer", scaffoldConstructor())
	set("Reader", scaffoldConstructor())
	set("Connection", scaffoldConstructor())
	set("SchemaRegistry", scaffoldConstructor())
	set("LoadJKS", m.loadJKS)
}

// scaffoldConstructor returns a native constructor that constructs without
// error. It returns nil so the runtime supplies the constructed object.
// Method behavior is added by later changes.
func scaffoldConstructor() func(sobek.ConstructorCall) *sobek.Object {
	return func(_ sobek.ConstructorCall) *sobek.Object { return nil }
}

// loadJKS is the LoadJKS function symbol. Keystore-loading behavior is deferred
// to the auth change; for now it is present and callable.
func (m *Module) loadJKS(_ sobek.FunctionCall) sobek.Value {
	return sobek.Undefined()
}
