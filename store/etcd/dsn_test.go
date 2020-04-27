package etcd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDSN(t *testing.T) {
	tests := []struct{
		name string
		dsn string
		expectError bool
		expectAddr string
		expectNamespace string
	}{
		{
			name:            "golden path",
			dsn:             "etcd://etcd.dmesh:2379/eos-dev1",
			expectError:     false,
			expectAddr:      "etcd.dmesh:2379",
			expectNamespace: "/eos-dev1",
		},
		{
			name:            "missing namespace",
			dsn:             "etcd://etcd.dmesh:2379",
			expectError:     true,
		},
		{
			name:            "missing addr",
			dsn:             "etcd://eos-dev1",
			expectError:     true,
		},
		{
			name:            "sub-namespace",
			dsn:             "etcd://etcd.dmesh:2379/eos/dev1",
			expectAddr:      "etcd.dmesh:2379",
			expectNamespace: "/eos/dev1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			addr, namespace, err := ParseDSN(test.dsn)
			if test.expectError {
				require.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expectAddr, addr)
				assert.Equal(t, test.expectNamespace, namespace)
			}
		})
	}
}