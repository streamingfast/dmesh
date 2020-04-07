// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dmesh

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ValidateServiceList(t *testing.T) {
	tests := []struct {
		name          string
		serviceList   []string
		expectList    []string
		expectedError error
	}{
		{
			name:          "prefixed service",
			serviceList:   []string{"/v2/service-name"},
			expectList:    []string{"/v2/service-name/"},
			expectedError: nil,
		},
		{
			name:          "suffixed service",
			serviceList:   []string{"v2/service-name/"},
			expectList:    []string{"/v2/service-name/"},
			expectedError: nil,
		},
		{
			name:          "prefixed and suffixed service",
			serviceList:   []string{"/v2/service-name/"},
			expectList:    []string{"/v2/service-name/"},
			expectedError: nil,
		},
		{
			name:          "no suffix nor prefix",
			serviceList:   []string{"v2/service-name"},
			expectList:    []string{"/v2/service-name/"},
			expectedError: nil,
		},
		{
			name:          "invalid service",
			serviceList:   []string{"v2-service-name"},
			expectList:    []string{""},
			expectedError: fmt.Errorf("Invalid service name: v2-service-name"),
		},
		{
			name:          "invalid service with too many /",
			serviceList:   []string{"v2/service/name"},
			expectList:    []string{""},
			expectedError: fmt.Errorf("Invalid service name: v2/service/name"),
		},
		{
			name:          "invalid service name in list",
			serviceList:   []string{"v2/service-name", "v2-service-name"},
			expectList:    []string{""},
			expectedError: fmt.Errorf("Invalid service name: v2-service-name"),
		},
		{
			name:          "validate service name in list",
			serviceList:   []string{"/v3/service-name-3/", "v2/service-name"},
			expectList:    []string{"/v3/service-name-3/", "/v2/service-name/"},
			expectedError: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := ValidateServiceList(test.serviceList)
			if test.expectedError != nil {
				assert.Equal(t, test.expectedError, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectList, res)
			}
		})
	}
}
