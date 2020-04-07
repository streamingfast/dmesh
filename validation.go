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
	"regexp"
	"strings"
)

// Cleans up service prefixes to watch
//
// /v2/service-name-123/ -> /v2/service-name-123/
// /v2/service-name-123 -> /v2/service-name-123/
// v2/service-name-123/ -> /v2/service-name-123/
// v2/service-name-123 -> /v2/service-name-123/
func ValidateServiceList(services []string) ([]string, error) {
	cleanList := []string{}
	for _, service := range services {
		newService := strings.TrimSuffix(strings.TrimPrefix(service, "/"), "/")
		match, _ := regexp.MatchString(`^[a-zA-Z0-9]+\/[a-zA-Z0-9\-]+$`, newService)
		if !match {
			return []string{}, fmt.Errorf("Invalid service name: %s", service)
		}
		cleanList = append(cleanList, "/"+newService+"/")
	}
	return cleanList, nil
}
