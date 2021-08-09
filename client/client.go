package client

import (
	"fmt"
	"net/url"

	"github.com/streamingfast/dmesh/client/etcd"
	"github.com/streamingfast/dmesh/client/local"
)

func New(dsnString string) (SearchClient, error) {
	dsn, err := url.Parse(dsnString)
	if err != nil {
		return nil, fmt.Errorf("cannot parse dsn %q: %w", dsnString, err)
	}
	switch dsn.Scheme {
	case "etcd":
		return etcd.New(dsn)
	case "local":
		return local.New(dsn)
	}
	return nil, fmt.Errorf("cannot resolve dmesh scheme %q", dsn.Scheme)

}



