package etcd

import (
	"fmt"
	"net/url"
)

func ParseDSN(dsnString string) (addr string, namespace string, err error) {
	dsn, err := url.Parse(dsnString)
	if err != nil {
		return "","", fmt.Errorf("cannot partse etcd dsn %q: %w", dsnString, err)
	}

	addr = dsn.Host
	if addr == "" {
		return "","", fmt.Errorf("invalid etcd dsn missing etcd addr %q", dsnString)
	}

	namespace = dsn.Path
	if namespace == "" {
		return "","", fmt.Errorf("invalid etcd dsn missing etcd namespace %q", dsnString)
	}

	return
}