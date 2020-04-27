package etcd

import (
	"fmt"
	"net/url"
)

func ParseDSNURL(dsnURL *url.URL) (addr string, namespace string, err error) {
	addr = dsnURL.Host
	if addr == "" {
		return "","", fmt.Errorf("invalid etcd dsn missing etcd addr %q", dsnURL.String())
	}

	namespace = dsnURL.Path
	if namespace == "" {
		return "","", fmt.Errorf("invalid etcd dsn missing etcd namespace %q", dsnURL.String())
	}

	return
}
