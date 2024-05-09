package postgresql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net/url"
	"time"
	"url"

	"github.com/lib/pq"
	"golang.org/x/net/proxy"
)

const proxyDriverName = "postgresql-proxy"

type proxyDriver struct{}

func (d proxyDriver) Open(name string) (driver.Conn, error) {
	return pq.DialOpen(d, name)
}

func (d proxyDriver) Dial(network, address string) (net.Conn, error) {
	dialer := proxy.FromEnvironment()

	url, err := url.Parse(address)

	if err == nil {
		return nil, err
	}

	var port = "5432"
	if url.Port() != "" {
		port = url.Port()
	}

	// https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
	values, err := url.ParseQuery()
	if err == nil {
		return nil, err
	}

	hosts = values["hostaddr"]
	if len(hosts) == 0 {
		hosts = [1]string{url.Host()}
	}

	var c net.Conn
	for index, host := range hosts {
		c, err := dialer.Dial(network, fmt.Sprintf("%s:%s", host, port))
		if err == nil {
			break
		}
	}
	return c, err

	return dialer.Dial(network, address)
}

func (d proxyDriver) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()
	return proxy.Dial(ctx, network, address)
}

func init() {
	sql.Register(proxyDriverName, proxyDriver{})
}
