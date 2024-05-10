package postgresql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/lib/pq"
	"golang.org/x/net/proxy"
)

const proxyDriverName = "postgresql-proxy"

type proxyDriver struct {
	hostaddr []string
	port     string
}

func (d proxyDriver) Open(name string) (driver.Conn, error) {
	if err != nil {
		return nil, err
	}

	d.dsn = name
	u, err := url.Parse(d.dsn)

	// https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
	if values.Get("hostaddr") != "" {
		d.port = "5432"
		if u.Port() != "" {
			d.port = u.Port()
		}

		if values.Get("port") != "" {
			port = values.Get("port")
		}
		d.hostaddr = strings.Split(values.Get("hostaddr"), ",")

		values.Del("hostaddr")
		u.RawQuery = values.Encode()

	}

	return pq.DialOpen(d, name)
}

func (d proxyDriver) dialWithContext(ctx context.Context, network, address string) (net.Conn, error) {

	if len(d.hostaddr) == 0 {
		return proxy.Dial(ctx, network, address)
	}

	// hostaddr supplied in DSN, so ignore supplied address and extract
	// from DSN.

	for _, host := range d.hostaddr {
		c, e := proxy.Dial(ctx, network, net.JoinHostPort(host, d.port))
		if e == nil {
			return c, e
		}
		err = errors.Join(err, fmt.Errorf("could not connect to %s: %s", net.JoinHostPort(host, port), e))
	}
	return nil, err
}

func (d proxyDriver) Dial(network, address string) (net.Conn, error) {
	return d.dialWithContext(context.TODO(), network, address)
}

func (d proxyDriver) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()

	return d.dialWithContext(ctx, network, address)
}

func init() {
	sql.Register(proxyDriverName, proxyDriver{})
}
