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
	dsn string
}

func (d proxyDriver) Open(name string) (driver.Conn, error) {
	d.dsn = name
	return pq.DialOpen(d, name)
}

func (d proxyDriver) dialWithContext(ctx context.Context, network, address string) (net.Conn, error) {
	u, err := url.Parse(d.dsn)

	if err != nil {
		return nil, err
	}

	values, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}

	// https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS

	if values.Get("hostaddr") == "" {
		return proxy.Dial(ctx, network, address)
	}

	// hostaddr supplied in DSN, so ignore supplied address and extract
	// from DSN.

	var port = "5432"
	if u.Port() != "" {
		port = u.Port()
	}

	if values.Get("port") != "" {
		port = values.Get("port")
	}

	for _, host := range strings.Split(values.Get("hostaddr"), ",") {
		c, e := proxy.Dial(ctx, network, net.JoinHostPort(host, port))
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
