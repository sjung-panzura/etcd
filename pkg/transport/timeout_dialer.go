// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"net"
	"strings"
	"time"
)

type rwTimeoutDialer struct {
	wtimeoutd  time.Duration
	rdtimeoutd time.Duration
	net.Dialer
}

var PanzuraConnect func(host string) (net.Conn, error)

func (d *rwTimeoutDialer) Dial(network, address string) (net.Conn, error) {
	var conn net.Conn
	var err error

	if PanzuraConnect == nil {
		conn, err = d.Dialer.Dial(network, address)
	} else {
		ls := strings.Split(address, ":")
		if network != "tcp" || len(ls) != 2 ||
			ls[0] == "localhost" || ls[0] == "127.0.0.1" {
			conn, err = d.Dialer.Dial(network, address)
		} else {
			conn, err = PanzuraConnect(ls[0])
		}
	}

	tconn := &timeoutConn{
		rdtimeoutd: d.rdtimeoutd,
		wtimeoutd:  d.wtimeoutd,
		Conn:       conn,
	}
	return tconn, err
}
