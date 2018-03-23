// panzura.go

package panzura

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/etcdserver/api/v3client"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	_ "github.com/coreos/etcd/etcdserver/membership"
	"github.com/coreos/etcd/pkg/transport"
	_ "github.com/coreos/etcd/pkg/types"
)

var (
	lock = &sync.Mutex{}

	etcdSocketPath      = "/tmp/etcd.conn"
	replockTopologyPath = "/tmp/replock.topology"

	topologyIndex uint64
	topology      *Topology
	sites         map[string]*Site
	theSite       *Site

	etcdDir        = "/opt/pixel8/data/etcd.dir"
	etcdToken      = "panzura"
	etcdPeerPort   = 20000
	etcdClientPort = 20001

	etcd    *embed.Etcd
	etcdCli *clientv3.Client
)

type Site struct {
	XMLName	         xml.Name `xml:"site"`
	Fs               string   `xml:"fs"`
	Host             string   `xml:"host"`
	Hosts            string   `xml:"hosts"`
	ActiveHost       string   `xml:"active-host"`
	Port             int      `xml:"port"`
	Local            int      `xml:"local"`
	Excluded         int      `xml:"excluded"`
	SyncedOnly       int      `xml:"syncedonly"`
	NotSyncingLocal  int      `xml:"nosyncinglocal"`
	Hub              int      `xml:"hub"`
	Spare            int      `xml:"spare"`
	Clone            int      `xml:"clone"`
	Ccid             int      `xml:"ccid"`
	Fsid             int      `xml:"fsid"`
	LastGenSnap      int      `xml:"lastgensnap"`
	LastUploadedSnap int      `xml:"lastuploadedsnap"`
	HelloTime        int      `xml:"hellotime"`
	State            int      `xml:"state"`
}

type Topology struct {
	XMLName xml.Name `xml:"cc"`
	Status  int      `xml:"status"`
	Version int      `xml:"version"`
	License int      `xml:"lic"`
	Active  int      `xml:"active"`
	Cloud   int      `xml:"cloud"`
	nego    string   `xml:"nego"`
	lock    string   `xml:"lock"`
	Count   int      `xml:"count"`
	Sites   []*Site   `xml:"site"`
}

func updateTopology(locked bool) error {
	if !locked {
		lock.Lock()
		defer lock.Unlock()
	}

	f, err := os.Open(replockTopologyPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		return err
	}

	var ix uint64
	b := make([]byte, 8)
	if n, err := f.Read(b); err != nil || n != 8 {
		return err
	}
	if ix, n := binary.Uvarint(b); n <= 0 {
		return fmt.Errorf("Malformed Data")
	} else if ix == topologyIndex && theSite != nil {
		// we are up-to-date 
		return nil
	}

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}

	// skip the first 8 bytes and truncate after "</cc>"
	if n := bytes.Index(data, []byte("</cc>")); n == -1 {
		return fmt.Errorf("Malformed Data")
	} else {
		data = data[8:n+5]
	}

	t := Topology{}
	if err = xml.Unmarshal(data, &t); err != nil {
		return err
	}

	ms := map[string]*Site{}
	var ls *Site
	for _, i := range t.Sites {
		ms[strings.ToLower(i.Host)] = i
		if i.Local != 0 {
			ls = i
		}
	}

	topologyIndex = ix
	topology = &t
	sites = ms
	theSite = ls

	return nil
}

func Connect(host string) (net.Conn, error) {
	port := 0

	lock.Lock()
	err := updateTopology(true)
	if err == nil {
		site, ok := sites[strings.ToLower(host)]
		if !ok {
			err = fmt.Errorf("Not Found")
		} else if site.State < 12 {
			err = fmt.Errorf("Not Up")
		} else {
			port = site.Port + 10000
		}
	}
	lock.Unlock()

	if port == 0 {
		return nil, err
	}

	// try local forwarding port
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		// try remote forwarding port
		port += 10000
		conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	}

	return conn, err
}

/*
		address = fmt.Sprintf("%s:22", ls[0])
		conn, err = d.Dialer.Dial(network, address)
		if err == nil {
			cmd := exec.Command("/usr/bin/ssh", "-T", "-o",
				"PasswordAuthentication no", "-o", "ControlPath none",
				"-z", "Panzura+", ls[0])
			cmd.Stdin = conn
			err = cmd.Run()
			if err != nil {
				conn.Close()
				return nil, err
			}
		}
*/

func PrintTopology() {
	lock.Lock()
	defer lock.Unlock()
	err := updateTopology(true)

	if err != nil {
		fmt.Printf("Failed to get topology: %s\n", err)
	} else {
		out, _ := xml.MarshalIndent(topology, "", "  ")
		fmt.Println(string(out))
	}
}

func etcdStart(cluster string, newCluster bool) error {
	lock.Lock()
	err := updateTopology(true)
	if err == nil && theSite == nil {
		err = fmt.Errorf("Local Not Set")
	}
	if err != nil {
		lock.Unlock()
		return err
	}

	host := theSite.Host
	name := strings.Split(host, ".")[0]
	if cluster == "" {
		cluster = fmt.Sprintf("%s=http://%s:%d", name, theSite.Host, etcdPeerPort)
	}
	lock.Unlock()

	// LPUrls: listening peer urls
	// APUrls: advertised peer urls
	// LCUrls: listening client urls
	// ACUrls: advertised client urls
	cfg := embed.NewConfig()
	cfg.Dir = etcdDir
	cfg.Name = name
	u, _ := url.Parse(fmt.Sprintf("http://0.0.0.0:%d", etcdPeerPort))
	cfg.LPUrls = []url.URL{*u}
	u, _ = url.Parse(fmt.Sprintf("http://%s:%d", host, etcdPeerPort))
	cfg.APUrls = []url.URL{*u}
	u, _ = url.Parse(fmt.Sprintf("http://0.0.0.0:%d", etcdClientPort))
	cfg.LCUrls = []url.URL{*u}
	cfg.ACUrls = []url.URL{*u}

	if newCluster {
		cfg.ClusterState = embed.ClusterStateFlagNew
	} else {
		cfg.ClusterState = embed.ClusterStateFlagExisting
	}
	cfg.InitialCluster = cluster
	cfg.InitialClusterToken = etcdToken

	etcd, err = embed.StartEtcd(cfg)
	if err != nil {
		return err
	}

	etcdCli = v3client.New(etcd.Server)
	return nil
}

func isHostInUrl(host, uri string) bool {
	u, e := url.Parse(uri)
	if e != nil {
		return false
	}
	return strings.EqualFold(host, u.Hostname())
}

func etcdJoin() error {
	lock.Lock()
	err := updateTopology(true)
	if err == nil && theSite == nil {
		err = fmt.Errorf("Local Not Set")
	}
	if err != nil {
		lock.Unlock()
		return err
	}

	host := theSite.Host
	name := strings.Split(host, ".")[0]
	ms := map[string]*Site{}
	for _, i := range sites {
		ms[strings.ToLower(i.Host)] = i
	}

	lock.Unlock()

	var cluster string
	for _, i := range ms {
		if i.Local != 0 || i.State < 12 {
			continue
		}

		cli, err := clientv3.New(clientv3.Config{
			Endpoints: []string{fmt.Sprintf("http://localhost:%d", i.Port + 10000)},
			DialTimeout: 10 * time.Second,
		})
		if err != nil {
			cli, err = clientv3.New(clientv3.Config{
				Endpoints: []string{fmt.Sprintf("http://localhost:%d", i.Port + 20000)},
				DialTimeout: 10 * time.Second,
			})
		}
		if err == nil {
			var ms []*etcdserverpb.Member

fmt.Printf("Getting member list from %s:%d...", i.Host, i.Port)
			ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
			mlr, err := cli.MemberList(ctx)
			cancel()
fmt.Printf("done\n")
			if err != nil {
				fmt.Printf("MemberList failed: %s\n", err)
				continue
			} else {
				for _, i := range mlr.Members {
fmt.Printf("host=%s peerurl=%s\n", host, i.PeerURLs[0])
					if isHostInUrl(host, i.PeerURLs[0]) {
						ms = mlr.Members
						break
					}
				}
			}

			if ms == nil {
				peerUrl := fmt.Sprintf("http://%s:%d", host, etcdPeerPort)
				ctx, cancel = context.WithTimeout(context.Background(), 10 * time.Second)
				mar, err := cli.MemberAdd(ctx, []string{peerUrl})
				cancel()
				if err != nil {
					fmt.Printf("MemberAdd failed: %s\n", err)
					continue
				} else {
					ms = mar.Members
				}
			}

			if ms != nil {
				var bb bytes.Buffer
				for _, i := range ms {
					if bb.Len() > 0 {
						bb.WriteString(",")
					}

					n := i.Name
					if n == "" && isHostInUrl(host, i.PeerURLs[0]) {
						n = name
					}

					bb.WriteString(fmt.Sprintf("%s=%s", n, strings.Join(i.PeerURLs, ",")))
				}
				cluster = bb.String()
			}
			cli.Close()
			break
		}
	}

	if cluster != "" {
		return etcdStart(cluster, false)
	} else {
		return fmt.Errorf("No Cluster Found")
	}
}

func etcdRun() error {
	fmt.Printf("Starting...\n")
	err := etcdStart("", false)
	if err == nil {
		return nil
	} else {
		fmt.Println(err)
	}

	// pz.etcd.master
	isEtcdMaster := false
	for {
		{
			cmd := exec.Command("/bin/kenv", "pz.etcd.master")
			out, err := cmd.Output()
			if err == nil {
				out = bytes.TrimSpace(out)
				isEtcdMaster = out[0] == '1'
			}
		}

		// try to join first: 10 times with 10 seconds interval
		for i := 0; i < 10; i++ {
			fmt.Printf("Trying to join...\n")
			err = etcdJoin()
			if err == nil {
				return nil
			}

			time.Sleep(10 * time.Second)
		}

		if isEtcdMaster {
			fmt.Printf("Initializing...\n")
			err = etcdStart("", true)
			if err == nil {
				return nil
			}
		}
	}
}

// print topology and member list
func etcdStatus() {
	PrintTopology()

	ep := fmt.Sprintf("http://localhost:%d", etcdPeerPort)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to server: %s\n", err)
		return
	}
	defer cli.Close()

	status, err := cli.Status(context.Background(), ep)
	if err != nil {
		fmt.Println(err)
		return
	}

	mlr, err := cli.MemberList(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}

    getMemberInfo := func(member *etcdserverpb.Member) *map[string]interface{} {
        return &map[string]interface{}{
            "name": member.Name,
            "id": fmt.Sprintf("%x", member.ID),
            "clientUrls": strings.Join(member.ClientURLs, ","),
            "peerUrls": strings.Join(member.PeerURLs, ","),
        }
    }

    var ms []*etcdserverpb.Member
    if err == nil {
        for _, i := range mlr.Members {
            ms = append(ms, i)
        }
        sort.Slice(ms, func (i, j int) bool {
            return ms[i].Name < ms[j].Name
        })
    }

    var bb bytes.Buffer
    var self, leader *etcdserverpb.Member
    var members []interface{}
    for _, i := range ms {
		if i.ID == status.Header.MemberId {
            self = i
        }
		if i.ID == status.Leader {
            leader = i
        }
        members = append(members, getMemberInfo(i))
        if bb.Len() > 0 {
            bb.WriteString(",")
        }
        bb.WriteString(fmt.Sprintf("%s=%s", i.Name,
            strings.Join(i.PeerURLs, ",")))
    }

    info := map[string]interface{}{
        "cluster": bb.String(),
        "members": members,
    }
    if self != nil {
        info["self"] = &map[string]interface{}{
            "name": self.Name,
            "id": fmt.Sprintf("%x", self.ID),
        }
    }
    if leader != nil {
        info["leader"] = &map[string]interface{}{
            "name": leader.Name,
            "id": fmt.Sprintf("%x", leader.ID),
        }
    }

	out, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to make json: %s\n", err)
	} else {
		fmt.Println(string(out))
	}
}

func startSocketServer(lconn net.Listener) {
	for {
		cc, err := lconn.Accept()
		// TODO: need to catch socket closed error
		if err != nil {
			fmt.Fprintf(os.Stderr, "Accept failed: %s\n", err)
			continue
		}

		go func(ccone net.Conn) {
			b := make([]byte, 4096)
			for {
				var n, m int
				var e error

				n, e = ccone.Read(b[:8])
				if e != nil {
					fmt.Fprintf(os.Stderr, "Read failed: %s\n", e)
					break
				}
				if x, m := binary.Varint(b[:8]); m <= 0 {
					fmt.Fprintf(os.Stderr, "Read failed: %s\n", e)
					break
				} else {
					n = int(x)
				}
				if n > len(b) {
					fmt.Fprintf(os.Stderr, "Size (%d) too big\n", n)
					break
				}
				m, e = ccone.Read(b[:n])
				if m != n {
					fmt.Fprintf(os.Stderr, "Cannot read data\n")
					break
				}

				var j map[string]string
				e = json.Unmarshal(b[:n], &j)
				if e != nil {
					fmt.Fprintf(os.Stderr, "json.Unmarshal() failed: %s\n", e)
					break
				}

				e = nil
				cmd, ok := j["cmd"]
				if !ok {
					fmt.Fprintf(os.Stderr, "Malformed packet\n")
					break
				}

				switch cmd {
				case "put":		// name, value
				case "get":		// name
				case "del":		// name
				case "lock":	// name
				case "unlock":	// name
				case "lease":	// name
				case "release":	// name
				case "watch":	// filter
				default:
				}
			}
			ccone.Close()
		}(cc)
	}
	lconn.Close()
}

func usage() {
	fmt.Printf(`Usage: %s panzura [run | stop | status]
`,
		path.Base(os.Args[0]))
}

func Main() {
	if len(os.Args) < 3 {
		usage()
		os.Exit(1)
	}

	transport.PanzuraConnect = Connect

	var err error

	cmd := os.Args[2]
	switch cmd {
	case "run":
		err = etcdRun()
		if err != nil {
			fmt.Println(err)
			return
		}
	case "stop":
		fmt.Printf("Not Implemented\n")
		return
	case "status":
		etcdStatus()
		return
	default:
		usage()
		return
	}

	os.Remove(etcdSocketPath)

	listener, err := net.Listen("unix", etcdSocketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot start socket server: %s\n", err)
		return
	}

	startSocketServer(listener)
}

// EOF
