package log_test

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	api "github.com/nickstrad/dcl_store/api/v1"
	"github.com/nickstrad/dcl_store/internal/discovery"
	"github.com/nickstrad/dcl_store/internal/log"
	"github.com/stretchr/testify/require"
)

func TestMultipleNodes(t *testing.T) {
	var logs []*log.DistributedLog
	nodeCount := 3
	ports := discovery.GetPorts(nodeCount)

	for i := 0; i < nodeCount; i++ {
		// each node needs its own dir
		dataDir, err := ioutil.TempDir("", "distributed-log-test")
		require.NoError(t, err)

		// remove this particular dir
		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)

		//create tcp listener for stream layer
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[i]))
		fmt.Printf("raft server '%d' listening 127.0.0.1:%d\n", i, ports[i])
		require.NoError(t, err)

		config := log.Config{}

		config.Raft.StreamLayer = log.NewStreamLayer(ln, nil, nil)
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))

		baseTimeout := 200 * time.Millisecond
		config.Raft.HeartbeatTimeout = baseTimeout
		config.Raft.ElectionTimeout = baseTimeout
		config.Raft.LeaderLeaseTimeout = baseTimeout

		config.Raft.CommitTimeout = 5 * time.Millisecond

		// make the first node the leader
		if i == 0 {
			config.Raft.Bootstrap = true
		}

		l, err := log.NewDistributedLog(dataDir, config)
		require.NoError(t, err)

		if i == 0 {
			// wait for leader, and the leader added each server
			err = l.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}

		if i != 0 {
			// logs[0] should be the leader, it needs to be called to add servers to cluster
			err = logs[0].Join(
				fmt.Sprintf("%d", i), ln.Addr().String(),
			)

			require.NoError(t, err)
		}
		logs = append(logs, l)
	}

	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}

	for _, record := range records {
		off, err := logs[0].Append(record)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			for j := 0; j < nodeCount; j++ {
				got, err := logs[j].Read(off)
				if err != nil {
					return false
				}
				record.Offset = off
				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}
			}
			return true
		}, 500*time.Millisecond, 50*time.Millisecond)
	}

	servers, err := logs[0].GetServers()
	require.NoError(t, err)
	require.Equal(t, 3, len(servers))
	require.True(t, servers[0].IsLeader)
	require.False(t, servers[1].IsLeader)
	require.False(t, servers[2].IsLeader)

	// Make server "1" leave from leader
	err = logs[0].Leave("1")
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	servers, err = logs[0].GetServers()
	require.NoError(t, err)
	require.Equal(t, 2, len(servers))

	require.True(t, servers[0].IsLeader)
	require.False(t, servers[1].IsLeader)
	// Add value
	off, err := logs[0].Append(&api.Record{Value: []byte("third")})

	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// verify you get an error from this since it was removed from cluster
	record, err := logs[1].Read(off)
	require.IsType(t, api.ErrOffsetOutOfRange{}, err)
	require.Nil(t, record)

	record, err = logs[2].Read(off)
	require.NoError(t, err)
	require.Equal(t, []byte("third"), record.Value)
	require.Equal(t, off, record.Offset)
}
