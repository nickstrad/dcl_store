package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	api "github.com/nickstrad/dkv_store/api/v1"
	"github.com/nickstrad/dkv_store/internal/agent"
	"github.com/nickstrad/dkv_store/internal/config"
	"github.com/nickstrad/dkv_store/internal/discovery"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			Server:        true,
			ServerAddress: "127.0.0.1",
		},
	)
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.RootClientCertFile,
			KeyFile:       config.RootClientKeyFile,
			CAFile:        config.CAFile,
			Server:        false,
			ServerAddress: "127.0.0.1",
		},
	)
	require.NoError(t, err)

	var agents []*agent.Agent
	for i := 0; i < 3; i++ {

		ports := discovery.GetPorts(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := ioutil.TempDir("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(
				startJoinAddrs,
				agents[0].Config.BindAddr,
			)
		}

		agent, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
		})
		require.NoError(t, err)

		agents = append(agents, agent)
	}
	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()

	// delay to let agents setup
	time.Sleep(3 * time.Second)

	// get first agent as "leader" agent
	leaderClient := client(t, agents[0], peerTLSConfig)
	appendResponse, err := leaderClient.Append(
		context.Background(),
		&api.AppendRequest{
			Record: &api.Record{
				Value: []byte("foo"),
			},
		},
	)
	require.NoError(t, err)

	readResponse, err := leaderClient.Read(context.Background(),
		&api.ReadRequest{
			Offset: appendResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, readResponse.Record.Value, []byte("foo"))

	// delay for replication
	time.Sleep(3 * time.Second)

	// get another agent that should have data replicated to it by now
	followerClient := client(t, agents[1], peerTLSConfig)
	readResponse, err = followerClient.Read(
		context.Background(),
		&api.ReadRequest{
			Offset: appendResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, readResponse.Record.Value, []byte("foo"))
}

func client(
	t *testing.T,
	agent *agent.Agent,
	tlsConfig *tls.Config,
) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
	}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)
	conn, err := grpc.Dial(rpcAddr, opts...)
	require.NoError(t, err)
	client := api.NewLogClient(conn)
	return client
}
