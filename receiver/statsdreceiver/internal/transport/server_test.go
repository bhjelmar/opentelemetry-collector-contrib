// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/testutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/statsdreceiver/internal/transport/client"
)

func Test_Server_ListenAndServe(t *testing.T) {
	testMetricNames := []string{
		"foo",
		"foo.bar.baz",
		"1234",
		strings.Repeat("reallylongstring", 15),
		strings.Repeat("reallylongstring", 10),
		strings.Repeat("solong", 1000),
		strings.Repeat("1234567890", 500),
		strings.Repeat("kinda-long", 10),
		"bar",
	}
	var testMetrics []client.Metric
	for _, v := range testMetricNames {
		testMetrics = append(testMetrics, client.Metric{
			Name: v, Value: "42", Type: "c",
		})
	}

	tests := []struct {
		name              string
		transport         Transport
		buildServerFn     func(transport Transport, addr string) (Server, error)
		getFreeEndpointFn func(t testing.TB, transport string) string
		//buildClientFn     func(transport string, address string) (*client.StatsD, error)
	}{
		//{
		//	name:              "udp",
		//	transport:         UDP,
		//	getFreeEndpointFn: testutil.GetAvailableLocalNetworkAddress,
		//	buildServerFn:     NewUDPServer,
		//	buildClientFn:     client.NewStatsD,
		//},
		{
			name:              "tcp",
			transport:         TCP,
			getFreeEndpointFn: testutil.GetAvailableLocalNetworkAddress,
			buildServerFn:     NewTCPServer,
			//buildClientFn:     client.NewStatsD,
		},
	}
	for _, tt := range tests {
		for _, n := range []int{1, 2, 3, 5, 7} {
			t.Run(fmt.Sprint(tt.name, "=", n), func(t *testing.T) {
				addr := tt.getFreeEndpointFn(t, tt.name)
				testFreeEndpoint(t, tt.name, addr)
				srv, err := tt.buildServerFn(tt.transport, addr)
				require.NoError(t, err)
				require.NotNil(t, srv)
				mc := new(consumertest.MetricsSink)
				require.NoError(t, err)
				mr := NewMockReporter(1)
				transferChan := make(chan Metric, 100)
				wgListenAndServe := sync.WaitGroup{}
				wgListenAndServe.Add(1)
				go func() {
					defer wgListenAndServe.Done()
					assert.Error(t, srv.ListenAndServe(mc, mr, transferChan))
				}()
				runtime.Gosched()
				conn, err := net.Dial(tt.transport.String(), addr)
				require.NoError(t, err)
				var buf strings.Builder
				for _, v := range testMetrics {
					buf.WriteString(v.Name)
					buf.WriteString(":42|c\n\n")
				}
				payload := []byte(buf.String())
				orig := payload
				var written []byte
				n := len(payload) / n
				for len(payload) > 0 {
					n = min(len(payload), n)
					_, err = conn.Write(payload[:n])
					written = append(written, payload[:n]...)
					require.NoError(t, err)
					payload = payload[n:]
					time.Sleep(5 * time.Millisecond)
				}
				require.EqualValues(t, orig, written)
				// Keep trying until we're timed out or got a result
				assert.Eventually(t, func() bool {
					return len(transferChan) >= len(testMetrics)
				}, 10*time.Second, 500*time.Millisecond)
				require.NoError(t, conn.Close())
				// Close the server connection, this will cause ListenAndServer to error out and the deferred wgListenAndServe.Done will fire
				err = srv.Close()
				assert.NoError(t, err)
				wgListenAndServe.Wait()
				assert.Len(t, transferChan, len(testMetrics))
				close(transferChan)
				for vv := range transferChan {
					name, _, _ := strings.Cut(vv.Raw, ":")
					require.Contains(t, testMetricNames, name)
				}
			})
		}
	}
}

func testFreeEndpoint(t *testing.T, transport string, address string) {
	t.Helper()

	var ln0, ln1 io.Closer
	var err0, err1 error

	trans := NewTransport(transport)
	require.NotEqual(t, trans, Transport(""))

	if trans.IsPacketTransport() {
		// Endpoint should be free.
		ln0, err0 = net.ListenPacket(transport, address)
		ln1, err1 = net.ListenPacket(transport, address)
	}

	if trans.IsStreamTransport() {
		// Endpoint should be free.
		ln0, err0 = net.Listen(transport, address)
		ln1, err1 = net.Listen(transport, address)
	}

	// Endpoint should be free.
	require.NoError(t, err0)
	require.NotNil(t, ln0)

	// Ensure that the endpoint wasn't something like ":0" by checking that a second listener will fail.
	require.Error(t, err1)
	require.Nil(t, ln1)

	// Unbind the local address so the mock UDP service can use it
	require.NoError(t, ln0.Close())
}
