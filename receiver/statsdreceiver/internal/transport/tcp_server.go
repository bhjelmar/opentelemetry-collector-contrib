// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package transport // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/statsdreceiver/internal/transport"

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"go.opentelemetry.io/collector/consumer"
)

var errTCPServerDone = errors.New("server stopped")

type tcpServer struct {
	listener  net.Listener
	reporter  Reporter
	wg        sync.WaitGroup
	transport Transport
	stopChan  chan struct{}
}

// Ensure that Server is implemented on TCP Server.
var _ Server = (*tcpServer)(nil)

// NewTCPServer creates a transport.Server using TCP as its transport.
func NewTCPServer(transport Transport, address string) (Server, error) {
	var tsrv tcpServer
	var err error

	if !transport.IsStreamTransport() {
		return nil, fmt.Errorf("NewTCPServer with %s: %w", transport.String(), ErrUnsupportedStreamTransport)
	}

	tsrv.transport = transport
	tsrv.listener, err = net.Listen(transport.String(), address)
	if err != nil {
		return nil, fmt.Errorf("starting to listen %s socket: %w", transport.String(), err)
	}

	tsrv.stopChan = make(chan struct{})
	return &tsrv, nil
}

// ListenAndServe starts the server ready to receive metrics.
func (t *tcpServer) ListenAndServe(nextConsumer consumer.Metrics, reporter Reporter, transferChan chan<- Metric) error {
	if nextConsumer == nil || reporter == nil {
		return errNilListenAndServeParameters
	}

	t.reporter = reporter
LOOP:
	for {
		connChan := make(chan net.Conn, 1)
		go func() {
			c, err := t.listener.Accept()
			if err != nil {
				t.reporter.OnDebugf("TCP Transport - Accept error: %v",
					err)
			} else {
				connChan <- c
			}
		}()

		select {
		case conn := <-connChan:
			t.wg.Add(1)
			go t.handleConn(conn, transferChan)
		case <-t.stopChan:
			break LOOP
		}
	}
	return errTCPServerDone
}

// handleConn is helper that parses the buffer and split it line by line to be parsed upstream.
func (t *tcpServer) handleConn(c net.Conn, transferChan chan<- Metric) {

	defer c.Close()
	defer t.wg.Done()

	reader := bufio.NewReader(c)
	var remainder []byte

	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			if len(line) > 0 {
				remainder = append(remainder, line...)
				processedLine := strings.TrimSpace(string(remainder))
				if processedLine != "" {
					transferChan <- Metric{processedLine, c.LocalAddr()}
				}
			}
			return
		}
		if err != nil {
			t.reporter.OnDebugf("TCP transport (%s) Error reading payload: %v", c.LocalAddr(), err)
			return
		}

		// Process the complete line
		fullLine := append(remainder, line...)
		remainder = nil // Clear remainder as it is now fully processed
		processedLine := strings.TrimSpace(string(fullLine))
		if processedLine != "" {
			transferChan <- Metric{processedLine, c.LocalAddr()}
		}
	}
}

// Close closes the server.
func (t *tcpServer) Close() error {
	close(t.stopChan)
	t.wg.Wait()
	return t.listener.Close()
}
