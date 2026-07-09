// Day 1: The proxy that makes the network stop being nice.
//
// Sits between client and server. For every message (one JSON line)
// passing through in either direction, it independently decides to:
//   - drop it        (probability -drop)
//   - delay it        (random delay up to -delaymax, per message --
//                       so messages can arrive OUT OF ORDER)
//   - duplicate it     (probability -dup)
//
// Each message is handled in its own goroutine with its own random
// delay, so messages sent in order can genuinely arrive out of order
// on the other side. That's real reordering, not a simulation of it.
//
// Run:
//   go run proxy.go
//   go run proxy.go -listenport 9001 -targetport 9000 -drop 0.3 -delaymax 250ms -dup 0.25
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	listenHost = "127.0.0.1"
	targetHost = "127.0.0.1"
)

var (
	dropProb  float64
	delayMax  time.Duration
	dupProb   float64
)

// writer wraps a net.Conn with a mutex so concurrent goroutines delivering
// chaos-delayed messages don't interleave partial writes into the stream.
type writer struct {
	conn net.Conn
	mu   sync.Mutex
}

func (w *writer) write(line []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.conn.Write(append(line, '\n'))
	return err
}

func deliver(dst *writer, line []byte, label string) {
	if rand.Float64() < dropProb {
		fmt.Printf("[proxy] %s DROPPED: %s\n", label, truncate(line))
		return
	}

	delay := time.Duration(rand.Int63n(int64(delayMax) + 1))
	time.Sleep(delay)

	if err := dst.write(line); err != nil {
		fmt.Printf("[proxy] %s failed to forward (dst closed): %v\n", label, err)
		return
	}
	fmt.Printf("[proxy] %s forwarded (delay=%v): %s\n", label, delay.Round(time.Millisecond), truncate(line))

	if rand.Float64() < dupProb {
		time.Sleep(time.Duration(rand.Int63n(int64(delayMax) + 1)))
		if err := dst.write(line); err == nil {
			fmt.Printf("[proxy] %s DUPLICATED: %s\n", label, truncate(line))
		}
	}
}

func truncate(b []byte) string {
	s := string(b)
	if len(s) > 60 {
		return s[:60]
	}
	return s
}

func forward(src net.Conn, dst *writer, label string, wg *sync.WaitGroup) {
	defer wg.Done()
	scanner := bufio.NewScanner(src)
	var inner sync.WaitGroup
	for scanner.Scan() {
		line := make([]byte, len(scanner.Bytes()))
		copy(line, scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		inner.Add(1)
		go func(l []byte) {
			defer inner.Done()
			deliver(dst, l, label)
		}(line)
	}
	inner.Wait()
}

func handleConnection(clientConn net.Conn, targetPort int) {
	defer clientConn.Close()
	addr := clientConn.RemoteAddr()
	fmt.Printf("[proxy] client connected: %s\n", addr)

	serverConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", targetHost, targetPort))
	if err != nil {
		fmt.Printf("[proxy] failed to connect to target: %v\n", err)
		return
	}
	defer serverConn.Close()

	clientWriter := &writer{conn: clientConn}
	serverWriter := &writer{conn: serverConn}

	var wg sync.WaitGroup
	wg.Add(2)
	go forward(clientConn, serverWriter, "client->server", &wg)
	go forward(serverConn, clientWriter, "server->client", &wg)
	wg.Wait()

	fmt.Printf("[proxy] client disconnected: %s\n", addr)
}

func main() {
	listenPort := flag.Int("listenport", 9001, "port to listen on")
	targetPort := flag.Int("targetport", 9000, "port to forward to")
	drop := flag.Float64("drop", 0.0, "drop probability 0-1")
	delayMaxFlag := flag.Duration("delaymax", 0, "max random delay, e.g. 250ms")
	dup := flag.Float64("dup", 0.0, "duplicate probability 0-1")
	flag.Parse()

	dropProb = *drop
	delayMax = *delayMaxFlag
	dupProb = *dup

	addr := fmt.Sprintf("%s:%d", listenHost, *listenPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("[proxy] failed to listen: %v\n", err)
		return
	}
	defer ln.Close()

	fmt.Printf("[proxy] listening on %s -> forwarding to %s:%d\n", addr, targetHost, *targetPort)
	fmt.Printf("[proxy] drop=%.2f delaymax=%v dup=%.2f\n", dropProb, delayMax, dupProb)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("[proxy] accept error: %v\n", err)
			continue
		}
		go handleConnection(conn, *targetPort)
	}
}