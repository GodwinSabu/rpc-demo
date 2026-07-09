// Day 1: A tiny RPC client.
//
// Points at whatever host:port you give it -- the server directly on
// day 1a, then the proxy on day 1b.
//
// Run:
//   go run client.go
//   go run client.go -port 9001 -n 20 -timeout 1s
//   go run client.go -port 9001 -n 15 -timeout 600ms -newconn
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"time"
)

type Request struct {
	ID     int    `json:"id"`
	Method string `json:"method"`
	A      int    `json:"a"`
	B      int    `json:"b"`
}

type Response struct {
	ID     int `json:"id"`
	Result int `json:"result"`
}

type outcome struct {
	status  string
	payload string
	elapsed time.Duration
}

func sendOne(conn net.Conn, reqID int, timeout time.Duration) outcome {
	req := Request{ID: reqID, Method: "add", A: reqID, B: 100}
	out, _ := json.Marshal(req)

	start := time.Now()
	conn.SetDeadline(time.Now().Add(timeout))

	if _, err := conn.Write(append(out, '\n')); err != nil {
		return outcome{"send_error", err.Error(), time.Since(start)}
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return outcome{"timeout", "", time.Since(start)}
		}
		return outcome{"recv_error", err.Error(), time.Since(start)}
	}

	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return outcome{"bad_response", string(line), time.Since(start)}
	}
	return outcome{"ok", fmt.Sprintf("{id:%d result:%d}", resp.ID, resp.Result), time.Since(start)}
}

func main() {
	host := flag.String("host", "127.0.0.1", "server host")
	port := flag.Int("port", 9000, "server port")
	n := flag.Int("n", 1, "number of requests")
	timeout := flag.Duration("timeout", 2*time.Second, "per-request timeout, e.g. 500ms")
	newConnEach := flag.Bool("newconn", false, "open a fresh TCP connection for every request")
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *host, *port)

	summary := map[string]int{}

	var conn net.Conn
	var err error
	if !*newConnEach {
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("[client] failed to connect: %v\n", err)
			return
		}
		defer conn.Close()
	}

	for i := 1; i <= *n; i++ {
		if *newConnEach {
			conn, err = net.Dial("tcp", addr)
			if err != nil {
				fmt.Printf("[client] id=%d connect_error status=%v\n", i, err)
				summary["connect_error"]++
				continue
			}
		}

		o := sendOne(conn, i, *timeout)
		summary[o.status]++
		fmt.Printf("[client] id=%-3d status=%-14s time=%6.2fs payload=%s\n",
			i, o.status, o.elapsed.Seconds(), o.payload)

		if *newConnEach {
			conn.Close()
		}
	}

	fmt.Println("\n[client] summary:", summary)
}