// Day 1: A tiny RPC server.
//
// Protocol: newline-delimited JSON over TCP.
// Request:  {"id": 1, "method": "add", "a": 3, "b": 4}
// Response: {"id": 1, "result": 7}
//
// Run: go run server.go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
)

const (
	host = "127.0.0.1"
	port = "9000"
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

func handleConn(conn net.Conn) {
	defer conn.Close()
	addr := conn.RemoteAddr()
	fmt.Printf("[server] connection from %s\n", addr)

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			fmt.Printf("[server] bad json: %s\n", line)
			continue
		}

		var result int
		switch req.Method {
		case "add":
			result = req.A + req.B
		default:
			result = 0
		}

		resp := Response{ID: req.ID, Result: result}
		out, err := json.Marshal(resp)
		if err != nil {
			log.Printf("[server] marshal error: %v", err)
			continue
		}

		fmt.Printf("[server] handled id=%d method=%s -> %d\n", req.ID, req.Method, result)
		if _, err := conn.Write(append(out, '\n')); err != nil {
			fmt.Printf("[server] write failed, closing: %v\n", err)
			return
		}
	}
	fmt.Printf("[server] connection closed %s\n", addr)
}

func main() {
	addr := host + ":" + port
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[server] failed to listen: %v", err)
	}
	defer ln.Close()
	fmt.Printf("[server] listening on %s\n", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[server] accept error: %v", err)
			continue
		}
		go handleConn(conn)
	}
}