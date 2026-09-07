package adminhttp

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSlowBodiesReleaseAdmissionForEveryMethod(t *testing.T) {
	for _, route := range []struct{ method, path string }{
		{"GET", "/dead-letters"}, {"HEAD", "/dead-letters"},
		{"GET", "/audit"}, {"HEAD", "/audit"},
		{"POST", "/dead-letters/1/requeue"},
	} {
		for _, framing := range []string{"length", "chunked"} {
			t.Run(route.method+route.path+"/"+framing, func(t *testing.T) {
				h, err := New(&fakeStore{}, Options{Namespace: "test", Timeout: 100 * time.Millisecond, MaxConcurrentLists: 1, MaxConcurrentRequeues: 1})
				if err != nil {
					t.Fatal(err)
				}
				finished := make(chan struct{}, 2)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(w, r); finished <- struct{}{} }))
				defer server.Close()
				conn, err := net.Dial("tcp", server.Listener.Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				body := "Content-Length: 100\r\n\r\n{"
				if framing == "chunked" {
					body = "Transfer-Encoding: chunked\r\n\r\n64\r\n{"
				}
				if _, err := fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: test\r\n%s", route.method, route.path, body); err != nil {
					t.Fatal(err)
				}
				select {
				case <-finished:
				case <-time.After(2 * time.Second):
					t.Fatal("slow body bypassed request timeout")
				}
				requestBody := ""
				if route.method == "POST" {
					requestBody = `{"failed_at":"2026-09-06T00:00:00Z"}`
				}
				req, err := http.NewRequest(route.method, server.URL+route.path, strings.NewReader(requestBody))
				if err != nil {
					t.Fatal(err)
				}
				client := server.Client()
				client.Timeout = 2 * time.Second
				response, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				if response.StatusCode != http.StatusOK {
					t.Fatalf("admission not reusable after timeout: %d", response.StatusCode)
				}
			})
		}
	}
}

func TestCompletedRequestDoesNotPoisonKeepAliveDeadline(t *testing.T) {
	const timeout = 50 * time.Millisecond
	h, err := New(&fakeStore{}, Options{Namespace: "test", Timeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	for i := range 2 {
		if i > 0 {
			time.Sleep(2 * timeout)
		}
		if _, err := fmt.Fprint(conn, "GET /dead-letters HTTP/1.1\r\nHost: test\r\nContent-Length: 3\r\n\r\nabc"); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(reader, &http.Request{Method: "GET"})
		if err != nil {
			t.Fatalf("request %d reused stale deadline: %v", i, err)
		}
		if _, err := io.Copy(io.Discard, response.Body); err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK || response.Close {
			t.Fatalf("request %d: status=%d close=%v", i, response.StatusCode, response.Close)
		}
	}
}
