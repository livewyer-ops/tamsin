//go:build ignore

// Serve immutable test fixtures with the standard HTTP range implementation.
package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("usage: e2e-http DIRECTORY PORT_FILE")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	files := http.FileServer(http.Dir(os.Args[1]))
	server := &http.Server{ReadHeaderTimeout: 10 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/stage/") {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/stage")
			r.Header.Del("Range")
		} else {
			// Fixtures are written before this process starts and stay immutable.
			w.Header().Set("ETag", `"tamsin-e2e"`)
		}
		files.ServeHTTP(w, r)
	})}
	if err := os.WriteFile(os.Args[2], []byte(strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)), 0o600); err != nil {
		log.Fatal(err)
	}
	log.Fatal(server.Serve(listener))
}
