package main

import (
	"context"
	"encoding/base64"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/tgdrive/vfscache-proxy/pkg/vfsproxy"

	"github.com/rclone/rclone/fs/config"
	"github.com/spf13/pflag"
)

var (
	port = pflag.String("port", "8080", "Port to listen on")
	opt  = vfsproxy.DefaultOptions()
)

func main() {
	opt.AddFlags(pflag.CommandLine)
	pflag.Parse()

	handler, err := vfsproxy.NewHandler(opt)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()

	mainHandler := func(w http.ResponseWriter, r *http.Request) {
		targetURL := r.URL.Query().Get("url")

		// Check for Base64 URL in path
		if targetURL == "" && strings.HasPrefix(r.URL.Path, "/stream/") {
			encodedURL := strings.TrimPrefix(r.URL.Path, "/stream/")
			if decoded, err := base64.RawURLEncoding.DecodeString(encodedURL); err == nil {
				targetURL = string(decoded)
			} else if decoded, err := base64.URLEncoding.DecodeString(encodedURL); err == nil {
				targetURL = string(decoded)
			}
		}

		if targetURL == "" {
			http.Error(w, "Missing 'url' parameter or base64 path", http.StatusBadRequest)
			return
		}

		handler.Serve(w, r, targetURL)
	}

	mux.HandleFunc("/stream", mainHandler)
	mux.HandleFunc("/stream/", mainHandler)

	srv := &http.Server{
		Addr:    ":" + *port,
		Handler: mux,
	}

	// Channel to listen for signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("VFS Proxy listening on :%s", *port)
		log.Printf("VFS Cache Mode: %v", handler.VFS.Opt.CacheMode)
		log.Printf("VFS Cache Dir: %s", config.GetCacheDir())
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	<-stop

	log.Println("Shutting down gracefully...")

	// Create a context with timeout for the shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Shutting down VFS...")
	handler.Shutdown()

	log.Println("Exit")
}
