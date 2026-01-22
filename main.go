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
	port              = pflag.String("port", "8080", "Port to listen on")
	cacheChunkSize    = pflag.String("chunk-size", "64M", "Default Chunk size of read request")
	cacheMaxAge       = pflag.String("max-age", "1h", "Max age of files in cache")
	cacheMaxSize      = pflag.String("max-size", "off", "Max total size of objects in cache")
	cacheDir          = pflag.String("cache-dir", "", "Cache directory")
	cacheChunkStreams = pflag.Int("chunk-streams", 2, "The number of parallel streams to read at once")
	stripQuery        = pflag.Bool("strip-query", false, "Strip query parameters from URL for caching")
	stripDomain       = pflag.Bool("strip-domain", false, "Strip domain and protocol from URL for caching")
	metadataCacheSize = pflag.Int("metadata-cache-size", 5*1024*1024, "Size of the in-memory metadata cache")
	fsName            = pflag.String("fs-name", "vfs-proxy", "The name of the VFS file system")

	// Additional VFS flags
	cacheMode         = pflag.String("cache-mode", "full", "VFS cache mode (off, minimal, writes, full)")
	cachePollInterval = pflag.String("poll-interval", "1m", "VFS cache poll interval")
	writeWait         = pflag.String("write-wait", "1s", "VFS write wait time")
	readWait          = pflag.String("read-wait", "20ms", "VFS read wait time")
	writeBack         = pflag.String("write-back", "5s", "VFS write back time")
	dirCacheTime      = pflag.String("dir-cache-time", "0s", "VFS directory cache time")
	fastFingerprint   = pflag.Bool("fast-fingerprint", false, "Use fast fingerprinting")
	cacheMinFreeSpace = pflag.String("min-free-space", "off", "VFS minimum free space in cache")
	caseInsensitive   = pflag.Bool("case-insensitive", false, "VFS case insensitive")
	readOnly          = pflag.Bool("read-only", false, "VFS read only")
	noModTime         = pflag.Bool("no-modtime", false, "VFS no modtime")
	noChecksum        = pflag.Bool("no-checksum", false, "VFS no checksum")
	noSeek            = pflag.Bool("no-seek", false, "VFS no seek")
	dirPerms          = pflag.String("dir-perms", "0777", "VFS directory permissions")
	filePerms         = pflag.String("file-perms", "0666", "VFS file permissions")
)

func main() {
	pflag.Parse()

	opt := vfsproxy.Options{
		FsName:            *fsName,
		CacheDir:          *cacheDir,
		CacheMaxAge:       *cacheMaxAge,
		CacheMaxSize:      *cacheMaxSize,
		CacheChunkSize:    *cacheChunkSize,
		CacheChunkStreams: *cacheChunkStreams,
		StripQuery:        *stripQuery,
		StripDomain:       *stripDomain,
		MetadataCacheSize: *metadataCacheSize,

		// Map additional VFS flags
		CacheMode:         *cacheMode,
		CachePollInterval: *cachePollInterval,
		WriteWait:         *writeWait,
		ReadWait:          *readWait,
		WriteBack:         *writeBack,
		DirCacheTime:      *dirCacheTime,
		FastFingerprint:   *fastFingerprint,
		CacheMinFreeSpace: *cacheMinFreeSpace,
		CaseInsensitive:   *caseInsensitive,
		ReadOnly:          *readOnly,
		NoModTime:         *noModTime,
		NoChecksum:        *noChecksum,
		NoSeek:            *noSeek,
		DirPerms:          *dirPerms,
		FilePerms:         *filePerms,
	}

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
