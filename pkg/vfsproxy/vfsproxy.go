package vfsproxy

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/spf13/pflag"
	"github.com/tgdrive/vfscache-proxy/backend/link"
)

type Options struct {
	FsName            string `vfs:"-" flag:"fs-name" caddy:"fs_name" help:"The name of the VFS file system"`
	CacheDir          string `vfs:"-" flag:"cache-dir" caddy:"cache_dir" help:"Cache directory"`
	CacheMaxAge       string `vfs:"vfs_cache_max_age" flag:"max-age" caddy:"max_age" help:"Max age of files in cache"`
	CacheMaxSize      string `vfs:"vfs_cache_max_size" flag:"max-size" caddy:"max_size" help:"Max total size of objects in cache"`
	CacheChunkSize    string `vfs:"vfs_read_chunk_size" flag:"chunk-size" caddy:"chunk_size" help:"Default Chunk size of read request"`
	CacheChunkStreams int    `vfs:"vfs_read_chunk_streams" flag:"chunk-streams" caddy:"chunk_streams" help:"The number of parallel streams to read at once"`
	StripQuery        bool   `vfs:"-" flag:"strip-query" caddy:"strip_query" help:"Strip query parameters from URL for caching"`
	StripDomain       bool   `vfs:"-" flag:"strip-domain" caddy:"strip_domain" help:"Strip domain and protocol from URL for caching"`
	ShardLevel        int    `vfs:"-" flag:"shard-level" caddy:"shard-level" help:"Number of shard levels"`

	// Additional VFS Options
	CacheMode         string `vfs:"vfs_cache_mode" flag:"cache-mode" caddy:"cache_mode" help:"VFS cache mode (off, minimal, writes, full)"`
	WriteWait         string `vfs:"vfs_write_wait" flag:"write-wait" caddy:"write_wait" help:"VFS write wait time"`
	ReadWait          string `vfs:"vfs_read_wait" flag:"read-wait" caddy:"read_wait" help:"VFS read wait time"`
	WriteBack         string `vfs:"vfs_write_back" flag:"write-back" caddy:"write_back" help:"VFS write back time"`
	DirCacheTime      string `vfs:"dir_cache_time" flag:"dir-cache-time" caddy:"dir_cache_time" help:"VFS directory cache time"`
	FastFingerprint   bool   `vfs:"vfs_fast_fingerprint" flag:"fast-fingerprint" caddy:"fast_fingerprint" help:"Use fast fingerprinting"`
	CacheMinFreeSpace string `vfs:"vfs_cache_min_free_space" flag:"min-free-space" caddy:"min_free_space" help:"VFS minimum free space in cache"`
	CaseInsensitive   bool   `vfs:"vfs_case_insensitive" flag:"case-insensitive" caddy:"case_insensitive" help:"VFS case insensitive"`
	ReadOnly          bool   `vfs:"read_only" flag:"read-only" caddy:"read_only" help:"VFS read only"`
	NoModTime         bool   `vfs:"no_modtime" flag:"no-modtime" caddy:"no_modtime" help:"VFS no modtime"`
	NoChecksum        bool   `vfs:"no_checksum" flag:"no-checksum" caddy:"no_checksum" help:"VFS no checksum"`
	NoSeek            bool   `vfs:"no_seek" flag:"no-seek" caddy:"no_seek" help:"VFS no seek"`
	DirPerms          string `vfs:"dir_perms" flag:"dir-perms" caddy:"dir_perms" help:"VFS directory permissions"`
	FilePerms         string `vfs:"file_perms" flag:"file-perms" caddy:"file_perms" help:"VFS file permissions"`
}

// AddFlags adds flags to the given FlagSet.
func (opt *Options) AddFlags(fs *pflag.FlagSet) {
	v := reflect.ValueOf(opt).Elem()
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		flagName := field.Tag.Get("flag")
		if flagName == "" || flagName == "-" {
			continue
		}
		help := field.Tag.Get("help")
		f := v.Field(i)
		switch f.Kind() {
		case reflect.String:
			fs.StringVar(f.Addr().Interface().(*string), flagName, f.String(), help)
		case reflect.Int:
			fs.IntVar(f.Addr().Interface().(*int), flagName, int(f.Int()), help)
		case reflect.Bool:
			fs.BoolVar(f.Addr().Interface().(*bool), flagName, f.Bool(), help)
		}
	}
}

// ToConfigMap converts Options to a rclone configmap.
func (opt *Options) ToConfigMap() configmap.Simple {
	m := configmap.Simple{}
	v := reflect.ValueOf(opt).Elem()
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("vfs")
		if tag == "" || tag == "-" {
			continue
		}
		f := v.Field(i)
		switch f.Kind() {
		case reflect.String:
			m[tag] = f.String()
		case reflect.Int:
			m[tag] = strconv.Itoa(int(f.Int()))
		case reflect.Bool:
			m[tag] = strconv.FormatBool(f.Bool())
		}
	}
	return m
}

// DefaultOptions returns Options with sensible defaults applied from rclone.
func DefaultOptions() Options {
	opt := Options{
		FsName:     "link-vfs",
		ShardLevel: 1,
	}

	vfsOpt := vfscommon.Opt
	items, err := configstruct.Items(&vfsOpt)
	if err != nil {
		return opt
	}

	optValue := reflect.ValueOf(&opt).Elem()
	optType := optValue.Type()

	for _, item := range items {
		valStr, _ := configstruct.InterfaceToString(item.Value)
		for i := 0; i < optType.NumField(); i++ {
			field := optType.Field(i)
			tag := field.Tag.Get("vfs")
			if tag == item.Name {
				f := optValue.Field(i)
				switch f.Kind() {
				case reflect.String:
					f.SetString(valStr)
				case reflect.Int:
					if val, err := strconv.Atoi(valStr); err == nil {
						f.SetInt(int64(val))
					}
				case reflect.Bool:
					f.SetBool(valStr == "true")
				}
				break
			}
		}
	}

	return opt
}

type Handler struct {
	VFS         *vfs.VFS
	mu          sync.RWMutex
	hashCache   map[string]string
	stripQuery  bool
	stripDomain bool
	shardLevel  int
}

func NewHandler(opt Options) (*Handler, error) {
	ctx := context.Background()

	m := configmap.Simple{
		"type":         "link",
		"strip_query":  strconv.FormatBool(opt.StripQuery),
		"strip_domain": strconv.FormatBool(opt.StripDomain),
		"shard_level":  strconv.Itoa(opt.ShardLevel),
	}

	// Create a new file system for the link backend
	f, err := fs.NewFs(ctx, opt.FsName+":")
	if err != nil {
		// Fallback to manual creation if not in rclone config
		f, err = link.NewFs(ctx, opt.FsName, "", m)
		if err != nil {
			return nil, fmt.Errorf("failed to create link backend: %w", err)
		}
	}

	// Configure VFS options
	vfsOpt := vfscommon.Opt
	optMap := opt.ToConfigMap()

	if err := configstruct.Set(optMap, &vfsOpt); err != nil {
		return nil, fmt.Errorf("failed to parse VFS options: %w", err)
	}
	vfsOpt.Init() // Initialize options (sets up permissions, etc.)

	actualCacheDir := opt.CacheDir
	if actualCacheDir == "" {
		actualCacheDir = filepath.Join(os.TempDir(), "rclone_vfs_cache")
	}
	if err := config.SetCacheDir(actualCacheDir); err != nil {
		return nil, fmt.Errorf("failed to set cache directory: %w", err)
	}

	vfsInstance := vfs.New(f, &vfsOpt)
	return &Handler{
		VFS:         vfsInstance,
		hashCache:   make(map[string]string),
		stripQuery:  opt.StripQuery,
		stripDomain: opt.StripDomain,
		shardLevel:  opt.ShardLevel,
	}, nil
}

func (h *Handler) Shutdown() {
	h.VFS.Shutdown()
}

func (h *Handler) getFileHash(targetURL string) string {
	h.mu.RLock()
	fileHash, exists := h.hashCache[targetURL]
	h.mu.RUnlock()

	if exists {
		return fileHash
	}

	// Apply stripping to the URL before hashing
	keyURL := link.StripURL(targetURL, h.stripQuery, h.stripDomain)

	hashBytes := md5.Sum([]byte(keyURL))
	computedHash := fmt.Sprintf("%x", hashBytes)

	// Double-checked locking to avoid duplicate computation
	h.mu.Lock()
	if fileHash, exists = h.hashCache[targetURL]; exists {
		h.mu.Unlock()
		return fileHash
	}
	h.hashCache[targetURL] = computedHash
	h.mu.Unlock()

	return computedHash
}

func (h *Handler) Serve(w http.ResponseWriter, r *http.Request, targetURL string) {
	if targetURL == "" {
		http.Error(w, "Target URL is required", http.StatusBadRequest)
		return
	}

	fileHash := h.getFileHash(targetURL)

	link.Register(fileHash, targetURL, r.Header.Clone())

	h.ServeFile(w, r, link.ShardedPath(fileHash, h.shardLevel))
}

func (h *Handler) ServeFile(w http.ResponseWriter, r *http.Request, remote string) {
	ctx := r.Context()
	node, err := h.VFS.Stat(remote)
	if err == vfs.ENOENT {
		fs.Infof(remote, "%s: File not found", r.RemoteAddr)
		http.Error(w, "File not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Failed to find file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !node.IsFile() {
		http.Error(w, "Not a file", http.StatusNotFound)
		return
	}

	entry := node.DirEntry()
	if entry == nil {
		http.Error(w, "Can't open file being written", http.StatusNotFound)
		return
	}
	obj := entry.(fs.Object)
	file := node.(*vfs.File)

	knownSize := obj.Size() >= 0
	if knownSize {
		w.Header().Set("Content-Length", strconv.FormatInt(node.Size(), 10))
	}

	mimeType := fs.MimeType(ctx, obj)
	if mimeType == "application/octet-stream" && path.Ext(remote) == "" {
	} else {
		w.Header().Set("Content-Type", mimeType)
	}
	w.Header().Set("Last-Modified", file.ModTime().UTC().Format(http.TimeFormat))

	if r.Method == "HEAD" {
		return
	}

	// open the object
	in, err := file.Open(os.O_RDONLY)
	if err != nil {
		http.Error(w, "Failed to open file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() {
		_ = in.Close()
	}()

	if knownSize {
		http.ServeContent(w, r, remote, file.ModTime(), in)
	} else {
		if rangeRequest := r.Header.Get("Range"); rangeRequest != "" {
			http.Error(w, "Can't use Range: on files of unknown length", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		n, err := io.Copy(w, in)
		if err != nil {
			fs.Errorf(obj, "Didn't finish writing GET request (wrote %d/unknown bytes): %v", n, err)
			return
		}
	}
}
